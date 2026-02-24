import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import numpy as np

st.set_page_config(page_title="SEAO — Tableau de bord", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@300;400;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Source Sans 3', sans-serif; }
    .block-container { padding-top: 1.5rem; }
    h1 { color: #003366; font-weight: 700; }
    h2, h3 { color: #1a4d80; }
    div[data-testid="stMetric"] { background: linear-gradient(135deg, #f0f4f8 0%, #e8eef5 100%); border: 1px solid #d0dbe8; border-radius: 10px; padding: 12px 16px; }
    div[data-testid="stMetric"] label { color: #4a6d8c; font-weight: 600; }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] { color: #003366; }
    section[data-testid="stSidebar"] { background: linear-gradient(180deg, #003366 0%, #1a5276 100%); }
    section[data-testid="stSidebar"] * { color: #e0e8f0 !important; }
</style>
""", unsafe_allow_html=True)

TI_KEYWORDS = ['informatiq', 'logiciel', 'technolog', 'numéri', 'cloud', 'cyber', 'télécommunic', 'réseau', 'données', 'internet']
SECTEUR_MACRO_MAP = {'72': 'Construction', '81': 'Ingénierie et services pro.', '80': 'Gestion et RH', '43': 'Technologies de l\'information', '42': 'Santé (matériel)', '85': 'Santé (services)', '92': 'Sécurité', '93': 'Administration publique', '15': 'Carburants et énergie', '78': 'Transport', '82': 'Communications et publicité', '30': 'Matériaux de construction', '25': 'Véhicules', '76': 'Nettoyage et entretien', '70': 'Aménagement paysager', '50': 'Aliments et boissons', '51': 'Médicaments'}

def classify_macro(unspsc):
    code = str(unspsc).replace('.0', '')[:2] if pd.notna(unspsc) else ''
    return SECTEUR_MACRO_MAP.get(code, 'Autre')

def is_ti(desc):
    if not isinstance(desc, str): return False
    d = desc.lower()
    return any(k in d for k in TI_KEYWORDS)

DATA_DIR = Path(".")

@st.cache_data(show_spinner="Chargement des données SEAO…")
def load_data():
    # 1. Fichiers csv.gz à la racine (structure GitHub)
    gz_files = sorted(Path(".").glob("SEAO_ENRICHI*.csv.gz"))
    if gz_files:
        dfs = [pd.read_csv(f, compression="gzip", low_memory=False) for f in gz_files]
    # 2. Parquet dans data/
    elif sorted(Path("data").glob("SEAO_ENRICHI*.parquet")):
        dfs = [pd.read_parquet(f) for f in sorted(Path("data").glob("SEAO_ENRICHI*.parquet"))]
    # 3. CSV dans data/
    else:
        files = sorted(Path("data").glob("SEAO_ENRICHI*.csv"))
        if not files:
            files = sorted(Path("data").glob("SEAO_FINAL_*.csv"))
        if not files:
            st.error("Aucun fichier de données trouvé.")
            return pd.DataFrame()
        dfs = [pd.read_csv(f, encoding="utf-8-sig", low_memory=False) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df["date_local"] = df["date"].dt.tz_localize(None)
    df["mois"] = df["date_local"].dt.to_period("M").astype(str)
    df["trimestre"] = df["date_local"].dt.to_period("Q").astype(str)
    for col in ["montant_adjuge", "montant_final", "ecart_prix", "taux_depassement"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["a_depassement"] = df.get("taux_depassement", 0) > 0
    for col, default in [("est_quebecois", 0), ("region_admin", "Inconnue"), ("est_municipal", ""), ("nb_amendments", 0)]:
        if col not in df.columns:
            df[col] = default
    # Normaliser modes d'adjudication (doublons XML vs JSON)
    mode_map = {
        "Avis d'appel d'offres": "Appel d'offres public",
        "Avis d\u2019appel d\u2019offres": "Appel d'offres public",
        "Avis d'appel d'offres sur invitation": "Appel d'offres sur invitation",
        "Avis d'appel d'offres régionalisé": "Appel d'offres régionalisé",
        "Contrat suite à un appel d'offres sur invitation": "Contrat à la suite d'un appel d'offres sur invitation",
        "Contrat suite à un achat mandaté ou à un regroupement d'organismes": "Contrat à la suite d'un achat mandaté ou d'un regroupement d'organismes",
        "Contrat conclu - Appel d'offres public non publié au SEAO": "Contrat conclu - AO non publié au SEAO",
    }
    df["mode_adjudication"] = df["mode_adjudication"].replace(mode_map)
    # Regroupement simplifié pour le dashboard
    def simplify_mode(m):
        if not isinstance(m, str): return "Inconnu"
        m_lower = m.lower()
        if "gré à gré" in m_lower: return "Gré à gré"
        if "appel d'offres public" in m_lower or "appel d'offres" == m_lower.strip() or m.startswith("Appel d'offres public"): return "Appel d'offres public"
        if "invitation" in m_lower: return "Appel d'offres sur invitation"
        if "régionalisé" in m_lower: return "Appel d'offres régionalisé"
        if "achat mandaté" in m_lower or "regroupement" in m_lower: return "Achat mandaté / regroupement"
        if "infrastructure" in m_lower: return "Infrastructures de transport"
        if "réservé" in m_lower or "petites entreprises" in m_lower: return "Réservé PME"
        if "inconnu" in m_lower: return "Inconnu"
        if "non publié" in m_lower: return "AO non publié au SEAO"
        return m
    df["mode_simplifie"] = df["mode_adjudication"].apply(simplify_mode)
    df["mode_adjudication"] = df["mode_adjudication"].replace(mode_map)
    # Normaliser régions fallback
    df["region_admin"] = df["region_admin"].replace({
        "Région de Montréal (Secteur Inconnu)": "Montréal",
        "Est du Québec (Secteur Inconnu)": "Est du Québec",
        "Ouest du Québec (Secteur Inconnu)": "Ouest du Québec",
    })
    df["secteur_macro"] = df["secteur_unspsc"].apply(classify_macro)
    df["est_ti"] = df["secteur_description"].apply(is_ti)
    if "annee_signature" in df.columns:
        df["annee_ref"] = pd.to_numeric(df["annee_signature"], errors="coerce").fillna(pd.to_numeric(df["annee"], errors="coerce")).astype("Int64")
    else:
        df["annee_ref"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")
    return df

df = load_data()
if df.empty:
    st.error("Aucun fichier CSV trouvé dans data/")
    st.stop()

COLORS = ["#003366", "#1a5276", "#2e86c1", "#5dade2", "#85c1e9", "#aed6f1", "#d35400", "#e67e22", "#f39c12", "#f1c40f"]
RED = "#c0392b"
GREEN = "#27ae60"

# === SIDEBAR ===
with st.sidebar:
    st.markdown("## 🔍 Filtres")
    annees = sorted(df["annee_ref"].dropna().unique())
    sel_annees = st.select_slider("Années", options=annees, value=(min(annees), max(annees))) if len(annees) >= 2 else (annees[0], annees[0])
    top_org = df.groupby("organisme")["montant_adjuge"].sum().nlargest(50).index.tolist()
    sel_organismes = st.multiselect("Organismes (vide = tous)", options=top_org, default=[])
    modes = sorted(df["mode_adjudication"].dropna().unique())
    sel_modes = st.multiselect("Mode d'adjudication", options=modes, default=[])
    regions_dispo = sorted(df["region_admin"].dropna().unique())
    sel_regions = st.multiselect("Région (vide = toutes)", options=regions_dispo, default=[])
    sel_montant_min = st.number_input("Montant minimal ($)", min_value=0, value=0, step=10000, format="%d")
    st.markdown("---")
    st.markdown(f"**{len(df):,}** contrats • **{df['organisme'].nunique():,}** organismes • **{df['fournisseur'].nunique():,}** fournisseurs")

mask = (df["annee_ref"] >= sel_annees[0]) & (df["annee_ref"] <= sel_annees[1])
if sel_organismes: mask &= df["organisme"].isin(sel_organismes)
if sel_modes: mask &= df["mode_adjudication"].isin(sel_modes)
if sel_regions: mask &= df["region_admin"].isin(sel_regions)
if sel_montant_min > 0: mask &= df["montant_adjuge"] >= sel_montant_min
dff = df[mask].copy()

st.markdown("# 📊 Tableau de bord SEAO")
st.markdown("*Système électronique d'appel d'offres du Québec*")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📊 Aperçu général", "🎯 Indicateurs Stratégie", "💻 Secteurs & TI", "⚠️ Dépassements", "🏢 Fournisseurs", "🗺️ Portrait régional"])

# === TAB 1 — APERÇU GÉNÉRAL ===
with tab1:
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Contrats", f"{len(dff):,}")
    c2.metric("Montant total", f"{dff['montant_adjuge'].sum()/1e9:.2f} G$")
    c3.metric("Montant moyen", f"{dff['montant_adjuge'].mean()/1e3:.0f} k$")
    nb_dep = dff["a_depassement"].sum()
    c4.metric("Avec dépassement", f"{nb_dep:,} ({100*nb_dep/max(len(dff),1):.1f}%)")
    c5.metric("Écart total", f"{dff['ecart_prix'].sum()/1e6:.1f} M$")
    st.markdown("---")
    col_a, col_b = st.columns(2)
    yearly = dff.groupby("annee_ref").agg(nb=("montant_adjuge", "count"), total=("montant_adjuge", "sum")).reset_index()
    with col_a:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=yearly["annee_ref"], y=yearly["total"]/1e9, name="Montant (G$)", marker_color="#003366"))
        fig.add_trace(go.Scatter(x=yearly["annee_ref"], y=yearly["nb"], name="Nb contrats", yaxis="y2", line=dict(color="#d35400", width=3), mode="lines+markers"))
        fig.update_layout(title="Montant et volume par année", height=400, yaxis=dict(title="G$"), yaxis2=dict(title="Nb", overlaying="y", side="right"), legend=dict(orientation="h", y=-0.15))
        st.plotly_chart(fig, use_container_width=True)
    with col_b:
        qt = dff.groupby("trimestre")["montant_adjuge"].sum().reset_index().sort_values("trimestre")
        fig2 = px.area(qt, x="trimestre", y=qt["montant_adjuge"]/1e6, title="Montant par trimestre (M$)", color_discrete_sequence=["#2e86c1"])
        fig2.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig2, use_container_width=True)
    mode_yr = dff.groupby(["annee_ref", "mode_simplifie"])["montant_adjuge"].sum().reset_index()
    fig3 = px.bar(mode_yr, x="annee_ref", y=mode_yr["montant_adjuge"]/1e9, color="mode_simplifie", title="Mode d'adjudication (G$)", color_discrete_sequence=COLORS)
    fig3.update_layout(height=450, barmode="stack", legend=dict(orientation="h", y=-0.3))
    st.plotly_chart(fig3, use_container_width=True)

# === TAB 2 — INDICATEURS STRATÉGIE ===
with tab2:
    st.subheader("🎯 Suivi — Stratégie gouvernementale des marchés publics")
    if dff["est_quebecois"].sum() == 0:
        st.warning("Colonnes `est_quebecois`/`region_admin` non renseignées. Utilisez `extract_seao.py` pour enrichir vos données.")
    else:
        total = len(dff)
        qc_n = int(dff["est_quebecois"].sum())
        pct_qc = 100 * qc_n / max(total, 1)
        regions_excl = ['Montréal', 'Capitale-Nationale', 'Inconnue', 'Hors Québec (Canada)', 'International']
        dff_reg = dff[~dff["region_admin"].isin(regions_excl)]
        pct_reg = 100 * len(dff_reg) / max(total, 1)
        non_gre = dff[~dff["mode_adjudication"].str.contains("gré", case=False, na=False)]
        pct_ao = 100 * len(non_gre) / max(total, 1)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Fournisseurs QC", f"{pct_qc:.1f}%", help="Bilan mi-parcours: 52%")
        c2.metric("Valeur contrats QC", f"{dff[dff['est_quebecois']==1]['montant_adjuge'].sum()/1e9:.2f} G$")
        c3.metric("Contractants régionaux", f"{pct_reg:.1f}%", help="Cible: 60%")
        c4.metric("Via appel d'offres", f"{pct_ao:.1f}%")
        st.markdown("---")
        col_e, col_f = st.columns(2)
        with col_e:
            qc_yr = dff.groupby("annee_ref").agg(t=("est_quebecois", "count"), q=("est_quebecois", "sum")).reset_index()
            qc_yr["pct"] = 100 * qc_yr["q"] / qc_yr["t"]
            fig_qc = go.Figure()
            fig_qc.add_trace(go.Bar(x=qc_yr["annee_ref"], y=qc_yr["pct"], marker_color="#003366"))
            fig_qc.add_hline(y=52, line_dash="dash", line_color=GREEN, annotation_text="Résultat 2023-24: 52%")
            fig_qc.update_layout(title="% fournisseurs québécois par année", height=380, yaxis=dict(range=[0, 100]))
            st.plotly_chart(fig_qc, use_container_width=True)
        with col_f:
            orig = dff.copy()
            orig["origine"] = orig.apply(lambda r: "Québec" if r["est_quebecois"] == 1 else ("Canada hors QC" if r["region_admin"] == "Hors Québec (Canada)" else ("International" if r["region_admin"] == "International" else "Inconnu")), axis=1)
            oa = orig.groupby("origine")["montant_adjuge"].sum().reset_index()
            fig_o = px.pie(oa, names="origine", values="montant_adjuge", title="Répartition par origine (montant)", color_discrete_sequence=["#003366", "#2e86c1", "#e67e22", "#bdc3c7"])
            fig_o.update_layout(height=380)
            st.plotly_chart(fig_o, use_container_width=True)
        st.markdown("#### 📊 Répartition régionale")
        ra = dff[dff["est_quebecois"]==1].groupby("region_admin").agg(nb=("montant_adjuge", "count"), val=("montant_adjuge", "sum")).sort_values("val", ascending=False).reset_index()
        ra["val_fmt"] = ra["val"].apply(lambda x: f"{x:,.0f} $")
        ra.columns = ["Région", "Nb contrats", "Valeur", "Valeur ($)"]
        st.dataframe(ra[["Région", "Nb contrats", "Valeur ($)"]], use_container_width=True, hide_index=True)

# === TAB 3 — SECTEURS & TI ===
with tab3:
    st.subheader("💻 Portrait sectoriel")
    col_g, col_h = st.columns(2)
    macro = dff.groupby("secteur_macro").agg(m=("montant_adjuge", "sum"), n=("montant_adjuge", "count")).nlargest(12, "m").reset_index()
    with col_g:
        fg = px.bar(macro.sort_values("m"), y="secteur_macro", x=macro.sort_values("m")["m"]/1e9, orientation="h", title="Montant par secteur (G$)", color_discrete_sequence=["#003366"])
        fg.update_layout(height=450, yaxis_title="")
        st.plotly_chart(fg, use_container_width=True)
    with col_h:
        mp = dff.groupby("secteur_macro")["montant_adjuge"].sum().nlargest(8).reset_index()
        fh = px.pie(mp, names="secteur_macro", values="montant_adjuge", title="Parts relatives", color_discrete_sequence=COLORS)
        fh.update_layout(height=450)
        st.plotly_chart(fh, use_container_width=True)
    st.markdown("---")
    st.markdown("### 💻 Focus TI")
    dti = dff[dff["est_ti"]].copy()
    if dti.empty:
        st.info("Aucun contrat TI.")
    else:
        t1, t2, t3, t4 = st.columns(4)
        t1.metric("Contrats TI", f"{len(dti):,}")
        t2.metric("Montant TI", f"{dti['montant_adjuge'].sum()/1e9:.2f} G$")
        t3.metric("Part du total", f"{100*dti['montant_adjuge'].sum()/max(dff['montant_adjuge'].sum(),1):.1f}%")
        t4.metric("Montant moyen", f"{dti['montant_adjuge'].mean()/1e6:.1f} M$")
        ca, cb = st.columns(2)
        with ca:
            tio = dti.groupby("organisme")["montant_adjuge"].sum().nlargest(15).reset_index()
            f1 = px.bar(tio.sort_values("montant_adjuge"), y="organisme", x=tio.sort_values("montant_adjuge")["montant_adjuge"]/1e6, orientation="h", title="TI — Top organismes (M$)", color_discrete_sequence=["#1a5276"])
            f1.update_layout(height=480, yaxis_title="")
            st.plotly_chart(f1, use_container_width=True)
        with cb:
            tif = dti.groupby("fournisseur")["montant_adjuge"].sum().nlargest(15).reset_index()
            f2 = px.bar(tif.sort_values("montant_adjuge"), y="fournisseur", x=tif.sort_values("montant_adjuge")["montant_adjuge"]/1e6, orientation="h", title="TI — Top fournisseurs (M$)", color_discrete_sequence=["#2e86c1"])
            f2.update_layout(height=480, yaxis_title="")
            st.plotly_chart(f2, use_container_width=True)
        st.markdown("#### Sous-catégories TI")
        ts = dti.groupby("secteur_description").agg(m=("montant_adjuge", "sum"), n=("montant_adjuge", "count")).nlargest(15, "m").reset_index()
        ts["m_fmt"] = ts["m"].apply(lambda x: f"{x/1e6:.1f} M$")
        ts.columns = ["Sous-catégorie", "Montant", "Nb contrats", "Montant (M$)"]
        st.dataframe(ts[["Sous-catégorie", "Nb contrats", "Montant (M$)"]], use_container_width=True, hide_index=True)

# === TAB 4 — DÉPASSEMENTS ===
with tab4:
    st.subheader("⚠️ Dépassements de coûts")
    dep = dff[dff["a_depassement"]].copy()
    if dep.empty:
        st.warning("Aucun dépassement trouvé. Les contrats récents n'ont souvent pas encore d'écart.")
    else:
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Avec dépassement", f"{len(dep):,}")
        d2.metric("Taux moyen", f"{dep['taux_depassement'].mean():.1f}%")
        d3.metric("Écart total", f"{dep['ecart_prix'].sum()/1e6:.1f} M$")
        d4.metric("Écart médian", f"{dep['ecart_prix'].median()/1e3:.0f} k$")
        st.markdown("---")
        dim = st.radio("Analyser par :", ["Secteur", "Organisme", "Fournisseur", "Année"], horizontal=True, key="dep_dim")
        gcol = {"Secteur": "secteur_macro", "Organisme": "organisme", "Fournisseur": "fournisseur", "Année": "annee_ref"}[dim]
        da = dep.groupby(gcol).agg(ecart=("ecart_prix", "sum"), nb=("ecart_prix", "count"), taux=("taux_depassement", "mean")).nlargest(20, "ecart").reset_index()
        cd1, cd2 = st.columns(2)
        with cd1:
            fd1 = px.bar(da.sort_values("ecart"), y=gcol, x=da.sort_values("ecart")["ecart"]/1e6, orientation="h", title=f"Écart total par {dim.lower()} (M$)", color_discrete_sequence=[RED])
            fd1.update_layout(height=500, yaxis_title="")
            st.plotly_chart(fd1, use_container_width=True)
        with cd2:
            fd2 = px.bar(da.sort_values("taux"), y=gcol, x="taux", orientation="h", title=f"Taux moyen par {dim.lower()} (%)", color_discrete_sequence=["#e67e22"])
            fd2.update_layout(height=500, yaxis_title="")
            st.plotly_chart(fd2, use_container_width=True)
        fdist = px.histogram(dep, x="taux_depassement", nbins=50, title="Distribution des taux (%)", color_discrete_sequence=["#e74c3c"])
        fdist.update_layout(height=300)
        st.plotly_chart(fdist, use_container_width=True)
        st.markdown("#### 🔴 Plus gros dépassements")
        td = dep.nlargest(25, "ecart_prix")[["annee_ref", "organisme", "fournisseur", "secteur_description", "montant_adjuge", "montant_final", "ecart_prix", "taux_depassement"]].copy()
        td.columns = ["Année", "Organisme", "Fournisseur", "Secteur", "Adjugé ($)", "Final ($)", "Écart ($)", "Taux (%)"]
        for c in ["Adjugé ($)", "Final ($)", "Écart ($)"]: td[c] = td[c].apply(lambda x: f"{x:,.0f}")
        td["Taux (%)"] = td["Taux (%)"].apply(lambda x: f"{x:.1f}%")
        st.dataframe(td, use_container_width=True, hide_index=True)

# === TAB 5 — FOURNISSEURS ===
with tab5:
    st.subheader("🏢 Fournisseurs")
    nf = st.slider("Nombre", 5, 30, 15, key="nf")
    tf = dff.groupby("fournisseur").agg(m=("montant_adjuge", "sum"), n=("montant_adjuge", "count"), no=("organisme", "nunique"), e=("ecart_prix", "sum")).nlargest(nf, "m").reset_index()
    cf1, cf2 = st.columns(2)
    with cf1:
        ff1 = px.bar(tf.sort_values("m"), y="fournisseur", x=tf.sort_values("m")["m"]/1e9, orientation="h", title=f"Top {nf} — Montant (G$)", color_discrete_sequence=["#003366"])
        ff1.update_layout(height=500, yaxis_title="")
        st.plotly_chart(ff1, use_container_width=True)
    with cf2:
        ff2 = px.scatter(tf, x="n", y=tf["m"]/1e6, size="no", hover_name="fournisseur", title="Contrats vs Montant", color_discrete_sequence=["#2e86c1"])
        ff2.update_layout(height=500, xaxis_title="Nb contrats", yaxis_title="Montant (M$)")
        st.plotly_chart(ff2, use_container_width=True)
    st.markdown("#### 📊 Concentration")
    fa = dff.groupby("fournisseur")["montant_adjuge"].sum().sort_values(ascending=False).reset_index()
    fa["cp"] = 100 * fa["montant_adjuge"].cumsum() / fa["montant_adjuge"].sum()
    fa["r"] = range(1, len(fa)+1)
    t50 = int((fa["cp"] <= 50).sum())
    t80 = int((fa["cp"] <= 80).sum())
    fc = px.line(fa.head(300), x="r", y="cp", title="Concentration", color_discrete_sequence=["#003366"])
    fc.add_hline(y=50, line_dash="dash", line_color=RED, annotation_text=f"50% ({t50} fourn.)")
    fc.add_hline(y=80, line_dash="dash", line_color="#e67e22", annotation_text=f"80% ({t80} fourn.)")
    fc.update_layout(height=400, xaxis_title="Rang", yaxis_title="% cumulatif")
    st.plotly_chart(fc, use_container_width=True)
    st.markdown("#### 🔎 Recherche")
    rech = st.text_input("Nom (partiel)")
    if rech:
        res = dff[dff["fournisseur"].str.contains(rech, case=False, na=False)]
        if res.empty:
            st.info("Aucun résultat.")
        else:
            rs = res.groupby("fournisseur").agg(m=("montant_adjuge", "sum"), n=("montant_adjuge", "count"), no=("organisme", "nunique")).sort_values("m", ascending=False).head(20).reset_index()
            rs["m"] = rs["m"].apply(lambda x: f"{x:,.0f} $")
            rs.columns = ["Fournisseur", "Montant total", "Nb contrats", "Nb organismes"]
            st.dataframe(rs, use_container_width=True, hide_index=True)

# === TAB 6 — PORTRAIT RÉGIONAL ===
with tab6:
    st.subheader("🗺️ Portrait régional")
    if dff["region_admin"].nunique() <= 2:
        st.warning("Données régionales insuffisantes. Utilisez extract_seao.py.")
    else:
        dr = dff[~dff["region_admin"].isin(["Inconnue", "Hors Québec (Canada)", "International"])].copy()
        rm = dr.groupby("region_admin").agg(m=("montant_adjuge", "sum"), n=("montant_adjuge", "count")).sort_values("m", ascending=False).reset_index()
        cr1, cr2 = st.columns(2)
        with cr1:
            fr1 = px.bar(rm.sort_values("m"), y="region_admin", x=rm.sort_values("m")["m"]/1e6, orientation="h", title="Montant par région (M$)", color_discrete_sequence=["#003366"])
            fr1.update_layout(height=500, yaxis_title="")
            st.plotly_chart(fr1, use_container_width=True)
        with cr2:
            fr2 = px.bar(rm.sort_values("n"), y="region_admin", x="n", orientation="h", title="Nb contrats par région", color_discrete_sequence=["#1a5276"])
            fr2.update_layout(height=500, yaxis_title="")
            st.plotly_chart(fr2, use_container_width=True)
        ft = px.treemap(dr, path=["region_admin", "secteur_macro"], values="montant_adjuge", title="Régions × Secteurs", color_discrete_sequence=COLORS)
        ft.update_layout(height=500)
        st.plotly_chart(ft, use_container_width=True)
        st.markdown("#### 📋 Détail régional")
        rd = dr.groupby("region_admin").agg(nb=("montant_adjuge", "count"), val=("montant_adjuge", "sum"), moy=("montant_adjuge", "mean"), nf=("fournisseur", "nunique"), gre=("mode_adjudication", lambda x: 100*x.str.contains("gré", case=False, na=False).mean())).sort_values("val", ascending=False).reset_index()
        rd["val"] = rd["val"].apply(lambda x: f"{x/1e6:,.1f} M$")
        rd["moy"] = rd["moy"].apply(lambda x: f"{x/1e3:,.0f} k$")
        rd["gre"] = rd["gre"].apply(lambda x: f"{x:.0f}%")
        rd.columns = ["Région", "Nb contrats", "Valeur totale", "Montant moyen", "Nb fournisseurs", "% gré à gré"]
        st.dataframe(rd, use_container_width=True, hide_index=True)
