# Tableau de bord SEAO — Marchés publics du Québec

Dashboard interactif pour analyser les données du Système électronique d'appel d'offres (SEAO) du Québec, couvrant la période **2018-2026**.

## 📊 Fonctionnalités

| Onglet | Description |
|--------|-------------|
| Aperçu général | KPIs, volumes, montants par année, modes d'adjudication |
| Indicateurs Stratégie | % fournisseurs québécois, contractants régionaux, cibles gouvernementales |
| Secteurs & TI | Analyse sectorielle, focus technologies de l'information |
| Dépassements de coûts | Taux, écarts, analyse croisée par secteur/organisme/fournisseur |
| Fournisseurs | Top fournisseurs, concentration du marché (courbe de Lorenz) |
| Portrait régional | Distribution par région administrative, treemap régions × secteurs |

## 🚀 Déploiement

### Local
```bash
pip install -r requirements.txt
streamlit run app.py
```

### Streamlit Cloud
1. Fork ou clone ce repo
2. Connectez-vous à [share.streamlit.io](https://share.streamlit.io)
3. Déployez en pointant vers `app.py`

## 📦 Données

- **Source** : [Données Québec — SEAO](https://www.donneesquebec.ca/recherche/dataset/systeme-electronique-dappel-doffres-seao)
- **Format** : Parquet (48 Mo compressé, 495k contrats)
- **Couverture** : 2018-2026, contrats adjugés uniquement
- **Enrichissement** : Code postal → région administrative, statut québécois, classification sectorielle

## 🔧 Pipeline de données

- `extract_seao.py` — Extraction JSON (OCDS) → CSV enrichi
- `pipeline_seao.py` — Téléchargement automatisé + extraction
- Extraction XML (2018-2020) via script séparé

## 📋 Indicateurs de la Stratégie gouvernementale

Suivi des objectifs de la [Stratégie gouvernementale des marchés publics](https://www.tresor.gouv.qc.ca/faire-affaire-avec-letat/marches-publics/strategie-gouvernementale-des-marches-publics/) :
- Part des fournisseurs québécois (cible : 52%)
- Contractants régionaux (cible : 60%)
- Part des appels d'offres publics
