#!/usr/bin/env python3
"""
SEAO — Pipeline automatisé complet
====================================
Étape 1 : Télécharge les fichiers JSON depuis Données Québec
Étape 2 : Extrait et enrichit les données en CSV

Usage:
    # Tout faire d'un coup
    python pipeline_seao.py

    # Étape 1 seulement (télécharger)
    python pipeline_seao.py --download-only

    # Étape 2 seulement (extraire, si les JSON sont déjà téléchargés)
    python pipeline_seao.py --extract-only

    # Filtrer par année
    python pipeline_seao.py --years 2023 2024 2025 2026

    # Forcer le re-téléchargement
    python pipeline_seao.py --force

Prérequis:
    pip install requests
"""

import json
import csv
import sys
import os
import time
import argparse
import hashlib
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except ImportError:
    print("❌ Le module 'requests' est requis. Installez-le avec : pip install requests")
    sys.exit(1)


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
INDEX_FILE = BASE_DIR / "index.json"
JSON_DIR = BASE_DIR / "json_files"
DATA_DIR = BASE_DIR / "data"

# Nombre de téléchargements simultanés
MAX_WORKERS = 3
# Pause entre les requêtes (secondes) pour ne pas surcharger le serveur
DELAY_BETWEEN_REQUESTS = 1.0
# Timeout pour le téléchargement (secondes)
DOWNLOAD_TIMEOUT = 300
# Nombre de tentatives en cas d'erreur
MAX_RETRIES = 3


# ─────────────────────────────────────────────
# Étape 0 : Télécharger/mettre à jour l'index
# ─────────────────────────────────────────────
INDEX_URL = "https://www.donneesquebec.ca/recherche/api/3/action/package_show?id=systeme-electronique-dappel-doffres-seao"


def download_index():
    """Télécharge l'index des ressources SEAO depuis Données Québec."""
    print("📋 Téléchargement de l'index SEAO...")

    if INDEX_FILE.exists():
        print(f"   Index existant trouvé: {INDEX_FILE}")
        with open(INDEX_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"   {data.get('nombre_ressources', '?')} ressources dans l'index")
        return data

    # Si pas d'index local, essayer de le télécharger
    try:
        resp = requests.get(INDEX_URL, timeout=30)
        resp.raise_for_status()
        api_data = resp.json()

        if api_data.get('success'):
            package = api_data['result']
            resources = package.get('resources', [])

            # Construire notre format d'index
            index = {
                "dataset": package.get('name', ''),
                "date_extraction": datetime.now().isoformat(),
                "nombre_ressources": len(resources),
                "ressources": []
            }

            for r in resources:
                if r.get('format', '').upper() == 'JSON' and r.get('url', '').endswith('.json'):
                    name = r.get('name', '') or r['url'].split('/')[-1]
                    # Extraire année du nom de fichier
                    annee = None
                    for part in name.replace('_', '-').split('-'):
                        if len(part) == 8 and part.isdigit():
                            annee = int(part[:4])
                            break

                    index['ressources'].append({
                        'id': r.get('id', ''),
                        'nom': name,
                        'format': 'JSON',
                        'url': r['url'],
                        'taille': r.get('size', 0) or 0,
                        'taille_lisible': f"{(r.get('size', 0) or 0) / 1e6:.1f} Mo",
                        'annee': annee or 2021,
                        'mois': '',
                        'date_creation': r.get('created', ''),
                        'date_modification': r.get('last_modified', ''),
                    })

            with open(INDEX_FILE, 'w', encoding='utf-8') as f:
                json.dump(index, f, ensure_ascii=False, indent=2)

            print(f"   ✅ Index téléchargé: {len(index['ressources'])} ressources JSON")
            return index
    except Exception as e:
        print(f"   ⚠️ Impossible de télécharger l'index: {e}")
        print(f"   Assurez-vous que le fichier index.json est présent dans {BASE_DIR}")
        sys.exit(1)


# ─────────────────────────────────────────────
# Étape 1 : Téléchargement des JSON
# ─────────────────────────────────────────────
def download_file(resource, force=False):
    """Télécharge un fichier JSON depuis Données Québec."""
    url = resource['url']
    nom = resource['nom']
    dest = JSON_DIR / nom

    # Vérifier si déjà téléchargé
    if dest.exists() and not force:
        expected_size = resource.get('taille', 0)
        actual_size = dest.stat().st_size
        # Tolérance de 10% sur la taille
        if expected_size == 0 or abs(actual_size - expected_size) / max(expected_size, 1) < 0.1:
            return {'status': 'skip', 'nom': nom, 'message': 'Déjà téléchargé'}

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
            resp.raise_for_status()

            with open(dest, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            time.sleep(DELAY_BETWEEN_REQUESTS)
            return {'status': 'ok', 'nom': nom, 'size': dest.stat().st_size}

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(5 * (attempt + 1))  # Backoff progressif
            else:
                return {'status': 'error', 'nom': nom, 'message': str(e)}

    return {'status': 'error', 'nom': nom, 'message': 'Max retries exceeded'}


def download_all(index, years=None, force=False):
    """Télécharge tous les fichiers JSON."""
    JSON_DIR.mkdir(parents=True, exist_ok=True)

    resources = index['ressources']
    if years:
        resources = [r for r in resources if r.get('annee') in years]

    print(f"\n📥 Téléchargement de {len(resources)} fichiers JSON...")
    total_size = sum(r.get('taille', 0) for r in resources)
    print(f"   Taille estimée: {total_size/1e9:.1f} Go")

    results = {'ok': 0, 'skip': 0, 'error': 0}
    errors = []

    # Téléchargement séquentiel (plus sûr pour un serveur gouvernemental)
    for i, resource in enumerate(resources):
        print(f"   [{i+1}/{len(resources)}] {resource['nom']}...", end=' ', flush=True)
        result = download_file(resource, force=force)

        if result['status'] == 'ok':
            print(f"✅ ({result['size']/1e6:.1f} Mo)")
            results['ok'] += 1
        elif result['status'] == 'skip':
            print("⏭️ déjà présent")
            results['skip'] += 1
        else:
            print(f"❌ {result['message']}")
            results['error'] += 1
            errors.append(result)

    print(f"\n📊 Résumé: {results['ok']} téléchargés, {results['skip']} déjà présents, {results['error']} erreurs")
    if errors:
        print("   Fichiers en erreur:")
        for e in errors:
            print(f"     - {e['nom']}: {e['message']}")

    return results


# ─────────────────────────────────────────────
# Étape 2 : Extraction (réutilise extract_seao.py)
# ─────────────────────────────────────────────
def run_extraction():
    """Lance l'extraction des JSON vers CSV enrichis."""
    extract_script = BASE_DIR / "extract_seao.py"

    if not extract_script.exists():
        print(f"❌ Script d'extraction non trouvé: {extract_script}")
        sys.exit(1)

    json_files = list(JSON_DIR.glob("*.json"))
    json_files = [f for f in json_files if f.name != 'index.json']

    if not json_files:
        print("❌ Aucun fichier JSON trouvé dans json_files/")
        sys.exit(1)

    print(f"\n🔄 Extraction de {len(json_files)} fichiers JSON...")

    # Importer et utiliser directement les fonctions de extract_seao.py
    import importlib.util
    spec = importlib.util.spec_from_file_location("extract_seao", str(extract_script))
    extract_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(extract_mod)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    all_rows = []

    for i, jf in enumerate(sorted(json_files)):
        print(f"   [{i+1}/{len(json_files)}] {jf.name}...", end=' ', flush=True)
        try:
            rows = extract_mod.process_file(jf)
            print(f"{len(rows)} contrats")
            all_rows.extend(rows)
        except Exception as e:
            print(f"❌ {e}")

    # Déduplication par ocid (garder le plus récent)
    print(f"\n🔄 Déduplication...")
    seen = {}
    for row in all_rows:
        ocid = row.get('ocid', '')
        if not ocid:
            continue
        if ocid not in seen or (row.get('date', '') > seen[ocid].get('date', '')):
            seen[ocid] = row

    deduped = list(seen.values())
    print(f"   {len(all_rows):,} → {len(deduped):,} contrats uniques")

    # Écrire le CSV final
    out_path = DATA_DIR / "SEAO_ENRICHI.csv"
    extract_mod.write_csv(deduped, out_path)

    # Stats
    regions = Counter(r.get('region_admin', 'Inconnue') for r in deduped)
    qc = sum(1 for r in deduped if r.get('est_quebecois') == 1)
    years = Counter(r.get('annee_signature') or r.get('annee') for r in deduped)
    dep = sum(1 for r in deduped if (r.get('taux_depassement') or 0) > 0)

    print(f"\n📊 Statistiques finales:")
    print(f"   Contrats uniques: {len(deduped):,}")
    print(f"   Fournisseurs québécois: {qc:,} ({100*qc/max(len(deduped),1):.1f}%)")
    print(f"   Avec dépassement: {dep:,}")
    print(f"   Années: {sorted(set(str(y) for y in years.keys() if y))}")
    print(f"   Top régions:")
    for reg, n in regions.most_common(10):
        print(f"     {reg}: {n:,}")

    return deduped


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Pipeline automatisé SEAO : téléchargement + extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python pipeline_seao.py                          # Tout faire
  python pipeline_seao.py --download-only          # Télécharger seulement
  python pipeline_seao.py --extract-only           # Extraire seulement
  python pipeline_seao.py --years 2024 2025 2026   # Années spécifiques
  python pipeline_seao.py --force                  # Re-télécharger tout
        """
    )
    parser.add_argument('--download-only', action='store_true', help='Télécharger les JSON seulement')
    parser.add_argument('--extract-only', action='store_true', help='Extraire les CSV seulement')
    parser.add_argument('--years', nargs='+', type=int, help='Années à traiter (ex: 2024 2025 2026)')
    parser.add_argument('--force', action='store_true', help='Forcer le re-téléchargement')

    args = parser.parse_args()

    print("=" * 60)
    print("  SEAO — Pipeline automatisé")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Charger l'index
    index = download_index()

    if not args.extract_only:
        # Étape 1 : Téléchargement
        download_all(index, years=args.years, force=args.force)

    if not args.download_only:
        # Étape 2 : Extraction
        run_extraction()

    print("\n" + "=" * 60)
    print("  🎉 Pipeline terminé!")
    print(f"  Données dans: {DATA_DIR}/SEAO_ENRICHI.csv")
    print(f"  Lancez le dashboard: streamlit run app.py")
    print("=" * 60)


if __name__ == '__main__':
    main()
