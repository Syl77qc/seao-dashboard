#!/usr/bin/env python3
"""
SEAO — Extraction enrichie des fichiers JSON vers CSV
======================================================
Ce script transforme les fichiers JSON hebdomadaires du SEAO (format OCDS)
en fichiers CSV enrichis avec toutes les données utiles pour le dashboard.

Nouvelles colonnes vs l'ancien format :
- code_postal_fournisseur → mapping vers région administrative
- pays_fournisseur → détermination québécois/canadien/international
- neq → Numéro d'entreprise du Québec
- ville_fournisseur
- est_municipal → acheteur municipal ou non
- date_signature → date réelle de signature du contrat
- justification_mode → raison du mode d'adjudication (gré à gré, etc.)
- nb_amendments → nombre de modifications au contrat
- region_admin → région administrative du Québec (dérivée du code postal)

Usage:
    python extract_seao.py --input-dir ./json_files --output-dir ./data
"""

import json
import csv
import sys
import os
from pathlib import Path
from collections import defaultdict
import argparse
from datetime import datetime

# ─────────────────────────────────────────────
# Mapping FSA (3 premiers caractères du code postal) → Région administrative
# Basé sur Postes Canada et MAMH
# ─────────────────────────────────────────────
FSA_TO_REGION = {}

# G0A à G0T, G1A à G9Z — Est du Québec
# Capitale-Nationale
for fsa in ['G1A','G1B','G1C','G1E','G1G','G1H','G1J','G1K','G1L','G1M',
            'G1N','G1P','G1R','G1S','G1T','G1V','G1W','G1X','G1Y',
            'G2A','G2B','G2C','G2E','G2G','G2J','G2K','G2L','G2M','G2N',
            'G3A','G3B','G3C','G3E','G3G','G3H','G3J','G3K','G3L','G3M','G3N','G3Z']:
    FSA_TO_REGION[fsa] = 'Capitale-Nationale'

# Chaudière-Appalaches
for fsa in ['G0N','G0R','G0S',
            'G5A','G5B','G5C','G5R','G5V','G5X','G5Y','G5Z',
            'G6A','G6B','G6C','G6E','G6G','G6H','G6J','G6K','G6L',
            'G6P','G6R','G6S','G6T','G6V','G6W','G6X','G6Y','G6Z',
            'G7A','G7B']:
    FSA_TO_REGION[fsa] = 'Chaudière-Appalaches'

# Bas-Saint-Laurent
for fsa in ['G0K','G0L',
            'G5H','G5J','G5L','G5M','G5N','G5T']:
    FSA_TO_REGION[fsa] = 'Bas-Saint-Laurent'

# Saguenay–Lac-Saint-Jean
for fsa in ['G0V','G0W',
            'G7A','G7B','G7G','G7H','G7J','G7K','G7N','G7P','G7S','G7T',
            'G7X','G7Y','G7Z','G8A','G8B','G8C','G8E','G8G','G8H','G8J',
            'G8K','G8L','G8M','G8N','G8P','G8T','G8Z']:
    FSA_TO_REGION[fsa] = 'Saguenay–Lac-Saint-Jean'

# Mauricie
for fsa in ['G0X',
            'G8V','G8W','G8Y',
            'G9A','G9B','G9C','G9H','G9N','G9P','G9R','G9T','G9X']:
    FSA_TO_REGION[fsa] = 'Mauricie'

# Estrie
for fsa in ['J0B','J0E',
            'J1A','J1C','J1E','J1G','J1H','J1J','J1K','J1L','J1M','J1N',
            'J1R','J1S','J1T','J1X','J1Z',
            'J2A','J2B','J2C','J2E','J2G','J2H','J2J','J2K','J2L','J2N',
            'J2R','J2S','J2T','J2X']:
    FSA_TO_REGION[fsa] = 'Estrie'

# Montréal (île)
for fsa in ['H1A','H1B','H1C','H1E','H1G','H1H','H1J','H1K','H1L','H1M',
            'H1N','H1P','H1R','H1S','H1T','H1V','H1W','H1X','H1Y','H1Z',
            'H2A','H2B','H2C','H2E','H2G','H2H','H2J','H2K','H2L','H2M',
            'H2N','H2P','H2R','H2S','H2T','H2V','H2W','H2X','H2Y','H2Z',
            'H3A','H3B','H3C','H3E','H3G','H3H','H3J','H3K','H3L','H3M',
            'H3N','H3P','H3R','H3S','H3T','H3V','H3W','H3X','H3Y','H3Z',
            'H4A','H4B','H4C','H4E','H4G','H4H','H4J','H4K','H4L','H4M',
            'H4N','H4P','H4R','H4S','H4T','H4V','H4W','H4X','H4Y','H4Z',
            'H5A','H5B',
            'H8N','H8P','H8R','H8S','H8T','H8Y','H8Z',
            'H9A','H9B','H9C','H9E','H9G','H9H','H9J','H9K','H9R','H9S','H9W','H9X']:
    FSA_TO_REGION[fsa] = 'Montréal'

# Laval
for fsa in ['H7A','H7B','H7C','H7E','H7G','H7H','H7J','H7K','H7L','H7M',
            'H7N','H7P','H7R','H7S','H7T','H7V','H7W','H7X','H7Y']:
    FSA_TO_REGION[fsa] = 'Laval'

# Montérégie
for fsa in ['J0H','J0J','J0L',
            'J2W','J2Y',
            'J3A','J3B','J3E','J3G','J3H','J3L','J3M','J3N','J3P',
            'J3R','J3S','J3T','J3V','J3X','J3Y','J3Z',
            'J4G','J4H','J4J','J4K','J4L','J4M','J4N','J4P','J4R',
            'J4S','J4T','J4V','J4W','J4X','J4Y','J4Z',
            'J5A','J5B','J5C','J5J','J5K','J5L','J5M','J5R','J5T','J5V','J5W','J5X','J5Y','J5Z',
            'J6A','J6E','J6J','J6K','J6N','J6R','J6S','J6T','J6V','J6W','J6X','J6Y','J6Z',
            'J7R','J7T','J7V','J7W','J7X','J7Y']:
    FSA_TO_REGION[fsa] = 'Montérégie'

# Laurentides
for fsa in ['J0N','J0R','J0T','J0V','J0W',
            'J5Z','J7A','J7B','J7C','J7E','J7G','J7H','J7J','J7K','J7L','J7M','J7N','J7P',
            'J7Z','J8A','J8B','J8C','J8E','J8H','J8L','J8N']:
    FSA_TO_REGION[fsa] = 'Laurentides'

# Lanaudière
for fsa in ['J0K',
            'J5T','J5V','J5W','J5X',
            'J6A','J6E',
            'J4T']:
    FSA_TO_REGION[fsa] = 'Lanaudière'

# Outaouais
for fsa in ['J0X',
            'J8M','J8P','J8R','J8T','J8V','J8X','J8Y','J8Z',
            'J9A','J9B','J9H','J9J','J9L','J9V','J9X','J9Y','J9Z']:
    FSA_TO_REGION[fsa] = 'Outaouais'

# Abitibi-Témiscamingue
for fsa in ['J0Y','J0Z',
            'J9E','J9L','J9P','J9T','J9X','J9Y','J9Z']:
    FSA_TO_REGION[fsa] = 'Abitibi-Témiscamingue'

# Côte-Nord
for fsa in ['G0G','G0H',
            'G4R','G4S','G4T','G4V','G4W','G4X','G4Z',
            'G5A','G5B','G5C']:
    FSA_TO_REGION[fsa] = 'Côte-Nord'

# Nord-du-Québec
for fsa in ['G0E',
            'J0M','J0Y','J0Z',
            'J9E']:
    FSA_TO_REGION[fsa] = 'Nord-du-Québec'

# Gaspésie–Îles-de-la-Madeleine
for fsa in ['G0C','G0E',
            'G4P','G4R','G4S','G4T','G4V','G4W','G4X','G4Z',
            'G5A']:
    FSA_TO_REGION[fsa] = 'Gaspésie–Îles-de-la-Madeleine'

# Corrections additionnelles (FSA manquants)
# Montérégie suppléments
for fsa in ['J4B','J0S','J0G','J5N']:
    FSA_TO_REGION[fsa] = 'Montérégie'
# Lanaudière suppléments
for fsa in ['J0C','J0A']:
    FSA_TO_REGION[fsa] = 'Lanaudière'
# Laurentides suppléments
for fsa in ['J0P','J8G']:
    FSA_TO_REGION[fsa] = 'Laurentides'
# Capitale-Nationale suppléments
for fsa in ['G0A','G0T','G3S']:
    FSA_TO_REGION[fsa] = 'Capitale-Nationale'
# Chaudière-Appalaches suppléments
for fsa in ['G0M']:
    FSA_TO_REGION[fsa] = 'Chaudière-Appalaches'
# Bas-Saint-Laurent suppléments
for fsa in ['G0J','G1Q','G4A']:
    FSA_TO_REGION[fsa] = 'Bas-Saint-Laurent'
# Estrie supplément
for fsa in ['G0Y']:
    FSA_TO_REGION[fsa] = 'Estrie'
# Montréal supplément
for fsa in ['H9P']:
    FSA_TO_REGION[fsa] = 'Montréal'

# Centre-du-Québec
for fsa in ['G0P','G0Z',
            'G9A','G9B','G9C','G9H','G9P','G9T',
            'J1A','J1Z',
            'J2A','J2B','J2C','J2E','J2S']:
    FSA_TO_REGION[fsa] = 'Centre-du-Québec'


def get_region_from_postal(postal_code, country='CAN'):
    """Détermine la région administrative du Québec à partir du code postal."""
    if not postal_code or not isinstance(postal_code, str):
        return 'Inconnue'
    
    pc = postal_code.strip().replace(' ', '').upper()
    
    if len(pc) < 3:
        return 'Inconnue'
    
    # Vérifier si c'est un code postal québécois (G, H, J)
    first_char = pc[0]
    if first_char not in ('G', 'H', 'J'):
        if country == 'CAN':
            return 'Hors Québec (Canada)'
        else:
            return 'International'
    
    fsa = pc[:3]
    region = FSA_TO_REGION.get(fsa)
    
    if region:
        return region
    
    # Fallback par première lettre
    if first_char == 'H':
        return 'Montréal (région)'
    elif first_char == 'G':
        return 'Est du Québec'
    elif first_char == 'J':
        return 'Ouest du Québec'
    
    return 'Inconnue'


def is_quebecois(postal_code, country='CAN'):
    """Détermine si le fournisseur est québécois."""
    if not postal_code:
        return 0
    pc = postal_code.strip().replace(' ', '').upper()
    if len(pc) >= 1 and pc[0] in ('G', 'H', 'J'):
        return 1
    return 0


def extract_release(release):
    """Extrait les données d'un release OCDS en un dict plat pour le CSV."""
    
    # Identifier buyer et supplier
    buyer_info = {}
    supplier_info = {}
    
    for party in release.get('parties', []):
        roles = party.get('roles', [])
        if 'buyer' in roles:
            buyer_info = party
        if 'supplier' in roles:
            supplier_info = party
    
    # Tender
    tender = release.get('tender', {})
    items = tender.get('items', [])
    
    # Classification UNSPSC
    secteur_unspsc = ''
    secteur_description = ''
    secteur_unspsc_parent = ''
    secteur_description_parent = ''
    if items:
        item = items[0]
        classification = item.get('classification', {})
        secteur_unspsc = classification.get('id', '')
        secteur_description = classification.get('description', '')
        # Classification parent
        addl = item.get('additionalClassifications', [])
        if addl:
            secteur_unspsc_parent = addl[0].get('id', '')
            secteur_description_parent = addl[0].get('description', '')
    
    # Awards
    awards = release.get('awards', [])
    award = awards[0] if awards else {}
    montant_adjuge = award.get('value', {}).get('amount', 0) or 0
    date_award = award.get('date', '')
    
    # Contracts
    contracts = release.get('contracts', [])
    contract = contracts[0] if contracts else {}
    montant_final = contract.get('value', {}).get('amount', 0) or 0
    date_signature = contract.get('dateSigned', '')
    
    # Amendments
    amendments = contract.get('amendments', [])
    nb_amendments = len(amendments)
    
    # Dépassement
    ecart_prix = montant_final - montant_adjuge if montant_adjuge > 0 else 0
    taux_depassement = (ecart_prix / montant_adjuge * 100) if montant_adjuge > 0 else 0
    
    # Supplier address
    supplier_addr = supplier_info.get('address', {})
    code_postal = supplier_addr.get('postalCode', '')
    pays = supplier_addr.get('countryName', '')
    ville = supplier_addr.get('locality', '')
    
    # Buyer details
    est_municipal = buyer_info.get('details', {}).get('municipal', '0')
    
    # Année du contrat (de la date de publication)
    date_str = release.get('date', '')
    annee = ''
    if date_str:
        try:
            annee = date_str[:4]
        except:
            pass
    
    # Année de signature
    annee_signature = ''
    if date_signature:
        try:
            annee_signature = date_signature[:4]
        except:
            pass
    
    return {
        'ocid': release.get('ocid', ''),
        'annee': annee,
        'date': date_str,
        'date_signature': date_signature,
        'annee_signature': annee_signature,
        'organisme': buyer_info.get('name', ''),
        'organisme_id': buyer_info.get('id', ''),
        'est_municipal': est_municipal,
        'fournisseur': supplier_info.get('name', ''),
        'fournisseur_id': supplier_info.get('id', ''),
        'neq': supplier_info.get('details', {}).get('neq', ''),
        'code_postal_fournisseur': code_postal,
        'ville_fournisseur': ville,
        'pays_fournisseur': pays,
        'region_admin': get_region_from_postal(code_postal, pays),
        'est_quebecois': is_quebecois(code_postal, pays),
        'mode_adjudication': tender.get('procurementMethodDetails', ''),
        'justification_mode': tender.get('procurementMethodRationale', ''),
        'secteur_unspsc': secteur_unspsc,
        'secteur_description': secteur_description,
        'secteur_unspsc_parent': secteur_unspsc_parent,
        'secteur_description_parent': secteur_description_parent,
        'montant_adjuge': montant_adjuge,
        'montant_final': montant_final,
        'ecart_prix': round(ecart_prix, 2),
        'taux_depassement': round(taux_depassement, 2),
        'nb_amendments': nb_amendments,
        'titre': tender.get('title', ''),
        'statut': contract.get('status', award.get('status', '')),
    }


# Colonnes du CSV de sortie
CSV_COLUMNS = [
    'ocid', 'annee', 'date', 'date_signature', 'annee_signature',
    'organisme', 'organisme_id', 'est_municipal',
    'fournisseur', 'fournisseur_id', 'neq',
    'code_postal_fournisseur', 'ville_fournisseur', 'pays_fournisseur',
    'region_admin', 'est_quebecois',
    'mode_adjudication', 'justification_mode',
    'secteur_unspsc', 'secteur_description',
    'secteur_unspsc_parent', 'secteur_description_parent',
    'montant_adjuge', 'montant_final', 'ecart_prix', 'taux_depassement',
    'nb_amendments', 'titre', 'statut',
]


def process_file(json_path):
    """Traite un fichier JSON SEAO et retourne une liste de dicts."""
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    releases = data.get('releases', [])
    rows = []
    for release in releases:
        try:
            row = extract_release(release)
            rows.append(row)
        except Exception as e:
            print(f"  ⚠️ Erreur sur release {release.get('ocid', '?')}: {e}", file=sys.stderr)
    
    return rows


def main():
    parser = argparse.ArgumentParser(description='Extraction enrichie SEAO JSON → CSV')
    parser.add_argument('--input-dir', '-i', required=True, help='Dossier contenant les fichiers JSON SEAO')
    parser.add_argument('--output-dir', '-o', required=True, help='Dossier de sortie pour les CSV')
    parser.add_argument('--by-year', action='store_true', help='Grouper les sorties par année')
    parser.add_argument('--single-file', action='store_true', help='Un seul CSV de sortie')
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    json_files = sorted(input_dir.glob('*.json'))
    # Exclure index.json
    json_files = [f for f in json_files if f.name != 'index.json']
    
    if not json_files:
        print(f"⚠️ Aucun fichier JSON trouvé dans {input_dir}")
        sys.exit(1)
    
    print(f"📂 {len(json_files)} fichiers JSON à traiter")
    
    all_rows = []
    rows_by_year = defaultdict(list)
    
    for i, jf in enumerate(json_files):
        print(f"  [{i+1}/{len(json_files)}] {jf.name}...", end=' ')
        rows = process_file(jf)
        print(f"{len(rows)} contrats")
        
        all_rows.extend(rows)
        for row in rows:
            year = row.get('annee_signature') or row.get('annee') or 'inconnu'
            rows_by_year[year].append(row)
    
    # Dédupliquer par ocid (garder le plus récent)
    seen = {}
    for row in all_rows:
        ocid = row['ocid']
        if ocid not in seen or row['date'] > seen[ocid]['date']:
            seen[ocid] = row
    
    deduped = list(seen.values())
    print(f"\n✅ Total: {len(all_rows)} → {len(deduped)} contrats uniques (après déduplication)")
    
    # Stats rapides
    regions = defaultdict(int)
    qc_count = 0
    for r in deduped:
        regions[r['region_admin']] += 1
        if r['est_quebecois']:
            qc_count += 1
    
    print(f"   Fournisseurs québécois: {qc_count}/{len(deduped)} ({100*qc_count/len(deduped):.1f}%)")
    top_regions = dict(sorted(regions.items(), key=lambda x: -x[1])[:10])
    print(f"   Régions: {top_regions}")
    
    if args.single_file:
        out_path = output_dir / 'SEAO_ENRICHI.csv'
        write_csv(deduped, out_path)
    elif args.by_year:
        # Regrouper les dédupliqués par année
        by_year = defaultdict(list)
        for row in deduped:
            year = row.get('annee_signature') or row.get('annee') or 'inconnu'
            by_year[year].append(row)
        
        for year, rows in sorted(by_year.items()):
            out_path = output_dir / f'SEAO_ENRICHI_{year}.csv'
            write_csv(rows, out_path)
    else:
        out_path = output_dir / 'SEAO_ENRICHI.csv'
        write_csv(deduped, out_path)
    
    print(f"\n🎉 Extraction terminée!")


def write_csv(rows, path):
    """Écrit les lignes dans un fichier CSV."""
    with open(path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    print(f"   📄 {path.name}: {len(rows)} lignes")


if __name__ == '__main__':
    main()
