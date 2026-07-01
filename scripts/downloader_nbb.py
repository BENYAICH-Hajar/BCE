"""
downloader_nbb.py
=================
Étape 3 du pipeline : télécharge les comptes annuels NBB pour toutes
les entreprises dans MongoDB, avec delta detection via State DB.

- Lit bce.enterprises depuis MongoDB
- Vérifie bce.downloads (State DB) → skip si déjà téléchargé
- Télécharge PDF + CSV depuis consult.cbso.nbb.be
- Sauvegarde dans bronze/nbb/<bce_number>/
- Met à jour la State DB (done / error)

Usage :
    python scripts/downloader_nbb.py               # toutes les entreprises MongoDB
    python scripts/downloader_nbb.py --bce 0878065378   # une seule entreprise
    python scripts/downloader_nbb.py --dry-run     # voir sans télécharger

Config :
    MONGO_URI      (env var, défaut: mongodb://localhost:27017)
    BRONZE_DIR     (env var, défaut: ./bronze)
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from pymongo import MongoClient

# Import State DB (même dossier)
import sys
sys.path.insert(0, str(Path(__file__).parent))
from state_db import is_done, mark_pending, mark_done, mark_error

log = logging.getLogger(__name__)

# --- Config ---
MONGO_URI  = os.environ.get("MONGO_URI",   "mongodb://localhost:27017")
MONGO_DB   = os.environ.get("MONGO_DB",    "bce")
BRONZE_DIR = Path(os.environ.get("BRONZE_DIR", "./bronze"))
BASE_URL   = "https://consult.cbso.nbb.be/api"

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

SOURCE = "nbb"


# --------------------------------------------------------------------------
# Session NBB
# --------------------------------------------------------------------------

def make_session(enterprise_number: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    page_url = f"https://consult.cbso.nbb.be/consult-enterprise/{enterprise_number}"
    session.headers["Referer"] = page_url
    try:
        session.get(page_url, timeout=10)
    except Exception as e:
        log.warning(f"  Impossible d'initialiser la session NBB : {e}")
    return session


# --------------------------------------------------------------------------
# Récupération des dépôts disponibles
# --------------------------------------------------------------------------

def get_deposits(session: requests.Session, enterprise_number: str) -> list[dict]:
    url = (
        f"{BASE_URL}/rs-consult/published-deposits"
        f"?page=0&size=20&enterpriseNumber={enterprise_number}"
        f"&sort=periodEndDate,desc&sort=depositDate,desc"
    )
    try:
        r = session.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        deposits = data.get("content", [])
        log.info(f"  [{enterprise_number}] {len(deposits)} dépôts trouvés")
        return deposits
    except Exception as e:
        log.error(f"  [{enterprise_number}] Erreur get_deposits : {e}")
        return []


# --------------------------------------------------------------------------
# Téléchargement PDF
# --------------------------------------------------------------------------

def download_pdf(
    session: requests.Session,
    enterprise_number: str,
    deposit: dict,
    dest_dir: Path,
    dry_run: bool = False,
) -> Path | None:
    deposit_id = str(deposit["id"])
    year       = deposit.get("periodEndDateYear", "unknown")
    reference  = deposit.get("reference", deposit_id)

    # --- Delta detection : skip si déjà fait ---
    if is_done(enterprise_number, SOURCE, deposit_id):
        log.info(f"    [SKIP] PDF {year} (deposit {deposit_id}) déjà téléchargé")
        return None

    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{enterprise_number}_{year}_{reference}.pdf"
    dest     = dest_dir / filename

    if dry_run:
        log.info(f"    [DRY-RUN] PDF {year} → {dest}")
        return dest

    mark_pending(enterprise_number, SOURCE, deposit_id, int(year) if str(year).isdigit() else 0)

    try:
        url = f"{BASE_URL}/external/broker/public/deposits/pdf/{deposit_id}"
        r   = session.get(url, timeout=30)
        r.raise_for_status()
        dest.write_bytes(r.content)
        hdfs_path = f"bronze/nbb/{enterprise_number}/{filename}"
        mark_done(enterprise_number, SOURCE, deposit_id, hdfs_path)
        log.info(f"    [OK] PDF {year} → {filename} ({len(r.content)//1024} KB)")
        return dest
    except Exception as e:
        mark_error(enterprise_number, SOURCE, deposit_id, str(e))
        log.error(f"    [ERR] PDF {year} → {e}")
        return None


# --------------------------------------------------------------------------
# Téléchargement CSV + calcul KPIs
# --------------------------------------------------------------------------

def download_csv_and_kpis(
    session: requests.Session,
    enterprise_number: str,
    deposit: dict,
    dest_dir: Path,
    dry_run: bool = False,
) -> dict | None:
    deposit_id = str(deposit["id"])
    year       = deposit.get("periodEndDateYear", "unknown")
    reference  = deposit.get("reference", deposit_id)

    # Les dépôts migrés (anciens) n'ont pas de CSV
    if deposit.get("migration"):
        log.info(f"    [SKIP] CSV {year} — dépôt migré, pas de CSV disponible")
        return None

    csv_deposit_id = f"csv_{deposit_id}"
    if is_done(enterprise_number, SOURCE, csv_deposit_id):
        log.info(f"    [SKIP] CSV {year} déjà téléchargé")
        return None

    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{enterprise_number}_{year}_{reference}.csv"
    dest     = dest_dir / filename

    if dry_run:
        log.info(f"    [DRY-RUN] CSV {year} → {dest}")
        return None

    mark_pending(enterprise_number, SOURCE, csv_deposit_id, int(year) if str(year).isdigit() else 0)

    try:
        url      = f"{BASE_URL}/external/broker/public/deposits/consult/csv/{deposit_id}"
        r        = session.get(url, timeout=15)
        r.raise_for_status()
        dest.write_text(r.text, encoding="utf-8")
        hdfs_path = f"bronze/nbb/{enterprise_number}/{filename}"
        mark_done(enterprise_number, SOURCE, csv_deposit_id, hdfs_path)
        log.info(f"    [OK] CSV {year} → {filename}")

        # Calcul des KPIs depuis le CSV
        kpis = compute_kpis_from_csv(r.text, year, reference)
        return kpis
    except Exception as e:
        mark_error(enterprise_number, SOURCE, csv_deposit_id, str(e))
        log.error(f"    [ERR] CSV {year} → {e}")
        return None


def compute_kpis_from_csv(csv_text: str, year, reference: str) -> dict:
    """Calcule les KPIs financiers depuis le texte CSV."""
    df   = pd.read_csv(StringIO(csv_text), header=None, skiprows=1)
    codes = {}
    for _, row in df.iterrows():
        key = str(row[0]).strip()
        try:
            codes[key] = float(row[1])
        except (ValueError, TypeError):
            codes[key] = row[1]

    def get(code):
        return codes.get(code, 0.0)

    def pct(num, denom):
        return round(num / denom * 100, 2) if denom else None

    omzet        = get("70")
    cogs         = get("60")
    depreciation = get("630")
    ebit         = get("9901")
    net_profit   = get("9904")
    cash         = get("54/58")
    equity       = get("10/15")
    total_assets = get("20/58")
    fin_debt     = get("17") + get("43")
    gross_profit = omzet - cogs
    ebitda       = ebit + depreciation

    return {
        "year":             year,
        "reference":        reference,
        "chiffre_affaires": omzet,
        "ebitda":           ebitda,
        "ebit":             ebit,
        "resultat_net":     net_profit,
        "marge_brute_pct":  pct(gross_profit, omzet),
        "marge_nette_pct":  pct(net_profit, omzet),
        "tresorerie":       cash,
        "dettes_fin":       fin_debt,
        "fonds_propres":    equity,
        "total_actif":      total_assets,
        "autonomie_fin_pct": pct(equity, total_assets),
    }


# --------------------------------------------------------------------------
# Traitement d'une entreprise
# --------------------------------------------------------------------------

def process_enterprise(enterprise: dict, dry_run: bool = False) -> list[dict]:
    bce    = enterprise["bce_number"]
    name   = enterprise.get("denomination", bce)
    log.info(f"\n{'='*55}")
    log.info(f"  {name} ({bce})")
    log.info(f"{'='*55}")

    dest_dir = BRONZE_DIR / "nbb" / bce
    session  = make_session(bce)
    deposits = get_deposits(session, bce)

    if not deposits:
        log.info("  Aucun dépôt trouvé.")
        return []

    all_kpis = []
    for deposit in deposits:
        # PDF
        download_pdf(session, bce, deposit, dest_dir, dry_run)
        time.sleep(0.5)

        # CSV + KPIs
        kpis = download_csv_and_kpis(session, bce, deposit, dest_dir, dry_run)
        if kpis:
            all_kpis.append(kpis)
        time.sleep(0.5)

    return all_kpis


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bce", nargs="+", default=None, help="Numéros BCE spécifiques")
    parser.add_argument("--dry-run", action="store_true", help="Voir sans télécharger")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Lire les entreprises depuis MongoDB
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    coll   = client[MONGO_DB]["enterprises"]

    if args.bce:
        query = {"bce_number": {"$in": args.bce}}
    else:
        query = {"is_active": True}

    enterprises = list(coll.find(query, {"_id": 0}))
    log.info(f"{len(enterprises)} entreprise(s) à traiter depuis MongoDB")

    if args.dry_run:
        log.info("MODE DRY-RUN — aucun fichier ne sera téléchargé")

    all_results = []
    for enterprise in enterprises:
        kpis = process_enterprise(enterprise, dry_run=args.dry_run)
        for k in kpis:
            k["bce_number"]  = enterprise["bce_number"]
            k["denomination"] = enterprise.get("denomination")
            all_results.append(k)

    # Résumé final
    if all_results:
        df = pd.DataFrame(all_results)
        log.info("\n=== KPI Summary ===")
        cols = ["denomination", "year", "chiffre_affaires", "ebitda", "resultat_net", "marge_nette_pct"]
        available = [c for c in cols if c in df.columns]
        print(df[available].to_string(index=False))

    log.info("\n✅ Téléchargements NBB terminés — vérifie le dossier bronze/nbb/")


if __name__ == "__main__":
    main()