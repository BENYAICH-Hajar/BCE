"""
hotellerie_nbb.py
=================
JOUR 2 - PART 2 : Ciblage hôtellerie + Scraping NBB

1. Filtre enterprise_silver pour extraire les entreprises hôtelières
2. Les charge dans la StateDB avec status=pending
3. Scrape les dépôts financiers NBB depuis 2021
4. Met à jour la StateDB (done/error)

Codes NACE hôtellerie : 55100, 55201, 55202, 55203, 55204,
                         55209, 55300, 55400, 55900

Usage :
    python scripts/hotellerie_nbb.py --list-only    # voir les entreprises sans scraper
    python scripts/hotellerie_nbb.py --dry-run      # voir sans télécharger
    python scripts/hotellerie_nbb.py                # scraping complet

Config :
    MONGO_URI    (env var, défaut: mongodb://localhost:27017)
    BRONZE_DIR   (env var, défaut: ./bronze)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

sys.path.insert(0, str(Path(__file__).parent))
from state_db import is_done, mark_pending, mark_done, mark_error

log = logging.getLogger(__name__)

MONGO_URI  = os.environ.get("MONGO_URI",   "mongodb://localhost:27017")
MONGO_DB   = os.environ.get("MONGO_DB",    "bce")
BRONZE_DIR = Path(os.environ.get("BRONZE_DIR", "./bronze"))
BASE_URL   = "https://consult.cbso.nbb.be/api"
SOURCE     = "nbb"

# Codes NACE hôtellerie
NACE_HOTELLERIE = {
    "55100", "55201", "55202", "55203", "55204",
    "55209", "55300", "55400", "55900",
}

# Formes juridiques exclues (entités publiques)
JURIDICAL_EXCLUDED = {
    "110", "114", "116", "117",          # entités publiques
    "301", "302", "303",                  # services fédéraux
    "310", "320", "330", "340", "350",   # autorités régionales
    "400", "411", "412", "413", "414",
    "415", "416", "417", "418", "419", "420",  # communes, CPAS...
}

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "fr-BE,fr;q=0.9",
}


# --------------------------------------------------------------------------
# PART 2A — Filtrage hôtellerie depuis enterprise_silver
# --------------------------------------------------------------------------

def get_hotellerie_enterprises(db) -> list[dict]:
    """
    Filtre enterprise_silver pour extraire les entreprises hôtelières.
    Critères :
    - Status = AC (actif)
    - TypeOfEnterprise = 2 (personne morale privée)
    - JuridicalForm pas dans les exclusions publiques
    - Au moins une activité MAIN avec NaceCode dans NACE_HOTELLERIE
    """
    log.info("🔍 Filtrage des entreprises hôtelières...")

    # On utilise enterprise_silver si disponible, sinon enterprises
    src = "enterprise_silver"
    if db[src].count_documents({}) == 0:
        src = "enterprises"
        log.warning(f"  enterprise_silver vide — fallback sur {src}")

    pipeline = [
        # Filtre de base
        {"$match": {
            "Status": "AC",
            
            "JuridicalForm": {"$nin": list(JURIDICAL_EXCLUDED)},
        }},
        # Lookup activités
        {"$lookup": {
            "from": "activities",
            "localField": "EnterpriseNumber",
            "foreignField": "EntityNumber",
            "as": "acts",
        }},
        # Garder uniquement si au moins une activité MAIN hôtellerie
        {"$match": {
            "acts": {"$elemMatch": {
                "Classification": "MAIN",
                "NaceCode": {"$in": list(NACE_HOTELLERIE)},
            }}
        }},
        # Projection
        {"$project": {
            "_id": 0,
            "EnterpriseNumber": 1,
            "JuridicalForm": 1,
            "denomination_principale": 1,
            "denomination": 1,
        }},
    ]

    results = list(db[src].aggregate(pipeline, allowDiskUse=True))
    log.info(f"  ✅ {len(results):,} entreprises hôtelières trouvées")
    return results


# --------------------------------------------------------------------------
# PART 2B — Chargement en StateDB
# --------------------------------------------------------------------------

def load_into_statedb(db, enterprises: list[dict]) -> int:
    """
    Charge les entreprises hôtelières dans la StateDB avec status=pending.
    Ne réécrit pas celles déjà done.
    """
    coll = db["downloads"]
    ops  = []
    for ent in enterprises:
        bce  = ent["EnterpriseNumber"]
        name = ent.get("denomination_principale") or ent.get("denomination", bce)
        ops.append(UpdateOne(
            {"bce_number": bce, "source": SOURCE, "deposit_id": "hotellerie_target"},
            {"$setOnInsert": {
                "bce_number":  bce,
                "denomination": name,
                "source":      SOURCE,
                "deposit_id":  "hotellerie_target",
                "status":      "pending",
                "year":        0,
                "hdfs_path":   None,
                "error_msg":   None,
                "updated_at":  datetime.now(timezone.utc),
            }},
            upsert=True,
        ))

    if ops:
        result = coll.bulk_write(ops, ordered=False)
        inserted = result.upserted_count
        log.info(f"  StateDB : {inserted:,} nouvelles entrées pending")
        return inserted
    return 0


# --------------------------------------------------------------------------
# PART 2C — Session NBB
# --------------------------------------------------------------------------

def make_session(bce: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    page_url = f"https://consult.cbso.nbb.be/consult-enterprise/{bce}"
    session.headers["Referer"] = page_url
    try:
        session.get(page_url, timeout=10)
    except Exception:
        pass
    return session


def get_deposits_since_2021(session: requests.Session, bce: str) -> list[dict]:
    """Récupère les dépôts financiers depuis 2021."""
    url = (
        f"{BASE_URL}/rs-consult/published-deposits"
        f"?page=0&size=20&enterpriseNumber={bce.replace(".", "").strip()}"
        f"&sort=periodEndDate,desc"
    )
    try:
        r = session.get(url, timeout=30)
        if r.status_code == 429:
            log.warning(f"  [{bce}] 429 Rate Limit — pause 60s")
            time.sleep(60)
            r = session.get(url, timeout=30)
        r.raise_for_status()
        deposits = r.json().get("content", [])
        # Filtrer >= 2021
        filtered = [
            d for d in deposits
            if int(d.get("periodEndDateYear", 0)) >= 2021
        ]
        return filtered
    except Exception as e:
        log.error(f"  [{bce}] Erreur get_deposits : {e}")
        return []


def download_csv(session: requests.Session, deposit_id: str) -> str | None:
    """Télécharge le CSV d'un dépôt."""
    url = f"{BASE_URL}/external/broker/public/deposits/consult/csv/{deposit_id}"
    try:
        r = session.get(url, timeout=30)
        if r.status_code == 429:
            log.warning(f"  429 Rate Limit CSV — pause 60s")
            time.sleep(60)
            r = session.get(url, timeout=30)
        r.raise_for_status()
        return r.text
    except Exception as e:
        log.error(f"  Erreur CSV {deposit_id} : {e}")
        return None


def download_pdf(session: requests.Session, deposit: dict, dest_dir: Path) -> Path | None:
    """Télécharge le PDF d'un dépôt."""
    deposit_id = str(deposit["id"])
    year       = deposit.get("periodEndDateYear", "unknown")
    reference  = deposit.get("reference", deposit_id)
    filename   = f"{year}_{reference}.pdf"
    dest       = dest_dir / filename

    if dest.exists():
        return dest

    url = f"{BASE_URL}/external/broker/public/deposits/pdf/{deposit_id}"
    try:
        r = session.get(url, timeout=30)
        if r.status_code == 429:
            time.sleep(60)
            r = session.get(url, timeout=30)
        r.raise_for_status()
        dest.write_bytes(r.content)
        return dest
    except Exception as e:
        log.error(f"  Erreur PDF {deposit_id} : {e}")
        return None


# --------------------------------------------------------------------------
# PART 2D — Scraping d'une entreprise
# --------------------------------------------------------------------------

def scrape_enterprise(bce: str, name: str, dry_run: bool = False) -> int:
    """
    Scrape les dépôts financiers d'une entreprise hôtelière.
    Retourne le nombre de fichiers téléchargés.
    """
    dest_dir = BRONZE_DIR / "nbb" / "hotellerie" / bce
    session  = make_session(bce)
    deposits = get_deposits_since_2021(session, bce)

    if not deposits:
        log.info(f"  [{bce}] Aucun dépôt depuis 2021")
        return 0

    log.info(f"  [{bce}] {name[:40]} — {len(deposits)} dépôts depuis 2021")

    if dry_run:
        for d in deposits:
            log.info(f"    [DRY-RUN] {d.get('periodEndDateYear')} — {d.get('reference')}")
        return len(deposits)

    dest_dir.mkdir(parents=True, exist_ok=True)
    downloaded = 0

    for deposit in deposits:
        deposit_id = str(deposit["id"])
        year       = int(deposit.get("periodEndDateYear", 0))
        reference  = deposit.get("reference", deposit_id)

        # Delta detection
        if is_done(bce, SOURCE, deposit_id):
            log.info(f"    [SKIP] {year} — {reference} (déjà téléchargé)")
            continue

        mark_pending(bce, SOURCE, deposit_id, year)

        # PDF
        pdf_path = download_pdf(session, deposit, dest_dir)

        # CSV (non-migré uniquement)
        csv_text = None
        if not deposit.get("migration"):
            csv_text = download_csv(session, deposit_id)
            if csv_text:
                csv_file = dest_dir / f"{year}_{reference}.csv"
                csv_file.write_text(csv_text, encoding="utf-8")

        if pdf_path:
            hdfs_path = f"bronze/nbb/hotellerie/{bce}/{year}_{reference}"
            mark_done(bce, SOURCE, deposit_id, hdfs_path)
            downloaded += 1
            log.info(f"    ✅ {year} — {reference}")
        else:
            mark_error(bce, SOURCE, deposit_id, "PDF téléchargement échoué")

        time.sleep(0.5)

    return downloaded


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list-only", action="store_true", help="Lister les entreprises sans scraper")
    parser.add_argument("--dry-run",   action="store_true", help="Voir sans télécharger")
    parser.add_argument("--limit",     type=int, default=0, help="Limiter le nombre d'entreprises")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Connexion MongoDB
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        log.info(f"✅ MongoDB connecté")
    except PyMongoError as e:
        log.error(f"❌ MongoDB inaccessible : {e}")
        return

    db = client[MONGO_DB]

    # Étape 1 : Filtrage hôtellerie
    enterprises = get_hotellerie_enterprises(db)

    if args.limit > 0:
        enterprises = enterprises[:args.limit]
        log.info(f"  Mode test : {len(enterprises)} entreprises")

    if not enterprises:
        log.warning("Aucune entreprise hôtelière trouvée.")
        return

    # Étape 2 : Chargement en StateDB
    load_into_statedb(db, enterprises)

    if args.list_only:
        log.info("\n=== Liste des entreprises hôtelières ===")
        for e in enterprises[:20]:
            name = e.get("denomination_principale") or e.get("denomination", "")
            log.info(f"  {e['EnterpriseNumber']}  {name}")
        if len(enterprises) > 20:
            log.info(f"  ... et {len(enterprises)-20} autres")
        return

    # Étape 3 : Scraping NBB
    log.info(f"\n🚀 Scraping NBB pour {len(enterprises)} entreprises hôtelières")
    if args.dry_run:
        log.info("MODE DRY-RUN — aucun fichier ne sera téléchargé")

    total       = 0
    consecutive_429 = 0

    for i, ent in enumerate(enterprises):
        bce  = ent["EnterpriseNumber"]
        name = ent.get("denomination_principale") or ent.get("denomination", bce)

        # Skip si déjà done
        if is_done(bce, SOURCE, "hotellerie_target"):
            log.info(f"[{i+1}/{len(enterprises)}] SKIP {bce} (déjà done)")
            continue

        log.info(f"\n[{i+1}/{len(enterprises)}] {bce} — {name[:50]}")

        try:
            count = scrape_enterprise(bce, name, dry_run=args.dry_run)
            total += count
            consecutive_429 = 0
            # Pause polie entre chaque entreprise
            time.sleep(2)
        except Exception as e:
            err = str(e)
            log.error(f"  Erreur : {err}")
            mark_error(bce, SOURCE, "hotellerie_target", err)
            if "429" in err:
                consecutive_429 += 1
                wait = min(300, 60 * consecutive_429)
                log.warning(f"  429 consécutifs={consecutive_429} — pause {wait}s")
                time.sleep(wait)

    log.info(f"\n✅ Scraping hôtellerie terminé — {total} fichiers téléchargés")
    log.info(f"   Dossier : {BRONZE_DIR / 'nbb' / 'hotellerie'}")


if __name__ == "__main__":
    main()