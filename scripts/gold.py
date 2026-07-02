"""
gold.py
=======
JOUR 3 - PART 1 : Gold Layer

Lit les CSVs PCMN depuis bronze/nbb/hotellerie/,
calcule les ratios financiers par exercice,
et consolide tout dans hotel_gold (1 document par entreprise).

Structure hotel_gold :
{
  "enterprise_number": "0400039084",
  "denomination": "...",
  "years": [
    {
      "year": 2023,
      "reference": "2024-00123456",
      "chiffre_affaires": 1234567.0,
      "marge_brute": 234567.0,
      "ebit": 123456.0,
      "resultat_net": 98765.0,
      "tresorerie": 50000.0,
      "dettes_financieres": 200000.0,
      "fonds_propres": 500000.0,
      "capital_souscrit": 100000.0,
      "ratios": {
        "marge_nette_pct": 8.0,
        "roe_pct": 19.75,
        "ratio_liquidite": 0.25,
        "taux_endettement_pct": 40.0
      }
    },
    ...
  ],
  "last_updated": "2026-07-02T..."
}

Usage :
    python scripts/gold.py                    # toutes les entreprises
    python scripts/gold.py --bce 0400039084   # une seule entreprise
    python scripts/gold.py --dry-run          # voir sans écrire en MongoDB

Config :
    MONGO_URI    (env var, défaut: mongodb://localhost:27017)
    BRONZE_DIR   (env var, défaut: ./bronze)
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

log = logging.getLogger(__name__)

MONGO_URI  = os.environ.get("MONGO_URI",   "mongodb://localhost:27017")
MONGO_DB   = os.environ.get("MONGO_DB",    "bce")
BRONZE_DIR = Path(os.environ.get("BRONZE_DIR", "./bronze"))
HOTEL_DIR  = BRONZE_DIR / "nbb" / "hotellerie"
GOLD_COLL  = "hotel_gold"

# --------------------------------------------------------------------------
# Mapping codes PCMN -> champs métier
# --------------------------------------------------------------------------

PCMN_MAP = {
    "70":   "chiffre_affaires",
    "60":   "achats",
    "71":   "variation_stocks",
    "9901": "ebit",
    "9904": "resultat_net",
    "54":   "tresorerie_54",
    "55":   "tresorerie_55",
    "17":   "dettes_fin_17",
    "43":   "dettes_fin_43",
    "10":   "fp_10",
    "11":   "fp_11",
    "12":   "fp_12",
    "13":   "fp_13",
    "14":   "fp_14",
    "15":   "fp_15",
    "100":  "capital_souscrit",
}


# --------------------------------------------------------------------------
# Parsing d'un CSV PCMN
# --------------------------------------------------------------------------

def parse_pcmn_csv(csv_path: Path) -> dict:
    """
    Parse un CSV PCMN NBB et retourne un dict de codes -> valeurs.
    Format : deux colonnes séparées par ; ou ,
    """
    codes = {}
    try:
        text = csv_path.read_text(encoding="utf-8", errors="ignore")
        lines = text.strip().split("\n")

        for line in lines[1:]:  # skip header
            line = line.strip()
            if not line:
                continue
            # Séparateur ; ou ,
            parts = line.replace(";", ",").split(",")
            if len(parts) < 2:
                continue
            code  = str(parts[0]).strip().strip('"')
            value = str(parts[1]).strip().strip('"')
            try:
                codes[code] = float(value.replace(" ", "").replace(",", "."))
            except ValueError:
                codes[code] = value

    except Exception as e:
        log.warning(f"  Erreur parsing {csv_path.name} : {e}")

    return codes


# --------------------------------------------------------------------------
# Calcul des ratios financiers
# --------------------------------------------------------------------------

def compute_financials(codes: dict) -> dict:
    """
    Calcule les métriques et ratios financiers depuis les codes PCMN.
    """
    def get(*keys) -> float:
        return sum(float(codes.get(k, 0) or 0) for k in keys)

    ca          = get("70")
    achats      = get("60")
    var_stocks  = get("71")
    ebit        = get("9901")
    net         = get("9904")
    tresorerie  = get("54") + get("55")
    dettes_fin  = get("17") + get("43")
    fonds_prop  = get("10") + get("11") + get("12") + get("13") + get("14") + get("15")
    capital     = get("100")
    marge_brute = ca - achats + var_stocks

    def pct(num, denom):
        return round(num / denom * 100, 2) if denom else None

    def ratio(num, denom):
        return round(num / denom, 4) if denom else None

    return {
        "chiffre_affaires":  ca,
        "marge_brute":       marge_brute,
        "ebit":              ebit,
        "resultat_net":      net,
        "tresorerie":        tresorerie,
        "dettes_financieres": dettes_fin,
        "fonds_propres":     fonds_prop,
        "capital_souscrit":  capital,
        "ratios": {
            "marge_nette_pct":      pct(net, ca),
            "roe_pct":              pct(net, fonds_prop),
            "ratio_liquidite":      ratio(tresorerie, dettes_fin),
            "taux_endettement_pct": pct(dettes_fin, fonds_prop),
            "marge_brute_pct":      pct(marge_brute, ca),
        }
    }


# --------------------------------------------------------------------------
# Traitement d'une entreprise
# --------------------------------------------------------------------------

def process_enterprise(bce_dir: Path, denomination: str = "") -> dict | None:
    """
    Lit tous les CSVs d'une entreprise et retourne le document Gold.
    """
    bce = bce_dir.name  # ex: "0400039084"
    csv_files = list(bce_dir.glob("*.csv"))

    if not csv_files:
        return None

    years = []
    for csv_path in sorted(csv_files):
        # Nom fichier : {year}_{reference}.csv
        parts = csv_path.stem.split("_", 1)
        year  = int(parts[0]) if parts[0].isdigit() else 0
        ref   = parts[1] if len(parts) > 1 else csv_path.stem

        codes = parse_pcmn_csv(csv_path)
        if not codes:
            continue

        financials = compute_financials(codes)
        financials["year"]      = year
        financials["reference"] = ref
        years.append(financials)

    if not years:
        return None

    # Trier par année décroissante
    years.sort(key=lambda x: x["year"], reverse=True)

    # Déterminer schema_type (full/abrégé/micro) selon la richesse des données
    latest = years[0]
    if latest.get("chiffre_affaires", 0) and latest.get("ebit", 0):
        schema_type = "full"
    elif latest.get("chiffre_affaires", 0):
        schema_type = "abrege"
    else:
        schema_type = "micro"

    return {
        "enterprise_number": bce,
        "denomination":      denomination,
        "years":             years,
        "schema_type":       schema_type,
        "last_updated":      datetime.now(timezone.utc),
    }


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bce",     default=None, help="Traiter une seule entreprise")
    parser.add_argument("--dry-run", action="store_true", help="Voir sans écrire en MongoDB")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    if not HOTEL_DIR.exists():
        log.error(f"❌ Dossier introuvable : {HOTEL_DIR}")
        log.error("   Lance d'abord hotellerie_nbb.py pour télécharger les CSVs")
        return

    # Connexion MongoDB
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        log.info(f"✅ MongoDB connecté")
        db   = client[MONGO_DB]
        coll = db[GOLD_COLL]
        coll.create_index("enterprise_number", unique=True)

        # Charger les dénominations depuis enterprise_silver
        silver = {
            doc["EnterpriseNumber"].replace(".", ""): doc.get("denomination_principale", "")
            for doc in db["enterprise_silver"].find(
                {}, {"_id": 0, "EnterpriseNumber": 1, "denomination_principale": 1}
            )
        }
        log.info(f"  {len(silver):,} dénominations chargées depuis enterprise_silver")

    except PyMongoError as e:
        log.error(f"❌ MongoDB inaccessible : {e}")
        return

    # Lister les entreprises à traiter
    if args.bce:
        bce_dirs = [HOTEL_DIR / args.bce.replace(".", "")]
    else:
        bce_dirs = [d for d in sorted(HOTEL_DIR.iterdir()) if d.is_dir()]

    log.info(f"📥 {len(bce_dirs)} entreprises à traiter depuis {HOTEL_DIR}")

    ops       = []
    processed = 0
    skipped   = 0

    for bce_dir in bce_dirs:
        bce   = bce_dir.name
        denom = silver.get(bce, "")

        doc = process_enterprise(bce_dir, denom)
        if not doc:
            skipped += 1
            continue

        log.info(f"  ✓ {bce}  {denom[:40]}  ({len(doc['years'])} exercices)")
        processed += 1

        if not args.dry_run:
            ops.append(UpdateOne(
                {"enterprise_number": bce},
                {"$set": doc},
                upsert=True,
            ))

        # Batch write tous les 500
        if len(ops) >= 500:
            coll.bulk_write(ops, ordered=False)
            ops = []

    # Dernier batch
    if ops:
        coll.bulk_write(ops, ordered=False)

    log.info(f"\n✅ Gold Layer terminée !")
    log.info(f"   Entreprises traitées : {processed}")
    log.info(f"   Sans CSVs (skippées) : {skipped}")
    log.info(f"   Collection           : {MONGO_DB}.{GOLD_COLL}")
    if not args.dry_run:
        log.info(f"   Total hotel_gold     : {coll.count_documents({}):,} documents")

    client.close()


if __name__ == "__main__":
    main()