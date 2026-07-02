"""
silver.py
=========
JOUR 2 - PART 1 : Création de la couche Silver

Lit enterprise_finale (ou enterprises) depuis MongoDB Bronze,
applique les transformations Silver, et écrit dans enterprise_silver.

Transformations :
1. Normalisation des dates (DD-MM-YYYY -> YYYY-MM-DD)
2. Déduplication des activités (même NaceCode + Classification)
3. Adresse unique (TypeOfAddress = REGO uniquement)
4. Dénomination principale en premier (TypeOfDenomination = 1)
5. Décodage des codes -> labels FR via code.csv

Usage :
    python scripts/silver.py
    python scripts/silver.py --batch 5000   # taille de batch

Config :
    MONGO_URI      (env var, défaut: mongodb://localhost:27017)
    KBO_DATA_DIR   (env var, défaut: ./data/KBO)
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

log = logging.getLogger(__name__)

MONGO_URI    = os.environ.get("MONGO_URI",    "mongodb://localhost:27017")
MONGO_DB     = os.environ.get("MONGO_DB",     "bce")
KBO_DATA_DIR = Path(os.environ.get("KBO_DATA_DIR", "./data/KBO"))

SRC_COLLECTION = "enterprises"   # Bronze
DST_COLLECTION = "enterprise_silver"  # Silver


# --------------------------------------------------------------------------
# Chargement des référentiels de labels depuis code.csv
# --------------------------------------------------------------------------

def load_code_labels(data_dir: Path) -> dict:
    """
    Charge code.csv et retourne un dict de lookup :
    {
      "JuridicalForm": {"610": "Société à responsabilité limitée", ...},
      "Status":        {"AC": "Actif", "STOP": "Arrêtée", ...},
      "Nace2008":      {"55100": "Hôtels et hébergement similaire", ...},
      "Nace2025":      {"55400": "Intermédiation pour l'hébergement", ...},
    }
    """
    csv_path = data_dir / "code.csv"
    if not csv_path.exists():
        log.warning(f"code.csv introuvable dans {data_dir} — labels non chargés")
        return {}

    df = pd.read_csv(csv_path, dtype=str)
    df = df[df["Language"] == "FR"]  # On garde uniquement les labels FR

    labels = {}
    for _, row in df.iterrows():
        cat  = str(row["Category"]).strip()
        code = str(row["Code"]).strip()
        desc = str(row["Description"]).strip()
        if cat not in labels:
            labels[cat] = {}
        labels[cat][code] = desc

    log.info(f"Labels chargés : {list(labels.keys())}")
    return labels


# --------------------------------------------------------------------------
# Transformations Silver
# --------------------------------------------------------------------------

def normalize_date(date_str: str | None) -> str | None:
    """DD-MM-YYYY -> YYYY-MM-DD"""
    if not date_str or not isinstance(date_str, str):
        return date_str
    parts = date_str.split("-")
    if len(parts) == 3 and len(parts[0]) == 2:
        return f"{parts[2]}-{parts[1]}-{parts[0]}"
    return date_str


def deduplicate_activities(activities: list[dict]) -> list[dict]:
    """
    Déduplique les activités : même NaceCode + même Classification = doublon.
    Codes différents (ex: 70220 vs 70200) sont conservés.
    """
    seen = set()
    result = []
    for act in activities or []:
        key = (str(act.get("NaceCode", "")), str(act.get("Classification", "")))
        if key not in seen:
            seen.add(key)
            result.append(act)
    return result


def get_rego_address(addresses: list[dict]) -> dict | None:
    """Retourne uniquement l'adresse REGO (siège social enregistré)."""
    for addr in addresses or []:
        if addr.get("TypeOfAddress") == "REGO":
            return addr
    # Fallback : première adresse disponible
    return addresses[0] if addresses else None


def sort_denominations(denominations: list[dict]) -> list[dict]:
    """Met la dénomination officielle (TypeOfDenomination=1) en premier."""
    official   = [d for d in denominations or [] if str(d.get("TypeOfDenomination", "")) == "001"]
    others     = [d for d in denominations or [] if str(d.get("TypeOfDenomination", "")) != "001"]
    return official + others


def add_labels(doc: dict, labels: dict) -> dict:
    """
    Ajoute les labels FR aux codes bruts.
    Modifie le document en place et le retourne.
    """
    # JuridicalForm
    jf_code = str(doc.get("JuridicalForm", ""))
    jf_labels = labels.get("JuridicalForm", {})
    if jf_code and jf_code in jf_labels:
        doc["JuridicalFormLabel"] = jf_labels[jf_code]

    # Status
    status_code = str(doc.get("Status", ""))
    status_labels = labels.get("Status", {})
    if status_code and status_code in status_labels:
        doc["StatusLabel"] = status_labels[status_code]

    # Activities NaceLabel
    nace2008 = labels.get("Nace2008", {})
    nace2025 = labels.get("Nace2025", {})
    nace_all = {**nace2008, **nace2025}

    for act in doc.get("activities", []):
        nace_code = str(act.get("NaceCode", ""))
        if nace_code in nace_all:
            act["NaceLabel"] = nace_all[nace_code]

    return doc


# --------------------------------------------------------------------------
# Transformation d'un document Bronze -> Silver
# --------------------------------------------------------------------------

def transform_to_silver(bronze_doc: dict, labels: dict) -> dict:
    """
    Transforme un document Bronze en document Silver.
    Ne modifie pas le Bronze original.
    """
    doc = {k: v for k, v in bronze_doc.items() if k != "_id"}

    # 1. Normalisation des dates
    doc["StartDate"] = normalize_date(doc.get("StartDate"))

    # 2. Adresse unique (REGO)
    if "addresses" in doc:
        doc["address"] = get_rego_address(doc.pop("addresses"))
    elif "address" in doc:
        pass  # déjà un objet unique

    # 3. Dénomination principale en premier
    if "denominations" in doc:
        doc["denominations"] = sort_denominations(doc["denominations"])
        # Extraire le nom principal
        if doc["denominations"]:
            doc["denomination_principale"] = doc["denominations"][0].get("Denomination")

    # 4. Déduplication des activités
    if "activities" in doc:
        doc["activities"] = deduplicate_activities(doc["activities"])

    # 5. Décodage labels
    doc = add_labels(doc, labels)

    # Métadonnées Silver
    doc["silver_at"]  = datetime.now(timezone.utc)
    doc["_silver_v"]  = 1

    return doc


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", type=int, default=1000, help="Taille de batch (défaut: 1000)")
    parser.add_argument("--limit", type=int, default=0,   help="Limiter le nombre de docs traités (test)")
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
        log.info(f"✅ MongoDB connecté : {MONGO_URI}")
    except PyMongoError as e:
        log.error(f"❌ MongoDB inaccessible : {e}")
        return

    db         = client[MONGO_DB]
    src_coll   = db[SRC_COLLECTION]
    dst_coll   = db[DST_COLLECTION]

    # Index sur la collection Silver
    dst_coll.create_index("EnterpriseNumber", unique=True)

    # Charger les labels
    labels = load_code_labels(KBO_DATA_DIR)

    # Compter les documents source
    total = src_coll.count_documents({})
    if args.limit > 0:
        total = min(total, args.limit)
    log.info(f"📥 {total:,} documents à transformer ({SRC_COLLECTION} -> {DST_COLLECTION})")

    # Traitement par batch
    cursor = src_coll.find({}, no_cursor_timeout=True)
    if args.limit > 0:
        cursor = cursor.limit(args.limit)

    batch      = []
    processed  = 0
    upserted   = 0

    for doc in cursor:
        silver_doc = transform_to_silver(doc, labels)
        key = silver_doc.get("EnterpriseNumber") or silver_doc.get("bce_number")

        if not key:
            continue

        batch.append(UpdateOne(
            {"EnterpriseNumber": key},
            {"$set": silver_doc},
            upsert=True,
        ))

        if len(batch) >= args.batch:
            result  = dst_coll.bulk_write(batch, ordered=False)
            upserted += result.upserted_count + result.modified_count
            processed += len(batch)
            batch = []
            log.info(f"  ... {processed:,} / {total:,} traités")

    # Dernier batch
    if batch:
        result  = dst_coll.bulk_write(batch, ordered=False)
        upserted += result.upserted_count + result.modified_count
        processed += len(batch)

    log.info(f"\n✅ Silver terminé !")
    log.info(f"   Documents traités  : {processed:,}")
    log.info(f"   Documents Silver   : {dst_coll.count_documents({}):,}")
    log.info(f"   Collection         : {MONGO_DB}.{DST_COLLECTION}")

    cursor.close()
    client.close()


if __name__ == "__main__":
    main()