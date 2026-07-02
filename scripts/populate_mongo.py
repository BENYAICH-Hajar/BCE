"""
populate_mongo.py  (v2 — approche directe CSV → MongoDB)
=========================================================
Importe chaque CSV KBO directement dans sa propre collection MongoDB.
Pas de jointures en RAM — MongoDB fait les $lookup sur les clés partagées.

Collections créées :
    bce.enterprises       ← enterprise.csv
    bce.denominations     ← denomination.csv
    bce.addresses         ← address.csv
    bce.activities        ← activity.csv
    bce.branches          ← branch.csv          ✅ nouveau
    bce.establishments    ← establishment.csv    ✅ nouveau
    bce.contacts          ← contact.csv          ✅ nouveau

Clés partagées :
    EnterpriseNumber  — dans enterprises, branches
    EntityNumber      — dans denominations, addresses, activities,
                        establishments, contacts

Usage :
    python scripts/populate_mongo.py                  # tous les CSV
    python scripts/populate_mongo.py --collection enterprises  # un seul

Config :
    MONGO_URI      (env var, défaut: mongodb://localhost:27017)
    KBO_DATA_DIR   (env var, défaut: ./data/KBO)
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

log = logging.getLogger(__name__)

MONGO_URI    = os.environ.get("MONGO_URI",    "mongodb://localhost:27017")
MONGO_DB     = os.environ.get("MONGO_DB",     "bce")
KBO_DATA_DIR = Path(os.environ.get("KBO_DATA_DIR", "./data/KBO"))

COLLECTIONS = {
    "enterprises":    {"file": "enterprise.csv",    "index": "EnterpriseNumber"},
    "denominations":  {"file": "denomination.csv",  "index": "EntityNumber"},
    "addresses":      {"file": "address.csv",        "index": "EntityNumber"},
    "activities":     {"file": "activity.csv",       "index": "EntityNumber"},
    "branches":       {"file": "branch.csv",         "index": "EnterpriseNumber"},
    "establishments": {"file": "establishment.csv",  "index": "EnterpriseNumber"},
    "contacts":       {"file": "contact.csv",        "index": "EntityNumber"},
}

BATCH_SIZE = 5_000


def import_csv_to_collection(
    client: MongoClient,
    collection_name: str,
    csv_path: Path,
    index_key: str,
) -> None:
    if not csv_path.exists():
        log.warning(f"  ⚠ Fichier introuvable : {csv_path} — collection '{collection_name}' skippée")
        return

    coll = client[MONGO_DB][collection_name]
    coll.create_index(index_key)

    log.info(f"  📥 {collection_name} ← {csv_path.name}")

    total_inserted = 0
    chunk_num = 0

    for chunk in pd.read_csv(csv_path, dtype=str, chunksize=BATCH_SIZE):
        chunk = chunk.where(pd.notna(chunk), None)
        docs  = chunk.to_dict(orient="records")

        ops = [
            UpdateOne(
                {index_key: doc[index_key]},
                {"$set": doc},
                upsert=True,
            )
            for doc in docs
            if doc.get(index_key)
        ]

        if ops:
            result = coll.bulk_write(ops, ordered=False)
            total_inserted += result.upserted_count + result.modified_count

        chunk_num += 1
        if chunk_num % 10 == 0:
            log.info(f"    ... {chunk_num * BATCH_SIZE:,} lignes traitées")

    log.info(f"  ✅ {collection_name} : {total_inserted:,} documents importés")


def add_derived_fields(client: MongoClient) -> None:
    NO_NOTAIRE_FORMS = {"009", "017", "018", "025", "026", "027", "051", "052"}
    coll = client[MONGO_DB]["enterprises"]
    log.info("  Ajout des champs calculés (is_active, needs_notaire)...")
    coll.update_many({"Status": "AC"},          {"$set": {"is_active": True}})
    coll.update_many({"Status": {"$ne": "AC"}}, {"$set": {"is_active": False}})
    coll.update_many(
        {"is_active": True, "JuridicalForm": {"$nin": list(NO_NOTAIRE_FORMS)}},
        {"$set": {"needs_notaire": True}},
    )
    coll.update_many(
        {"$or": [{"is_active": False}, {"JuridicalForm": {"$in": list(NO_NOTAIRE_FORMS)}}]},
        {"$set": {"needs_notaire": False}},
    )
    total   = coll.count_documents({})
    active  = coll.count_documents({"is_active": True})
    notaire = coll.count_documents({"needs_notaire": True})
    log.info(f"  📊 {total:,} entreprises | {active:,} actives | {notaire:,} avec notaire")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collection", default=None, choices=list(COLLECTIONS.keys()))
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    log.info(f"Connexion MongoDB : {MONGO_URI}")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        log.info("✅ MongoDB connecté")
    except PyMongoError as e:
        log.error(f"❌ Impossible de joindre MongoDB : {e}")
        return

    to_import = (
        {args.collection: COLLECTIONS[args.collection]}
        if args.collection
        else COLLECTIONS
    )

    log.info(f"\n{len(to_import)} collection(s) à importer depuis {KBO_DATA_DIR}\n")

    for coll_name, config in to_import.items():
        csv_path = KBO_DATA_DIR / config["file"]
        import_csv_to_collection(client, coll_name, csv_path, config["index"])

    if "enterprises" in to_import:
        add_derived_fields(client)

    log.info("\n✅ Import terminé — toutes les collections sont dans MongoDB")
    log.info(f"   Base : {MONGO_DB}   |   Host : {MONGO_URI}")
    log.info("""
Exemple $lookup dans MongoDB Compass (onglet Aggregation) :
[
  { $match: { EnterpriseNumber: "0878.065.378" } },
  { $lookup: { from: "denominations",  localField: "EnterpriseNumber", foreignField: "EntityNumber", as: "denominations" } },
  { $lookup: { from: "addresses",      localField: "EnterpriseNumber", foreignField: "EntityNumber", as: "addresses" } },
  { $lookup: { from: "activities",     localField: "EnterpriseNumber", foreignField: "EntityNumber", as: "activities" } },
  { $lookup: { from: "branches",       localField: "EnterpriseNumber", foreignField: "EnterpriseNumber", as: "branches" } },
  { $lookup: { from: "establishments", localField: "EnterpriseNumber", foreignField: "EntityNumber", as: "establishments" } },
  { $lookup: { from: "contacts",       localField: "EnterpriseNumber", foreignField: "EntityNumber", as: "contacts" } }
]
    """)


if __name__ == "__main__":
    main()