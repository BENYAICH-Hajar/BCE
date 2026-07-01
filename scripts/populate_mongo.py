"""
populate_mongo.py
==================
Étape 1 du pipeline : peupler MongoDB avec les entreprises belges depuis les
fichiers CSV KBO Open Data (enterprise.csv, denomination.csv, address.csv,
activity.csv).

Usage :
    python populate_mongo.py                     # Google / Apple / SNCB (test)
    python populate_mongo.py --all                # tout le KBO (actifs uniquement)
    python populate_mongo.py --bce 0878065378 0836157420

Config :
    MONGO_URI   (env var, défaut: mongodb://localhost:27017)
    KBO_DATA_DIR (env var, défaut: /home/jovyan/work/data/KBO)

Chaque entreprise devient UN document fusionné dans la collection
`enterprises`, avec `bce_number` (format normalisé sans points, ex.
"0878065378") comme clé pivot — c'est cette clé que la State DB et les DAGs
Airflow utiliseront ensuite.
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

# --- Config (toute surchargeable via variables d'env, défauts simples) ---
MONGO_URI    = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB     = os.environ.get("MONGO_DB", "bce")
MONGO_COLL   = os.environ.get("MONGO_COLLECTION", "enterprises")
KBO_DATA_DIR = Path(os.environ.get("KBO_DATA_DIR", "/home/jovyan/work/data/KBO"))

# Entreprises de test par défaut (numéro BCE sans points)
DEFAULT_BCE_NUMBERS = ["0878065378", "0836157420", "0203430576"]

# Formes juridiques qui ne nécessitent PAS de notaire (cf. strapor.py)
NO_NOTAIRE_FORMS = {"009", "017", "018", "025", "026", "027", "051", "052"}


def normalize_bce(numero: str) -> str:
    """'0878.065.378' ou '878065378' -> '0878065378' (10 chiffres, sans points)."""
    digits = "".join(c for c in numero if c.isdigit())
    return digits.zfill(10)


def to_csv_format(numero: str) -> str:
    """'0878065378' -> '0878.065.378' (format utilisé dans les CSV KBO)."""
    n = normalize_bce(numero)
    return f"{n[0:4]}.{n[4:7]}.{n[7:10]}"


# --------------------------------------------------------------------------
# Chargement des CSV KBO
# --------------------------------------------------------------------------

def load_kbo_tables(data_dir: Path) -> dict[str, pd.DataFrame]:
    log.info(f"Chargement des CSV KBO depuis {data_dir} ...")
    tables = {
        "enterprise":   pd.read_csv(data_dir / "enterprise.csv", dtype=str),
        "denomination": pd.read_csv(data_dir / "denomination.csv", dtype=str),
        "address":      pd.read_csv(data_dir / "address.csv", dtype=str),
        "activity":     pd.read_csv(data_dir / "activity.csv", dtype=str),
    }
    for name, df in tables.items():
        log.info(f"  {name:<13}: {len(df):>10,} lignes")
    return tables


# --------------------------------------------------------------------------
# Construction d'un document fusionné par entreprise
# --------------------------------------------------------------------------

def build_entity_doc(numero_csv: str, tables: dict[str, pd.DataFrame]) -> dict | None:
    """
    Construit un document fusionné prêt pour MongoDB à partir des tables KBO.
    `numero_csv` doit être au format CSV ("0878.065.378").
    """
    df_enterprise   = tables["enterprise"]
    df_denomination = tables["denomination"]
    df_address      = tables["address"]
    df_activity     = tables["activity"]

    base = df_enterprise[df_enterprise["EnterpriseNumber"] == numero_csv]
    if base.empty:
        log.warning(f"  {numero_csv} introuvable dans enterprise.csv")
        return None
    base_row = base.iloc[0]

    # --- dénomination officielle (priorité FR puis NL) ---
    denoms = df_denomination[df_denomination["EntityNumber"] == numero_csv]
    officielles = denoms[denoms["TypeOfDenomination"] == "001"].sort_values("Language")
    denomination = officielles["Denomination"].iloc[0] if not officielles.empty else None

    # --- adresse principale (siège social = REGO, sinon fallback) ---
    addrs = df_address[df_address["EntityNumber"] == numero_csv]
    siege = addrs[addrs["TypeOfAddress"] == "REGO"]
    if siege.empty:
        siege = addrs
    adresse = None
    if not siege.empty:
        a = siege.iloc[0]
        adresse = {
            "street_fr":   a.get("StreetFR"),
            "house_number": a.get("HouseNumber"),
            "zipcode":     a.get("Zipcode"),
            "municipality_fr": a.get("MunicipalityFR"),
        }

    # --- activités (codes NACE bruts, groupés par version) ---
    activites_raw = df_activity[df_activity["EntityNumber"] == numero_csv]
    activites = [
        {
            "nace_version": row["NaceVersion"],
            "nace_code":    row["NaceCode"],
            "classification": row["Classification"],
        }
        for _, row in activites_raw.iterrows()
    ]

    juridical_form = base_row.get("JuridicalForm")
    status = base_row.get("Status")  # ex: "AC" = actif

    doc = {
        "bce_number": normalize_bce(numero_csv),
        "bce_number_csv_format": numero_csv,
        "denomination": denomination,
        "juridical_form": juridical_form,
        "status": status,
        "is_active": status == "AC",
        "needs_notaire": (
            status == "AC" and juridical_form not in NO_NOTAIRE_FORMS
        ),
        "start_date": base_row.get("StartDate"),
        "address": adresse,
        "activities": activites,
        "source": "KBO_open_data",
        "updated_at": datetime.now(timezone.utc),
    }
    return doc


# --------------------------------------------------------------------------
# Écriture MongoDB (upsert, idempotent)
# --------------------------------------------------------------------------

def upsert_entities(docs: list[dict]) -> None:
    if not docs:
        log.warning("Aucun document à insérer.")
        return

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except PyMongoError as e:
        log.error(f"Impossible de joindre MongoDB ({MONGO_URI}) : {e}")
        log.error("Vérifie MONGO_URI (variable d'env) ou que le conteneur mongo tourne.")
        raise

    coll = client[MONGO_DB][MONGO_COLL]
    coll.create_index("bce_number", unique=True)

    ops = [
        UpdateOne({"bce_number": d["bce_number"]}, {"$set": d}, upsert=True)
        for d in docs
    ]
    result = coll.bulk_write(ops)
    log.info(
        f"MongoDB : {result.upserted_count} créés, "
        f"{result.modified_count} mis à jour "
        f"(collection '{MONGO_DB}.{MONGO_COLL}')"
    )
    client.close()


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bce", nargs="+", default=None,
        help="Numéros BCE spécifiques à charger (défaut: Google/Apple/SNCB pour test)",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Charger TOUTES les entreprises actives du KBO (gros volume)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    tables = load_kbo_tables(KBO_DATA_DIR)

    if args.all:
        numeros_csv = tables["enterprise"].loc[
            tables["enterprise"]["Status"] == "AC", "EnterpriseNumber"
        ].tolist()
        log.info(f"Mode --all : {len(numeros_csv):,} entreprises actives à charger")
    else:
        numeros = args.bce or DEFAULT_BCE_NUMBERS
        numeros_csv = [to_csv_format(n) for n in numeros]
        log.info(f"Mode test : {len(numeros_csv)} entreprise(s) -> {numeros_csv}")

    docs = []
    for numero_csv in numeros_csv:
        doc = build_entity_doc(numero_csv, tables)
        if doc:
            docs.append(doc)
            log.info(f"  ✓ {doc['bce_number']}  {doc['denomination']}")

    upsert_entities(docs)


if __name__ == "__main__":
    main()