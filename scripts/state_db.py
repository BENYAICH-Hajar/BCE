"""
state_db.py
===========
Étape 2 du pipeline : State DB — tracker chaque téléchargement.

Elle garantit qu'on ne re-télécharge JAMAIS ce qui existe déjà.

Collection MongoDB : bce.downloads

Chaque document :
{
  "bce_number":  "0878065378",
  "source":      "nbb" | "notaire" | "ejustice",
  "deposit_id":  "123456",
  "year":        2023,
  "status":      "pending" | "done" | "error",
  "hdfs_path":   "bronze/nbb/0878065378/2023_123456.pdf",
  "error_msg":   null,
  "updated_at":  datetime
}

Usage direct pour tester :
    python scripts/state_db.py
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

log = logging.getLogger(__name__)

# --- Config ---
MONGO_URI  = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB   = os.environ.get("MONGO_DB",  "bce")
COLL_NAME  = "downloads"


def get_collection():
    """Retourne la collection MongoDB + crée les index si besoin."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    coll   = client[MONGO_DB][COLL_NAME]

    # Index unique : une seule entrée par (bce_number + source + deposit_id)
    coll.create_index(
        [("bce_number", ASCENDING), ("source", ASCENDING), ("deposit_id", ASCENDING)],
        unique=True,
        name="idx_bce_source_deposit",
    )
    # Index pour chercher rapidement par statut
    coll.create_index([("status", ASCENDING)], name="idx_status")

    return coll


# --------------------------------------------------------------------------
# Fonctions principales — utilisées par les DAGs Airflow ensuite
# --------------------------------------------------------------------------

def is_done(bce_number: str, source: str, deposit_id: str) -> bool:
    """
    Retourne True si ce fichier a déjà été téléchargé avec succès.
    C'est le check principal avant chaque téléchargement.
    """
    coll = get_collection()
    doc  = coll.find_one({
        "bce_number": bce_number,
        "source":     source,
        "deposit_id": str(deposit_id),
        "status":     "done",
    })
    return doc is not None


def mark_pending(bce_number: str, source: str, deposit_id: str, year: int) -> None:
    """
    Enregistre un téléchargement comme 'en cours'.
    Appelé AVANT de démarrer le téléchargement.
    """
    coll = get_collection()
    coll.update_one(
        {
            "bce_number": bce_number,
            "source":     source,
            "deposit_id": str(deposit_id),
        },
        {"$set": {
            "year":       year,
            "status":     "pending",
            "error_msg":  None,
            "hdfs_path":  None,
            "updated_at": datetime.now(timezone.utc),
        }},
        upsert=True,
    )
    log.info(f"  [pending] {source} | {bce_number} | {deposit_id} ({year})")


def mark_done(bce_number: str, source: str, deposit_id: str, hdfs_path: str) -> None:
    """
    Marque un téléchargement comme réussi + enregistre le chemin HDFS.
    Appelé APRÈS un téléchargement réussi.
    """
    coll = get_collection()
    coll.update_one(
        {
            "bce_number": bce_number,
            "source":     source,
            "deposit_id": str(deposit_id),
        },
        {"$set": {
            "status":     "done",
            "hdfs_path":  hdfs_path,
            "error_msg":  None,
            "updated_at": datetime.now(timezone.utc),
        }},
    )
    log.info(f"  [done]    {source} | {bce_number} | {deposit_id} → {hdfs_path}")


def mark_error(bce_number: str, source: str, deposit_id: str, error_msg: str) -> None:
    """
    Marque un téléchargement comme échoué + enregistre le message d'erreur.
    Appelé quand un téléchargement échoue.
    """
    coll = get_collection()
    coll.update_one(
        {
            "bce_number": bce_number,
            "source":     source,
            "deposit_id": str(deposit_id),
        },
        {"$set": {
            "status":     "error",
            "error_msg":  error_msg,
            "updated_at": datetime.now(timezone.utc),
        }},
    )
    log.warning(f"  [error]   {source} | {bce_number} | {deposit_id} → {error_msg}")


def get_pending(source: str = None) -> list[dict]:
    """Retourne tous les téléchargements en statut 'pending' ou 'error'."""
    coll   = get_collection()
    query  = {"status": {"$in": ["pending", "error"]}}
    if source:
        query["source"] = source
    return list(coll.find(query, {"_id": 0}))


def get_stats() -> dict:
    """Résumé rapide de l'état de tous les téléchargements."""
    coll = get_collection()
    pipeline = [
        {"$group": {"_id": {"source": "$source", "status": "$status"}, "count": {"$sum": 1}}}
    ]
    results = {}
    for r in coll.aggregate(pipeline):
        source = r["_id"]["source"]
        status = r["_id"]["status"]
        if source not in results:
            results[source] = {}
        results[source][status] = r["count"]
    return results


# --------------------------------------------------------------------------
# Test direct
# --------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    log.info("=== Test State DB ===")

    # --- Simulation d'un téléchargement NBB pour Google Belgium ---
    BCE   = "0878065378"
    SRC   = "nbb"
    DEP   = "DEP_TEST_001"
    YEAR  = 2023
    PATH  = f"bronze/nbb/{BCE}/{YEAR}_{DEP}.pdf"

    # 1. Vérifier si déjà téléchargé (doit retourner False)
    already = is_done(BCE, SRC, DEP)
    log.info(f"Déjà téléchargé ? {already}  (attendu: False)")

    # 2. Marquer comme pending
    mark_pending(BCE, SRC, DEP, YEAR)

    # 3. Simuler un succès → marquer done
    mark_done(BCE, SRC, DEP, PATH)

    # 4. Re-vérifier (doit retourner True maintenant)
    already = is_done(BCE, SRC, DEP)
    log.info(f"Déjà téléchargé ? {already}  (attendu: True)")

    # 5. Tester un erreur pour Apple Belgium
    mark_pending("0836157420", "notaire", "NOTAIRE_TEST_001", 2022)
    mark_error("0836157420", "notaire", "NOTAIRE_TEST_001", "HTTP 404 - document not found")

    # 6. Afficher les stats
    log.info("\n=== Stats downloads ===")
    stats = get_stats()
    for source, statuses in stats.items():
        log.info(f"  {source}: {statuses}")

    log.info("\n✅ State DB OK — collection 'bce.downloads' prête")