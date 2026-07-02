"""
api.py
======
JOUR 3 - PART 2 : Backend FastAPI

Expose les données Gold et Silver au frontend React.

Endpoints :
    GET /search?q=...           Recherche entreprise par nom ou BCE
    GET /enterprise/{bce}       Fiche complète (Silver + Gold)
    GET /enterprise/{bce}/ratios Ratios financiers par année
    GET /enterprise/{bce}/statuts SSE streaming statuts notaire
    GET /health                 Santé de l'API

Usage :
    pip install fastapi uvicorn
    python scripts/api.py

    Ou avec uvicorn :
    uvicorn scripts.api:app --reload --port 8000
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pymongo import MongoClient

log = logging.getLogger(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.environ.get("MONGO_DB",  "bce")

app = FastAPI(
    title="BCE Pipeline API",
    description="API pour les données hôtellerie belge (Silver + Gold)",
    version="1.0.0",
)

# CORS pour le frontend React
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------------------------------------
# Connexion MongoDB
# --------------------------------------------------------------------------

def get_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return client[MONGO_DB]


# --------------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------------

@app.get("/health")
def health():
    """Vérifie que l'API et MongoDB sont opérationnels."""
    try:
        db = get_db()
        db.command("ping")
        return {
            "status": "ok",
            "mongodb": "connected",
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/search")
def search(q: str = Query(..., min_length=2)):
    """
    Recherche une entreprise par nom ou numéro BCE.
    Retourne les 20 premiers résultats.
    """
    db = get_db()

    # Normaliser la query
    q_clean = q.strip()

    # Chercher par numéro BCE (exact ou partiel)
    is_number = any(c.isdigit() for c in q_clean)

    if is_number:
        bce_norm = q_clean.replace(".", "").replace(" ", "")
        results = list(db["enterprise_silver"].find(
            {"EnterpriseNumber": {"$regex": bce_norm}},
            {"_id": 0, "EnterpriseNumber": 1, "denomination_principale": 1,
             "JuridicalFormLabel": 1, "StatusLabel": 1, "address": 1},
        ).limit(20))
    else:
        # Chercher par dénomination (insensible à la casse)
        results = list(db["enterprise_silver"].find(
            {"denomination_principale": {"$regex": q_clean, "$options": "i"}},
            {"_id": 0, "EnterpriseNumber": 1, "denomination_principale": 1,
             "JuridicalFormLabel": 1, "StatusLabel": 1, "address": 1},
        ).limit(20))

    return {"query": q, "count": len(results), "results": results}


@app.get("/enterprise/{bce}")
def get_enterprise(bce: str):
    """
    Fiche complète d'une entreprise :
    - Infos Silver (nom, adresse, activités, labels)
    - Ratios Gold (tous les exercices)
    """
    db = get_db()

    # Normaliser le BCE
    bce_norm = bce.replace(".", "").replace(" ", "").zfill(10)
    bce_csv  = f"{bce_norm[0:4]}.{bce_norm[4:7]}.{bce_norm[7:10]}"

    # Silver
    silver = db["enterprise_silver"].find_one(
        {"EnterpriseNumber": bce_csv},
        {"_id": 0},
    )
    if not silver:
        raise HTTPException(status_code=404, detail=f"Entreprise {bce} introuvable")

    # Gold
    gold = db["hotel_gold"].find_one(
        {"enterprise_number": bce_norm},
        {"_id": 0},
    )

    return {
        "enterprise_number": bce_norm,
        "silver": silver,
        "gold": gold,
    }


@app.get("/enterprise/{bce}/ratios")
def get_ratios(bce: str):
    """
    Retourne uniquement les ratios financiers par année pour une entreprise.
    """
    db = get_db()
    bce_norm = bce.replace(".", "").replace(" ", "").zfill(10)

    gold = db["hotel_gold"].find_one(
        {"enterprise_number": bce_norm},
        {"_id": 0, "years": 1, "schema_type": 1},
    )
    if not gold:
        raise HTTPException(status_code=404, detail=f"Pas de données Gold pour {bce}")

    return {
        "enterprise_number": bce_norm,
        "schema_type": gold.get("schema_type"),
        "years": gold.get("years", []),
    }


@app.get("/enterprise/{bce}/statuts")
async def stream_statuts(bce: str):
    """
    SSE — Stream les statuts notaire pour une entreprise.
    Chaque événement SSE contient un document notaire.
    """
    bce_norm = bce.replace(".", "").replace(" ", "").zfill(10)
    db = get_db()

    # Vérifier si déjà en base
    existing = list(db["notaire_statuts"].find(
        {"bce_number": bce_norm},
        {"_id": 0},
    ))

    async def event_generator():
        if existing:
            # Servir depuis la base
            for doc in existing:
                yield f"data: {json.dumps(doc, default=str)}\n\n"
                await asyncio.sleep(0.05)
            yield "data: {\"type\": \"done\", \"source\": \"cache\"}\n\n"
            return

        # Sinon scraper en live
        try:
            import sys
            from pathlib import Path
            sys.path.insert(0, str(Path(__file__).parent))
            from downloader_notaire import make_session, get_statutes, TorRotator

            tor     = TorRotator(use_tor=False)
            session = make_session(proxies=None)
            statuts = get_statutes(session, bce_norm)

            for s in statuts:
                doc = {
                    "bce_number":    bce_norm,
                    "documentId":    s.get("documentId"),
                    "documentTitle": s.get("documentTitle"),
                    "deedDate":      s.get("deedDate"),
                    "documentStatus": s.get("documentStatus"),
                }
                # Persister en base
                db["notaire_statuts"].update_one(
                    {"bce_number": bce_norm, "documentId": doc["documentId"]},
                    {"$set": doc},
                    upsert=True,
                )
                yield f"data: {json.dumps(doc, default=str)}\n\n"
                await asyncio.sleep(0.1)

        except Exception as e:
            yield f"data: {{\"type\": \"error\", \"message\": \"{str(e)}\"}}\n\n"

        yield "data: {\"type\": \"done\", \"source\": \"live\"}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/hotellerie/stats")
def hotellerie_stats():
    """Statistiques globales sur les hôtels en base."""
    db = get_db()
    gold_count = db["hotel_gold"].count_documents({})

    # Moyenne CA par année
    pipeline = [
        {"$unwind": "$years"},
        {"$group": {
            "_id": "$years.year",
            "avg_ca": {"$avg": "$years.chiffre_affaires"},
            "avg_marge_nette": {"$avg": "$years.ratios.marge_nette_pct"},
            "count": {"$sum": 1},
        }},
        {"$sort": {"_id": -1}},
        {"$limit": 5},
    ]
    stats_by_year = list(db["hotel_gold"].aggregate(pipeline))

    return {
        "total_enterprises": gold_count,
        "stats_by_year": stats_by_year,
    }


# --------------------------------------------------------------------------
# Lancement direct
# --------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)