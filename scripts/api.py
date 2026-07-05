"""
api.py — Backend FastAPI BCE Hôtellerie
"""
from __future__ import annotations
import asyncio, json, logging, os
from datetime import datetime
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pymongo import MongoClient

log = logging.getLogger(__name__)
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.environ.get("MONGO_DB",  "bce")

app = FastAPI(title="BCE Pipeline API", description="API pour les données hôtellerie belge (Silver + Gold)", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def get_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return client[MONGO_DB]

@app.get("/health")
def health():
    try:
        db = get_db()
        db.command("ping")
        return {"status": "ok", "mongodb": "connected", "timestamp": datetime.utcnow().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

@app.get("/search")
def search(q: str = Query(..., min_length=2)):
    db = get_db()
    q_clean = q.strip()
    is_number = any(c.isdigit() for c in q_clean)

    hotel_bce_set = set(
        doc["enterprise_number"]
        for doc in db["hotel_gold"].find({}, {"_id": 0, "enterprise_number": 1})
    )

    if is_number:
        bce_norm = q_clean.replace(".", "").replace(" ", "")
        candidates = list(db["enterprise_silver"].find(
            {"EnterpriseNumber": {"$regex": bce_norm}},
            {"_id": 0, "EnterpriseNumber": 1, "denomination_principale": 1, "JuridicalFormLabel": 1, "StatusLabel": 1, "address": 1},
        ).limit(100))
    else:
        candidates = list(db["enterprise_silver"].find(
            {"denomination_principale": {"$regex": q_clean, "$options": "i"}},
            {"_id": 0, "EnterpriseNumber": 1, "denomination_principale": 1, "JuridicalFormLabel": 1, "StatusLabel": 1, "address": 1},
        ).limit(100))

    results = [r for r in candidates if r["EnterpriseNumber"].replace(".", "") in hotel_bce_set][:20]
    return {"query": q, "count": len(results), "results": results}

@app.get("/enterprise/{bce}")
def get_enterprise(bce: str):
    db = get_db()
    bce_norm = bce.replace(".", "").replace(" ", "").zfill(10)
    bce_csv  = f"{bce_norm[0:4]}.{bce_norm[4:7]}.{bce_norm[7:10]}"
    silver = db["enterprise_silver"].find_one({"EnterpriseNumber": bce_csv}, {"_id": 0})
    if not silver:
        raise HTTPException(status_code=404, detail=f"Entreprise {bce} introuvable")
    gold = db["hotel_gold"].find_one({"enterprise_number": bce_norm}, {"_id": 0})
    return {"enterprise_number": bce_norm, "silver": silver, "gold": gold}

@app.get("/enterprise/{bce}/ratios")
def get_ratios(bce: str):
    db = get_db()
    bce_norm = bce.replace(".", "").replace(" ", "").zfill(10)
    gold = db["hotel_gold"].find_one({"enterprise_number": bce_norm}, {"_id": 0, "years": 1, "schema_type": 1})
    if not gold:
        raise HTTPException(status_code=404, detail=f"Pas de données Gold pour {bce}")
    return {"enterprise_number": bce_norm, "schema_type": gold.get("schema_type"), "years": gold.get("years", [])}

@app.get("/enterprise/{bce}/statuts")
async def stream_statuts(bce: str):
    bce_norm = bce.replace(".", "").replace(" ", "").zfill(10)
    db = get_db()
    existing = list(db["notaire_statuts"].find({"bce_number": bce_norm}, {"_id": 0}))

    async def event_generator():
        if existing:
            for doc in existing:
                yield f"data: {json.dumps(doc, default=str)}\n\n"
                await asyncio.sleep(0.05)
            yield 'data: {"type": "done", "source": "cache"}\n\n'
            return
        try:
            import sys
            from pathlib import Path
            sys.path.insert(0, str(Path(__file__).parent))
            from downloader_notaire import make_session, get_statutes
            session = make_session(proxies=None)
            statuts = get_statutes(session, bce_norm)
            for s in statuts:
                doc = {"bce_number": bce_norm, "documentId": s.get("documentId"), "documentTitle": s.get("documentTitle"), "deedDate": s.get("deedDate"), "documentStatus": s.get("documentStatus")}
                db["notaire_statuts"].update_one({"bce_number": bce_norm, "documentId": doc["documentId"]}, {"$set": doc}, upsert=True)
                yield f"data: {json.dumps(doc, default=str)}\n\n"
                await asyncio.sleep(0.1)
        except Exception as e:
            yield f'data: {{"type": "error", "message": "{str(e)}"}}\n\n'
        yield 'data: {"type": "done", "source": "live"}\n\n'

    return StreamingResponse(event_generator(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.get("/hotellerie/stats")
def hotellerie_stats():
    db = get_db()
    gold_count = db["hotel_gold"].count_documents({})
    pipeline = [
        {"$unwind": "$years"},
        {"$group": {"_id": "$years.year", "avg_ca": {"$avg": "$years.chiffre_affaires"}, "avg_marge_nette": {"$avg": "$years.ratios.marge_nette_pct"}, "count": {"$sum": 1}}},
        {"$sort": {"_id": -1}},
        {"$limit": 5},
    ]
    stats_by_year = list(db["hotel_gold"].aggregate(pipeline))
    return {"total_enterprises": gold_count, "stats_by_year": stats_by_year}

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)