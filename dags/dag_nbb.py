from __future__ import annotations
import logging, os, sys
from datetime import datetime
from pathlib import Path
from airflow.sdk import dag, task

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
log = logging.getLogger(__name__)
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.environ.get("MONGO_DB",  "bce")

@dag(dag_id="bce_download_nbb", schedule="0 2 * * 0", start_date=datetime(2026,1,1), catchup=False, tags=["bce","nbb"])
def bce_download_nbb():

    @task()
    def get_enterprises():
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI)
        ents = list(client[MONGO_DB]["enterprises"].find({"is_active": True}, {"_id":0,"EnterpriseNumber":1}))
        log.info(f"{len(ents)} entreprises trouvées")
        client.close()
        return ents

    @task()
    def download_nbb(enterprises):
        from downloader_nbb import process_enterprise
        total = 0
        for ent in enterprises:
            bce = ent.get("EnterpriseNumber","").replace(".","").zfill(10)
            try:
                kpis = process_enterprise({"bce_number": bce, "denomination": bce})
                total += len(kpis)
            except Exception as e:
                log.error(f"Erreur {bce}: {e}")
        log.info(f"✅ {total} KPIs téléchargés")
        return {"total": total}

    download_nbb(get_enterprises())

bce_download_nbb()
