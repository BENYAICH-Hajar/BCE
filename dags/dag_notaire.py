from __future__ import annotations
import logging, os, sys
from datetime import datetime
from pathlib import Path
from airflow.sdk import dag, task

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
log = logging.getLogger(__name__)
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.environ.get("MONGO_DB",  "bce")

@dag(dag_id="bce_download_notaire", schedule="0 3 * * 6", start_date=datetime(2026,1,1), catchup=False, tags=["bce","notaire"])
def bce_download_notaire():

    @task()
    def get_enterprises_notaire():
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI)
        ents = list(client[MONGO_DB]["enterprises"].find({"is_active":True,"needs_notaire":True}, {"_id":0,"EnterpriseNumber":1}))
        log.info(f"{len(ents)} entreprises avec notaire")
        client.close()
        return ents

    @task()
    def download_notaire(enterprises):
        from downloader_notaire import process_enterprise, TorRotator
        tor = TorRotator(use_tor=True)
        total = 0
        for ent in enterprises:
            bce = ent.get("EnterpriseNumber","").replace(".","").zfill(10)
            try:
                count = process_enterprise({"bce_number":bce,"denomination":bce,"needs_notaire":True}, tor=tor)
                total += count
            except Exception as e:
                log.error(f"Erreur {bce}: {e}")
        log.info(f"✅ {total} PDFs téléchargés")
        return {"total": total}

    download_notaire(get_enterprises_notaire())

bce_download_notaire()
