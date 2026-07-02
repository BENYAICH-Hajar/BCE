"""
dag_gold.py
===========
DAG Airflow — Recalcul annuel de la Gold Layer

Logique :
1. Lister les entreprises hôtelières (StateDB status=done)
2. Vérifier les nouveaux dépôts NBB pour chacune
3. Télécharger les nouveaux exercices manquants
4. Recalculer la Gold Layer sur les entreprises mises à jour
5. Upsert hotel_gold.years dans MongoDB

Schedule : tous les 1er janvier à 6h00
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Config ──
MONGO_URI  = os.environ.get("MONGO_URI",   "mongodb://localhost:27017")
MONGO_DB   = os.environ.get("MONGO_DB",    "bce")
BRONZE_DIR = Path(os.environ.get("BRONZE_DIR", "/mnt/c/Users/BENYA/Desktop/BCE/bce-pipeline/bronze"))
SCRIPTS_DIR = Path("/mnt/c/Users/BENYA/Desktop/BCE/bce-pipeline/scripts")

BASE_URL = "https://consult.cbso.nbb.be/api"
SOURCE   = "nbb"

log = logging.getLogger(__name__)

default_args = {
    "owner":            "hajar",
    "retries":          2,
    "retry_delay":      timedelta(minutes=30),
    "email_on_failure": False,
}

# --------------------------------------------------------------------------
# TASK 1 — Lister les entreprises hôtelières done
# --------------------------------------------------------------------------

def list_hotel_enterprises(**context):
    """Récupère toutes les entreprises avec status=done dans StateDB."""
    from pymongo import MongoClient

    db = MongoClient(MONGO_URI)[MONGO_DB]

    # Entreprises hôtelières avec au moins un dépôt done
    pipeline = [
        {"$match": {"source": SOURCE, "status": "done"}},
        {"$group": {"_id": "$bce_number"}},
    ]
    bce_list = [doc["_id"] for doc in db["downloads"].aggregate(pipeline)]
    log.info(f"✅ {len(bce_list)} entreprises hôtelières avec status=done")

    # Passer à la prochaine tâche via XCom
    context["ti"].xcom_push(key="bce_list", value=bce_list)
    return len(bce_list)


# --------------------------------------------------------------------------
# TASK 2 — Vérifier les nouveaux dépôts NBB
# --------------------------------------------------------------------------

def check_new_deposits(**context):
    """
    Pour chaque entreprise, compare les dépôts NBB disponibles
    avec ceux déjà téléchargés en StateDB.
    Retourne la liste des (bce, deposit_id, year) à télécharger.
    """
    import requests
    from pymongo import MongoClient

    db      = MongoClient(MONGO_URI)[MONGO_DB]
    bce_list = context["ti"].xcom_pull(key="bce_list", task_ids="list_hotel_enterprises")

    if not bce_list:
        log.warning("Aucune entreprise à vérifier")
        context["ti"].xcom_push(key="new_deposits", value=[])
        return 0

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept":     "application/json",
    }

    new_deposits = []

    for i, bce in enumerate(bce_list):
        bce_clean = bce.replace(".", "").strip()
        url = (
            f"{BASE_URL}/rs-consult/published-deposits"
            f"?page=0&size=20&enterpriseNumber={bce_clean}"
            f"&sort=periodEndDate,desc"
        )

        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 429:
                time.sleep(60)
                r = requests.get(url, headers=headers, timeout=30)
            if r.status_code != 200:
                continue

            deposits = r.json().get("content", [])

            for d in deposits:
                year = int(d.get("periodEndDateYear", 0))
                if year < 2021:
                    continue
                deposit_id = str(d["id"])

                # Vérifier si déjà téléchargé
                existing = db["downloads"].find_one({
                    "bce_number": bce,
                    "source":     SOURCE,
                    "deposit_id": deposit_id,
                    "status":     "done",
                })
                if not existing:
                    new_deposits.append({
                        "bce":        bce,
                        "deposit_id": deposit_id,
                        "year":       year,
                        "reference":  d.get("reference", deposit_id),
                    })

        except Exception as e:
            log.error(f"  [{bce}] Erreur : {e}")

        if (i + 1) % 50 == 0:
            log.info(f"  {i+1}/{len(bce_list)} vérifiés — {len(new_deposits)} nouveaux")
        time.sleep(0.5)

    log.info(f"✅ {len(new_deposits)} nouveaux dépôts à télécharger")
    context["ti"].xcom_push(key="new_deposits", value=new_deposits)
    return len(new_deposits)


# --------------------------------------------------------------------------
# TASK 3 — Télécharger les nouveaux dépôts
# --------------------------------------------------------------------------

def download_new_deposits(**context):
    """Télécharge les PDF et CSV des nouveaux dépôts."""
    import requests
    from pymongo import MongoClient

    sys.path.insert(0, str(SCRIPTS_DIR))
    from state_db import mark_pending, mark_done, mark_error

    db = MongoClient(MONGO_URI)[MONGO_DB]
    new_deposits = context["ti"].xcom_pull(key="new_deposits", task_ids="check_new_deposits")

    if not new_deposits:
        log.info("Aucun nouveau dépôt à télécharger")
        context["ti"].xcom_push(key="updated_bce", value=[])
        return 0

    headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
    updated_bce = set()
    downloaded  = 0

    for dep in new_deposits:
        bce        = dep["bce"]
        deposit_id = dep["deposit_id"]
        year       = dep["year"]
        reference  = dep["reference"]

        dest_dir = BRONZE_DIR / "nbb" / "hotellerie" / bce
        dest_dir.mkdir(parents=True, exist_ok=True)

        mark_pending(bce, SOURCE, deposit_id, year)

        # PDF
        pdf_url  = f"{BASE_URL}/external/broker/public/deposits/pdf/{deposit_id}"
        pdf_path = dest_dir / f"{year}_{reference}.pdf"
        try:
            r = requests.get(pdf_url, headers=headers, timeout=30)
            if r.status_code == 429:
                time.sleep(60)
                r = requests.get(pdf_url, headers=headers, timeout=30)
            r.raise_for_status()
            pdf_path.write_bytes(r.content)
        except Exception as e:
            log.error(f"  [{bce}] PDF erreur : {e}")
            mark_error(bce, SOURCE, deposit_id, str(e))
            continue

        # CSV
        csv_url  = f"{BASE_URL}/external/broker/public/deposits/consult/csv/{deposit_id}"
        csv_path = dest_dir / f"{year}_{reference}.csv"
        try:
            r = requests.get(csv_url, headers=headers, timeout=30)
            if r.status_code == 429:
                time.sleep(60)
                r = requests.get(csv_url, headers=headers, timeout=30)
            if r.status_code == 200:
                csv_path.write_text(r.text, encoding="utf-8")
        except Exception as e:
            log.warning(f"  [{bce}] CSV erreur (non bloquant) : {e}")

        hdfs_path = f"bronze/nbb/hotellerie/{bce}/{year}_{reference}"
        mark_done(bce, SOURCE, deposit_id, hdfs_path)
        updated_bce.add(bce)
        downloaded += 1
        time.sleep(0.5)

    log.info(f"✅ {downloaded} fichiers téléchargés — {len(updated_bce)} entreprises mises à jour")
    context["ti"].xcom_push(key="updated_bce", value=list(updated_bce))
    return downloaded


# --------------------------------------------------------------------------
# TASK 4 — Recalcul Gold Layer (incrémental)
# --------------------------------------------------------------------------

def recalculate_gold(**context):
    """
    Recalcule hotel_gold uniquement pour les entreprises
    ayant de nouveaux dépôts (incrémental).
    """
    from pymongo import MongoClient, UpdateOne

    sys.path.insert(0, str(SCRIPTS_DIR))
    from gold import process_enterprise

    db          = MongoClient(MONGO_URI)[MONGO_DB]
    updated_bce = context["ti"].xcom_pull(key="updated_bce", task_ids="download_new_deposits")

    if not updated_bce:
        log.info("Aucune entreprise à recalculer")
        return 0

    # Charger les dénominations
    silver = {
        doc["EnterpriseNumber"].replace(".", ""): doc.get("denomination_principale", "")
        for doc in db["enterprise_silver"].find(
            {}, {"_id": 0, "EnterpriseNumber": 1, "denomination_principale": 1}
        )
    }

    ops       = []
    processed = 0

    for bce in updated_bce:
        bce_dir = BRONZE_DIR / "nbb" / "hotellerie" / bce
        if not bce_dir.exists():
            continue

        denom = silver.get(bce.replace(".", ""), "")
        doc   = process_enterprise(bce_dir, denom)
        if not doc:
            continue

        ops.append(UpdateOne(
            {"enterprise_number": bce.replace(".", "")},
            {"$set": doc},
            upsert=True,
        ))
        processed += 1

        if len(ops) >= 100:
            db["hotel_gold"].bulk_write(ops, ordered=False)
            ops = []

    if ops:
        db["hotel_gold"].bulk_write(ops, ordered=False)

    log.info(f"✅ Gold Layer recalculée pour {processed} entreprises")
    return processed


# --------------------------------------------------------------------------
# Définition du DAG
# --------------------------------------------------------------------------

with DAG(
    dag_id="bce_gold_recalcul_annuel",
    description="Recalcul annuel de la Gold Layer hôtellerie BCE",
    schedule="0 6 1 1 *",      # 1er janvier à 6h00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bce", "gold", "hotellerie"],
) as dag:

    t1 = PythonOperator(
        task_id="list_hotel_enterprises",
        python_callable=list_hotel_enterprises,
    )

    t2 = PythonOperator(
        task_id="check_new_deposits",
        python_callable=check_new_deposits,
    )

    t3 = PythonOperator(
        task_id="download_new_deposits",
        python_callable=download_new_deposits,
    )

    t4 = PythonOperator(
        task_id="recalculate_gold",
        python_callable=recalculate_gold,
    )

    t1 >> t2 >> t3 >> t4