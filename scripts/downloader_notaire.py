"""
downloader_notaire.py
=====================
Étape 4 du pipeline : télécharge les statuts notariés depuis
statuts.notaire.be pour toutes les entreprises dans MongoDB.

- Lit bce.enterprises depuis MongoDB (uniquement needs_notaire=True)
- Vérifie bce.downloads (State DB) → skip si déjà téléchargé
- Télécharge les PDFs via l'API statuts.notaire.be
- Tourne à travers 3 proxies Tor en rotation (évite le blocage IP)
- Sauvegarde dans bronze/notaire/<bce_number>/
- Met à jour la State DB (done / error)

Usage :
    python scripts/downloader_notaire.py               # toutes les entreprises
    python scripts/downloader_notaire.py --bce 0878065378
    python scripts/downloader_notaire.py --dry-run     # voir sans télécharger
    python scripts/downloader_notaire.py --no-tor      # sans Tor (test local)

Config :
    MONGO_URI    (env var, défaut: mongodb://localhost:27017)
    BRONZE_DIR   (env var, défaut: ./bronze)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from itertools import cycle
from pathlib import Path

import requests
from pymongo import MongoClient

import sys
sys.path.insert(0, str(Path(__file__).parent))
from state_db import is_done, mark_pending, mark_done, mark_error

log = logging.getLogger(__name__)

# --- Config ---
MONGO_URI  = os.environ.get("MONGO_URI",   "mongodb://localhost:27017")
MONGO_DB   = os.environ.get("MONGO_DB",    "bce")
BRONZE_DIR = Path(os.environ.get("BRONZE_DIR", "./bronze"))
BASE       = "https://statuts.notaire.be/stapor_v1"
SOURCE     = "notaire"
PAGE_SIZE  = 20

# --- 3 proxies Tor (ports du docker-compose) ---
TOR_PROXIES = [
    {"http": "socks5h://127.0.0.1:9050", "https": "socks5h://127.0.0.1:9050"},
    {"http": "socks5h://127.0.0.1:9052", "https": "socks5h://127.0.0.1:9052"},
    {"http": "socks5h://127.0.0.1:9054", "https": "socks5h://127.0.0.1:9054"},
]

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
    "sec-fetch-dest":  "empty",
    "sec-fetch-mode":  "cors",
    "sec-fetch-site":  "same-origin",
}

COOKIE_FILE = Path("notaire_cookies.json")


# --------------------------------------------------------------------------
# Gestion des proxies Tor en rotation
# --------------------------------------------------------------------------

class TorRotator:
    """
    Tourne automatiquement entre les 3 proxies Tor.
    Si Tor n'est pas disponible, bascule sur connexion directe.
    """

    def __init__(self, use_tor: bool = True):
        self.use_tor    = use_tor
        self._cycle     = cycle(TOR_PROXIES)
        self._current   = next(self._cycle)
        self._available = None  # on teste au premier appel

    def _test_tor(self) -> bool:
        try:
            r = requests.get(
                "https://check.torproject.org/api/ip",
                proxies=self._current,
                timeout=10,
            )
            is_tor = r.json().get("IsTor", False)
            log.info(f"  Tor disponible : {is_tor} (IP : {r.json().get('IP')})")
            return is_tor
        except Exception as e:
            log.warning(f"  Tor non disponible : {e} — connexion directe utilisée")
            return False

    def get_proxies(self) -> dict | None:
        if not self.use_tor:
            return None
        if self._available is None:
            self._available = _test_tor_available()
        if not self._available:
            return None
        self._current = next(self._cycle)
        return self._current

    def next(self):
        """Force le passage au proxy suivant (ex: après un blocage)."""
        self._current = next(self._cycle)
        return self._current


def _test_tor_available() -> bool:
    """Teste si au moins un proxy Tor répond."""
    for proxy in TOR_PROXIES:
        try:
            r = requests.get(
                "https://check.torproject.org/api/ip",
                proxies=proxy,
                timeout=8,
            )
            if r.json().get("IsTor"):
                log.info(f"  ✓ Tor disponible via {proxy['http']}")
                return True
        except Exception:
            continue
    log.warning("  ⚠ Tor non disponible — connexion directe utilisée")
    return False


# --------------------------------------------------------------------------
# Session Notaire (avec cookies F5 + proxy Tor)
# --------------------------------------------------------------------------

def make_session(proxies: dict | None = None) -> requests.Session:
    """
    Crée une session requests avec :
    - Headers notaire.be
    - Cookies F5 depuis notaire_cookies.json (si disponible)
    - Proxy Tor (si disponible)
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    if proxies:
        session.proxies.update(proxies)

    # Charger les cookies F5 si disponibles
    if COOKIE_FILE.exists():
        cookies = json.loads(COOKIE_FILE.read_text())
        for c in cookies:
            session.cookies.set(c["name"], c["value"], domain=c.get("domain"))
        log.info(f"  Cookies F5 chargés depuis {COOKIE_FILE}")
    else:
        log.warning(
            f"  ⚠ Pas de cookies F5 ({COOKIE_FILE} introuvable)\n"
            "    → Lance d'abord strapor.py une fois pour générer les cookies\n"
            "    → OU copie notaire_cookies.json depuis ton navigateur"
        )

    return session


def session_valid(session: requests.Session, bce: str) -> bool:
    """Vérifie rapidement si la session est encore valide."""
    try:
        r = session.get(
            f"{BASE}/api/enterprises/{bce}/statutes",
            params={"offset": 0, "limit": 1},
            timeout=10,
        )
        return "application/json" in r.headers.get("content-type", "")
    except Exception:
        return False


# --------------------------------------------------------------------------
# Récupération des statuts disponibles
# --------------------------------------------------------------------------

def get_statutes(session: requests.Session, bce: str) -> list[dict]:
    """Récupère tous les statuts DONE disponibles pour une entreprise."""
    url = f"{BASE}/api/enterprises/{bce}/statutes"
    session.headers["Referer"] = (
        f"{BASE}/enterprise/{bce}/statutes"
        f"?enterpriseNumber={bce}&statuteStart=0&statuteCount=5"
    )

    all_statutes, offset = [], 0

    while True:
        try:
            r = session.get(
                url,
                params={"deedDate": "", "offset": offset, "limit": PAGE_SIZE},
                timeout=15,
            )
            r.raise_for_status()

            if "application/json" not in r.headers.get("content-type", ""):
                log.error(f"  [{bce}] Réponse non-JSON — session expirée ou bloquée")
                break

            data  = r.json()
            batch = data.get("statutes", [])
            total = data.get("totalItems", 0)
            all_statutes.extend(batch)
            log.info(f"  [{bce}] offset={offset} — {len(batch)} statuts (total: {total})")

            if not batch or len(all_statutes) >= total:
                break
            offset += PAGE_SIZE
            time.sleep(0.5)

        except Exception as e:
            log.error(f"  [{bce}] Erreur get_statutes : {e}")
            break

    done = [s for s in all_statutes if s.get("documentStatus") == "DONE"]
    log.info(f"  [{bce}] → {len(done)} statuts DONE")
    return done


# --------------------------------------------------------------------------
# Téléchargement d'un PDF de statut
# --------------------------------------------------------------------------

def download_statute_pdf(
    session: requests.Session,
    bce: str,
    statute: dict,
    dest_dir: Path,
    dry_run: bool = False,
) -> Path | None:

    doc_id    = str(statute["documentId"])
    deed_date = statute.get("deedDate", "unknown").replace("-", "")
    title     = statute.get("documentTitle", "")[:50]

    # --- Delta detection ---
    if is_done(bce, SOURCE, doc_id):
        log.info(f"    [SKIP] {deed_date} — {title} (déjà téléchargé)")
        return None

    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{bce}_{deed_date}_{doc_id}.pdf"
    dest     = dest_dir / filename

    if dry_run:
        log.info(f"    [DRY-RUN] {deed_date} — {title} → {filename}")
        return dest

    year = int(deed_date[:4]) if len(deed_date) >= 4 and deed_date[:4].isdigit() else 0
    mark_pending(bce, SOURCE, doc_id, year)

    try:
        r = session.get(
            f"{BASE}/api/enterprises/{bce}/statutes/non-certified/{doc_id}",
            timeout=30,
        )

        if r.status_code == 404:
            mark_error(bce, SOURCE, doc_id, "HTTP 404 — document non disponible")
            log.warning(f"    [404] {deed_date} — {title}")
            return None

        r.raise_for_status()

        if "pdf" not in r.headers.get("content-type", "") and len(r.content) < 1000:
            mark_error(bce, SOURCE, doc_id, "Réponse non-PDF ou trop petite")
            return None

        dest.write_bytes(r.content)
        hdfs_path = f"bronze/notaire/{bce}/{filename}"
        mark_done(bce, SOURCE, doc_id, hdfs_path)
        log.info(f"    [OK] {deed_date} — {title} ({len(r.content)//1024} KB)")
        return dest

    except Exception as e:
        mark_error(bce, SOURCE, doc_id, str(e))
        log.error(f"    [ERR] {deed_date} — {e}")
        return None


# --------------------------------------------------------------------------
# Traitement d'une entreprise
# --------------------------------------------------------------------------

def process_enterprise(
    enterprise: dict,
    tor: TorRotator,
    dry_run: bool = False,
) -> int:
    bce  = enterprise["bce_number"]
    name = enterprise.get("denomination", bce)

    log.info(f"\n{'='*55}")
    log.info(f"  {name} ({bce})")
    log.info(f"{'='*55}")

    if not enterprise.get("needs_notaire", True):
        log.info("  [SKIP] Forme juridique ne nécessite pas de notaire")
        return 0

    dest_dir = BRONZE_DIR / "notaire" / bce
    proxies  = tor.get_proxies()
    session  = make_session(proxies)

    # Vérifier la session — renouveler automatiquement si expirée
    if not dry_run and not session_valid(session, bce):
        log.warning(f"  Session expirée pour {bce} — renouvellement automatique...")
        try:
            from strapor import get_session as strapor_get_session
            session = strapor_get_session()
            if proxies:
                session.proxies.update(proxies)
            if not session_valid(session, bce):
                log.error(f"  Session toujours invalide pour {bce} — skip")
                return 0
            log.info("  ✅ Session renouvelée avec succès")
        except Exception as e:
            log.error(f"  Impossible de renouveler la session : {e}")
            return 0

    statutes = get_statutes(session, bce)
    if not statutes:
        log.info("  Aucun statut disponible.")
        return 0

    downloaded = 0
    for i, statute in enumerate(statutes):
        # Rotation Tor tous les 10 téléchargements
        if i > 0 and i % 10 == 0:
            proxies = tor.next()
            session.proxies.update(proxies or {})
            log.info(f"  → Rotation proxy Tor (téléchargement {i})")

        result = download_statute_pdf(session, bce, statute, dest_dir, dry_run)
        if result:
            downloaded += 1
        time.sleep(0.4)

    log.info(f"  [{bce}] {downloaded} PDFs téléchargés")
    return downloaded


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bce", nargs="+", default=None, help="Numéros BCE spécifiques")
    parser.add_argument("--dry-run", action="store_true", help="Voir sans télécharger")
    parser.add_argument("--no-tor", action="store_true", help="Sans proxy Tor")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Lire les entreprises depuis MongoDB
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    coll   = client[MONGO_DB]["enterprises"]

    if args.bce:
        query = {"bce_number": {"$in": args.bce}, "needs_notaire": True}
    else:
        query = {"is_active": True, "needs_notaire": True}

    enterprises = list(coll.find(query, {"_id": 0}))
    log.info(f"{len(enterprises)} entreprise(s) avec statuts notaire à traiter")

    if args.dry_run:
        log.info("MODE DRY-RUN — aucun fichier ne sera téléchargé")

    tor   = TorRotator(use_tor=not args.no_tor)
    total = 0

    for i, enterprise in enumerate(enterprises):
        count = process_enterprise(enterprise, tor, dry_run=args.dry_run)
        total += count
        if i < len(enterprises) - 1:
            log.info("  Pause 5s avant la prochaine entreprise...")
            time.sleep(5)

    log.info(f"\n✅ Terminé — {total} PDFs notaire téléchargés au total")
    log.info(f"   Dossier : {BRONZE_DIR / 'notaire'}")


if __name__ == "__main__":
    main()