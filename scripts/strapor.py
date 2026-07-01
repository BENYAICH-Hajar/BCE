"""
strapor.py
==========
Génère et gère les cookies F5 pour statuts.notaire.be via Playwright.
Lance Chrome quelques secondes pour passer le challenge anti-bot,
puis sauvegarde les cookies dans notaire_cookies.json.

Usage :
    python scripts/strapor.py     # génère notaire_cookies.json
"""

import json
import logging
import time
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright

log = logging.getLogger(__name__)

BASE        = "https://statuts.notaire.be/stapor_v1"
COOKIE_FILE = Path("notaire_cookies.json")
PAGE_SIZE   = 20

HEADERS_API = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
    "sec-fetch-dest":  "empty",
    "sec-fetch-mode":  "cors",
    "sec-fetch-site":  "same-origin",
}

SEED_BCE = "0836157420"


def _fetch_cookies_via_playwright() -> list[dict]:
    """
    Ouvre Chrome (visible ~3s) pour passer le challenge F5.
    Retourne la liste brute de cookies Playwright.
    """
    seed_url = (
        f"{BASE}/enterprise/{SEED_BCE}/statutes"
        f"?enterpriseNumber={SEED_BCE}&statuteStart=0&statuteCount=5"
    )
    log.info("Ouverture Chrome pour récupérer les cookies F5...")

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(channel="chrome", headless=False)
        except Exception:
            log.warning("Chrome introuvable — fallback sur Chromium")
            browser = p.chromium.launch(headless=False)

        ctx = browser.new_context(
            locale="fr-BE",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
            ),
        )
        page = ctx.new_page()

        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        page.goto("https://statuts.notaire.be/", wait_until="load", timeout=20_000)
        page.wait_for_timeout(2000)
        page.goto(seed_url, wait_until="load", timeout=30_000)

        for i in range(40):
            names = {c["name"] for c in ctx.cookies()}
            if "OClmoOot" in names and "Lyp1CWKh" in names:
                log.info(f"  Cookies F5 OK ({i * 500}ms)")
                break
            page.wait_for_timeout(500)
        else:
            log.warning(f"  Timeout — cookies présents : {[c['name'] for c in ctx.cookies()]}")

        cookies = ctx.cookies()
        browser.close()

    return cookies


def _build_session(cookies: list[dict]) -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS_API)
    for c in cookies:
        session.cookies.set(c["name"], c["value"], domain=c["domain"])
    return session


def _session_valid(session: requests.Session) -> bool:
    """Vérifie rapidement si la session est encore valide."""
    try:
        r = session.get(
            f"{BASE}/api/enterprises/{SEED_BCE}/statutes",
            params={"offset": 0, "limit": 1},
            timeout=10,
        )
        return "application/json" in r.headers.get("content-type", "")
    except Exception:
        return False


def get_session() -> requests.Session:
    """
    Retourne une session requests valide.
    - Charge notaire_cookies.json si disponible et encore valides
    - Relance Playwright automatiquement sinon (Chrome s'ouvre ~3s)
    """
    if COOKIE_FILE.exists():
        cookies = json.loads(COOKIE_FILE.read_text())
        session = _build_session(cookies)
        if _session_valid(session):
            log.info("Session OK (cookies en cache)")
            return session
        log.info("Cookies expirés — renouvellement automatique...")

    cookies = _fetch_cookies_via_playwright()
    COOKIE_FILE.write_text(json.dumps(cookies, indent=2))
    log.info(f"Cookies sauvegardés → {COOKIE_FILE}")
    return _build_session(cookies)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    log.info("=== Génération des cookies F5 pour statuts.notaire.be ===")
    session = get_session()

    if _session_valid(session):
        log.info("✅ Cookies valides — notaire_cookies.json prêt !")
        log.info("   Tu peux maintenant lancer downloader_notaire.py")
    else:
        log.error("❌ Session invalide — réessaie ou vérifie ta connexion internet")