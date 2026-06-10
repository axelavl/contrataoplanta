import contextlib
import socket
import subprocess
import time
from pathlib import Path

import pytest

playwright = pytest.importorskip("playwright.sync_api")

REPO_ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = REPO_ROOT / "web"


def _free_port():
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def web_server():
    port = _free_port()
    proc = subprocess.Popen(
        ["python", "-m", "http.server", str(port), "--bind", "127.0.0.1"],
        cwd=WEB_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1)
    yield f"http://127.0.0.1:{port}"
    proc.terminate()
    proc.wait(timeout=5)


def test_desktop_navigation_and_breadcrumb(web_server):
    with playwright.sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1366, "height": 900})
        page.goto(f"{web_server}/favoritos.html", wait_until="networkidle")
        page.wait_for_selector("header nav .nav-inner")
        assert page.locator(".nav-links a.active").first.inner_text().strip() in {"♡ Mis favoritos", "♥ Mis favoritos"}
        assert page.locator("nav.breadcrumb .breadcrumb-actual").count() == 1

        # quick internal links smoke from header
        hrefs = page.eval_on_selector_all("header a[href]", "els => els.map(e => e.getAttribute('href'))")
        assert "estadisticas.html" in hrefs
        assert "faq.html" in hrefs
        browser.close()


def test_mobile_menu_smoke(web_server):
    with playwright.sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 390, "height": 844})
        page.goto(f"{web_server}/estadisticas.html", wait_until="networkidle")
        page.wait_for_selector("#hamburger-btn")
        page.click("#hamburger-btn")
        page.wait_for_selector("#nav-mobile-panel.visible")
        assert page.locator("#nav-mobile-panel a").count() >= 4
        browser.close()
