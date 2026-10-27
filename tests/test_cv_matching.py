"""Tests de api/services/cv_matching.py y del endpoint POST /api/cv/match."""
from __future__ import annotations

import io
import zipfile
from datetime import date

import pytest

from api.services import cv_matching as cm

CV_PSICOLOGA = """
María Pérez Soto — Psicóloga
Concepción, Chile
Título Profesional de Psicóloga, Universidad de Concepción
Magíster en Psicología Clínica
2016 - actualidad: Psicóloga clínica, CESFAM Lorenzo Arenas
2013 - 2016: Psicóloga programa de salud mental, Hospital Regional
Diplomado en Salud Mental Comunitaria
Curso de Primeros Auxilios Psicológicos
"""

OFERTA_PSICOLOGO = {
    "id": 1, "cargo": "Psicólogo(a) Programa Salud Mental",
    "institucion": "Servicio de Salud Concepción", "region": "Biobío",
    "nivel": "Profesional", "area_profesional": None,
    "requisitos": "Título profesional de Psicólogo. Mínimo 3 años de experiencia "
                  "en salud mental en atención primaria.",
    "fecha_cierre": date(2026, 11, 10),
}
OFERTA_ABOGADO = {
    "id": 2, "cargo": "Abogado Fiscalía Regional",
    "institucion": "Ministerio Público", "region": "Metropolitana de Santiago",
    "nivel": "Profesional", "area_profesional": None,
    "requisitos": "Título de abogado. 5 años de experiencia en litigación penal.",
    "fecha_cierre": date(2026, 11, 5),
}


def _docx(texto: str) -> bytes:
    ns = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    parrafos = "".join(f'<w:p><w:r><w:t>{linea}</w:t></w:r></w:p>'
                       for linea in texto.strip().splitlines())
    xml = f'<?xml version="1.0"?><w:document xmlns:w="{ns}"><w:body>{parrafos}</w:body></w:document>'
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("[Content_Types].xml", "<Types/>")
        z.writestr("word/document.xml", xml)
    return buf.getvalue()


def _pdf(texto: str) -> bytes:
    canvas = pytest.importorskip("reportlab.pdfgen.canvas")
    buf = io.BytesIO()
    c = canvas.Canvas(buf)
    y = 800
    for linea in texto.strip().splitlines():
        c.drawString(40, y, linea)
        y -= 16
    c.save()
    return buf.getvalue()


# ── Perfil ───────────────────────────────────────────────────────────────────
def test_perfil_detecta_area_region_experiencia_y_cargos():
    p = cm.perfil_desde_texto(CV_PSICOLOGA, hoy=date(2026, 10, 27))
    assert p.areas[0] == "Salud"
    assert p.region == "Biobío"
    assert p.anios_experiencia == 13          # desde 2013
    assert any("Psicóloga clínica" in c for c in p.cargos_previos)
    assert any("Salud Mental" in c for c in p.capacitaciones)
    assert "palabras" not in p.a_dict()       # la bolsa de palabras no sale


def test_lee_docx_en_memoria():
    texto = cm.leer_cv(_docx(CV_PSICOLOGA))
    assert "Psicóloga clínica" in texto


def test_lee_pdf_en_memoria():
    texto = cm.leer_cv(_pdf(CV_PSICOLOGA.replace("—", "-")))
    assert "CESFAM" in texto


@pytest.mark.parametrize("datos,msg", [
    (b"", "vacío"),
    (b"hola, soy un txt" * 10, "Formato"),
    (b"%PDF-" + b"x" * (cm.MAX_BYTES_CV + 1), "5 MB"),
])
def test_rechaza_archivos_invalidos(datos, msg):
    with pytest.raises(cm.CVInvalido, match=msg):
        cm.leer_cv(datos)


def test_pdf_sin_texto_da_mensaje_util():
    with pytest.raises(cm.CVInvalido, match="escaneado"):
        cm.leer_cv(_pdf("x"))


def test_docx_con_xml_gigante_se_rechaza(monkeypatch):
    monkeypatch.setattr(cm, "MAX_BYTES_DOCX_XML", 100)
    with pytest.raises(cm.CVInvalido, match="demasiado grande"):
        cm.leer_cv(_docx(CV_PSICOLOGA))


# ── Afinidad ─────────────────────────────────────────────────────────────────
def test_ranking_prioriza_la_oferta_afin_y_explica():
    p = cm.perfil_desde_texto(CV_PSICOLOGA, hoy=date(2026, 10, 27))
    res = cm.rankear(p, [OFERTA_ABOGADO, OFERTA_PSICOLOGO], minimo=0)
    assert res[0].oferta["id"] == 1
    assert res[0].banda == "alta"
    assert any("profesión" in r for r in res[0].razones)
    assert any("Cumples la experiencia" in r for r in res[0].razones)
    # El abogado no tiene ninguna señal de fondo: queda fuera aunque
    # coincidan nivel y años de experiencia.
    assert [r.oferta["id"] for r in res] == [1]
    assert cm.afinidad(p, OFERTA_ABOGADO).banda == "baja"


def test_rankear_descarta_lo_irrelevante():
    p = cm.perfil_desde_texto(CV_PSICOLOGA)
    assert [r.oferta["id"] for r in cm.rankear(p, [OFERTA_ABOGADO])] == []


def test_reparo_si_falta_experiencia():
    p = cm.perfil_desde_texto(CV_PSICOLOGA, hoy=date(2026, 10, 27))
    p.anios_experiencia = 1
    r = cm.afinidad(p, OFERTA_PSICOLOGO)
    assert any("Piden 3 años" in x for x in r.reparos)


# ── Endpoint ─────────────────────────────────────────────────────────────────
@pytest.fixture
def cliente(monkeypatch):
    pytest.importorskip("fastapi")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api.routers import cv as cv_router

    monkeypatch.setattr(cv_router, "check_public_rate_limit", lambda *a, **k: None)
    monkeypatch.setattr(cv_router, "execute_fetch_all",
                        lambda *a, **k: [OFERTA_PSICOLOGO, OFERTA_ABOGADO])
    app = FastAPI()
    app.include_router(cv_router.router)
    return TestClient(app)


def test_endpoint_exige_consentimiento(cliente):
    r = cliente.post("/api/cv/match",
                     files={"archivo": ("cv.docx", _docx(CV_PSICOLOGA))})
    assert r.status_code == 400


def test_endpoint_devuelve_sugerencias(cliente):
    r = cliente.post("/api/cv/match", data={"consentimiento": "true"},
                     files={"archivo": ("cv.docx", _docx(CV_PSICOLOGA))})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["evaluadas"] == 2
    assert body["resultados"][0]["oferta"]["id"] == 1
    assert body["resultados"][0]["oferta"]["fecha_cierre"] == "2026-11-10"
    assert "requisitos" not in body["resultados"][0]["oferta"]
    assert body["perfil"]["areas"][0] == "Salud"


def test_endpoint_archivo_invalido_422(cliente):
    r = cliente.post("/api/cv/match", data={"consentimiento": "true"},
                     files={"archivo": ("cv.txt", b"texto plano " * 20)})
    assert r.status_code == 422
    assert "PDF" in r.json()["detail"]
