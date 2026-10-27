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


# ── Profesiones (detección y reglas de calce) ────────────────────────────────
from api.services import profesiones_cv as prof


@pytest.mark.parametrize("texto,esperado", [
    ("TENS SERVICIO MÉDICO QUIRURGICO", ["tens"]),           # no es médico
    ("Técnico en Enfermería (TENS) - Maternidad", ["tens"]),  # no es enfermera
    ("TECNÓLOGO MÉDICO BANCO DE SANGRE", ["tecnologo_medico"]),
    ("EU CLINICA SERVICIO CIRUGIA", ["enfermera"]),
    ("Ingeniero/a en Automatización", ["ing_electrico"]),
    ("Base de Datos Reemplazos Educadora Párvulos", ["educadora_parvulos"]),
    ("Asistente de Tardes de Juego en mi Jardín", ["tecnico_parvulos"]),
    ("Profesional de Apoyo a la Gestión Administrativa", []),
    ("Enfermera supervisora de equipo de TENS", ["enfermera", "tens"]),
])
def test_detecta_profesion(texto, esperado):
    assert prof.todas(cm._norm(texto)) == esperado


def test_tecnico_no_calza_con_cargo_de_ingeniero():
    p = cm.perfil_desde_texto("Técnico en Electricidad, INACAP\nPuente Alto\n"
                              "2018 - actualidad: Electricista de mantención")
    r = cm.afinidad(p, {"cargo": "Ingeniero/a en Automatización",
                        "requisitos": "Formación: Ingeniería en Automatización o Electrónica."})
    assert not r.nucleo


def test_calce_solo_afin_no_es_banda_alta():
    p = cm.perfil_desde_texto("Ingeniera Comercial, Universidad de Santiago\nSantiago\n"
                              "2016 - 2019: Profesional de apoyo administrativo")
    r = cm.afinidad(p, {"cargo": "Administrativo clínico", "region": "Metropolitana de Santiago",
                        "requisitos": "Enseñanza media completa. 1 año de experiencia como administrativo."})
    assert r.nucleo and r.banda != "alta"
    assert any("afín" in x for x in r.razones)


def test_reparo_si_exigen_especialidad():
    p = cm.perfil_desde_texto("Médico Cirujano, Universidad de Chile\nSantiago\n"
                              "2019 - actualidad: Médico EDF, CESFAM")
    r = cm.afinidad(p, {"cargo": "Médico Residente UPC", "requisitos":
                        "Título de médico cirujano con especialidad en Medicina Interna "
                        "o Medicina de Urgencia (excluyente)"})
    assert any("especialidad en medicina interna" in x for x in r.reparos)


def test_region_no_sale_del_nombre_de_la_universidad():
    p = cm.perfil_desde_texto("Ana Díaz\nKinesióloga, Universidad de Concepción\nViña del Mar")
    assert p.region == "Valparaíso"


def test_perfil_expone_profesion():
    p = cm.perfil_desde_texto(CV_PSICOLOGA)
    assert p.a_dict()["profesiones"] == ["Psicología"]
    assert "texto_norm" not in p.a_dict()
