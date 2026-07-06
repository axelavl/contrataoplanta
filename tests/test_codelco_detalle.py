"""Codelco (empleos.codelco.cl, SAP SuccessFactors): parseo del detalle.

Regresión de la oferta "Ingeniero/a Jefe/a Operaciones Concentradora" (División
Salvador, verificada 07/2026): el aviso usa los encabezados "Funciones del
Cargo" / "Requisitos del Cargo" (no "Funciones Principales" / "Requisitos de
Postulación") y una sección "Aspectos Deseables". Antes el scraper no recogía
funciones, requisitos, formación ni deseables, y volcaba jornada/vacantes/
contrato como texto suelto en la descripción.
"""
from __future__ import annotations

from datetime import date

import scrapers.codelco as C

# Estructura real del <span itemprop=description> (encabezados "del Cargo").
_AVISO = """
Ingeniero/a Jefe/a Operaciones Concentradora - Division Salvador
¿Quieres ser parte del desarrollo sostenible de Chile?
Diversidad e Inclusión
En Codelco estamos llamados a ser líderes en diversidad e inclusión.
Propósito del Cargo:
Nuestra estrategia de negocio es maximizar en forma competitiva y sustentable
el valor económico de Codelco a través de la explotación minera del cobre.
¡Porque aquí el despliegue de tus talentos se pondrá al servicio de Chile!
Funciones del Cargo:
Dirigir y supervisar la gestión operacional del área de Chancado.
Administrar contratos de servicios de su área de responsabilidad.
Evaluar gestión del desempeño de equipo de trabajo a su cargo.
Requisitos del Cargo:
Profesional Titulado/a de Ingenieria Civil Química, Metalúrgica, o Industrial,
de 10 semestres comprobables.
Mínimo 5 años de experiencia en áreas operativas de faenas mineras.
Contar con licencia de conducir clase B vigente.
Contar con salud compatible con el cargo.
Aspectos Deseables:
2 años o mas de experiencia en Plantas Concentradoras.
Postgrado y/o Diplomado relacionado a Gestión de Operaciones.
Condiciones Ofrecidas:
Contrato: Indefinido.
Cargo contractual Ingeniero Jefe.
Lugar de trabajo: Division Salvador.
Jornada laboral: Lunes a Jueves.
Número de vacantes: 01.
Cierre de Postulaciones: Lunes 13 de Julio de 2026.
Hora de cierre de postulaciones: 23:59 hrs.
"""

_HTML = f'<html><body><span itemprop="description">{_AVISO}</span></body></html>'


def _detalle() -> dict:
    return C.parsear_detalle(_HTML)


def test_detalle_recoge_encabezados_del_cargo():
    d = _detalle()
    assert d.get("funciones") and "Dirigir y supervisar" in d["funciones"]
    assert d.get("requisitos") and "Profesional Titulado" in d["requisitos"]
    # La formación se reconoce como dato estructurado.
    assert d.get("formacion") and "Ingenieria Civil Química" in d["formacion"]
    # Los "Aspectos Deseables" ya no se pierden.
    assert d.get("deseables") and "Plantas Concentradoras" in d["deseables"]
    assert d.get("cargo_contractual") == "Ingeniero Jefe"


def test_detalle_metadata_estructurada():
    d = _detalle()
    assert d.get("jornada") == "Lunes a Jueves"
    assert d.get("vacantes") == 1
    assert d.get("contrato") == "Indefinido"
    assert d.get("fecha_cierre") == date(2026, 7, 13)
    assert d.get("region") == "Atacama"
    assert d.get("ciudad") == "El Salvador"


def test_requisitos_no_arrastran_aspectos_deseables():
    # El bloque de requisitos se corta antes de "Aspectos Deseables";
    # los deseables viajan aparte, no mezclados con los obligatorios.
    d = _detalle()
    assert "Plantas Concentradoras" not in d["requisitos"]


def _oferta() -> dict:
    item = {
        "titulo": "Ingeniero/a Jefe/a Operaciones Concentradora",
        "url": "https://empleos.codelco.cl/job/x/1405841000/",
        "id_sf": "1405841000",
        "id_proceso": "89612",
        "fecha_publicacion": None,
        "region": None,
    }
    return C.construir_oferta(item, _detalle())


def test_oferta_expone_jornada_y_vacantes_como_columnas():
    o = _oferta()
    assert o["jornada"] == "Lunes a Jueves"
    assert o["numero_vacantes"] == 1


def test_descripcion_es_narrativa_sin_soup_de_metadata():
    o = _oferta()
    desc = o["descripcion"]
    assert "Propósito del cargo:" in desc
    assert "Funciones principales:" in desc
    # La metadata (jornada/contrato/vacantes/ID) ya no ensucia la narrativa.
    assert "Jornada:" not in desc
    assert "Contrato:" not in desc
    assert "ID de proceso" not in desc


def test_requisitos_texto_incluye_formacion_y_deseables():
    o = _oferta()
    req = o["requisitos_texto"]
    assert "Formación requerida:" in req
    assert "Aspectos deseables:" in req
    assert "Plantas Concentradoras" in req
