"""bases_pdf: lectura de decretos de bases municipales (texto/OCR) y vigencia.

El texto de prueba replica el decreto REAL 7738/2024 de Viña del Mar
("Profesional Maestranza", escaneado): tabla PLANTA/GRADO/VACANTES/CARGO,
secciones 2.1–2.5, requisitos 3.1/3.2, destinaciones y cronograma. El criterio
de éxito del usuario: esa oferta debe quedar con grado, planta, cargo,
funciones, requisitos, título, competencias, cronograma y estado VENCIDO
calculado desde el PDF (no desde la etiqueta "Nueva" de la tarjeta).
"""
from __future__ import annotations

from datetime import date

import scrapers.bases_pdf as B

_DECRETO = """
MRS/ELV/PS                                VIÑA DEL MAR, 04 JUN. 2024
ESTA ALCALDIA DECRETO HOY LO QUE SIGUE:
N° 7738 / VISTOS: Los antecedentes; Decreto Alcaldicio N° 16.209/2022 ...

DECRETO:

Llámese a Concurso Público para proveer el cargo que a continuación se indica.

1. CARGO A CONCURSAR:
PLANTA GRADO VACANTES CARGO A DESEMPEÑAR
Profesionales 11° 1 Profesional Maestranza

2. PERFIL DEL CARGO:
2.1 Objetivo del cargo:
El objetivo del cargo a proveer corresponde a un/a profesional que tenga los
conocimientos y la experiencia necesaria en el área mecánica, que permita
gestionar la mantención y reparación de vehículos y maquinarias municipales.

2.2 Funciones
- Colaborar en la mantención y reparación de vehículos y maquinarias municipales.
- Llevar el control de vehículos, equipo y maquinaria pesada municipales.
- Abordar los siniestros y accidentes sufridos por vehículos municipales y/o arrendados.
- Efectuar las comisiones de servicio, en conformidad a la Ley 18883/89.
- Las demás funciones que le asigne su jefatura.

2.3 Competencias
- Capacidad de liderazgo.
- Habilidades sociales.
- Capacidad de organización y planificación.
- Capacidad de gestión y coordinación.
- Creatividad de innovación
- Proactividad y autonomía
- Ética y probidad

2.4 Conocimientos específicos (deseables, no excluyentes):
- Conocimientos Ley 18.695 Orgánica Constitucional de Municipalidades.
- Conocimientos Ley 18.883 Estatuto Administrativo para funcionarios Municipales.
- Conocimientos en materia de mecánica automotriz y gestión del mantenimiento de vehículos.
- Conocimientos en gestión logística de flota de vehículos.

2.5 Habilidades
- Manejo de Google Workspace
- Trabajo en equipo
- Capacidad de gestionar, adaptarse ante eventuales cambios y tomar decisiones asertivas.

3. REQUISITOS
3.1 Requisitos generales
Los requisitos generales que deben cumplir todos/as los/las postulantes son los
siguientes: a) Ser ciudadano. b) Haber cumplido con la Ley de Reclutamiento.
c) Tener salud compatible con el desempeño del Cargo.

3.2. Requisitos Específicos
Los requisitos asociados al presente concurso son los siguientes:
- Poseer Título Profesional de al menos 8 semestres de duración otorgado por una
Institución de Educación Superior del Estado o reconocida por éste.
- Poseer Título Profesional del área de la mecánica automotriz (excluyente).

4. DESTINACIONES
El/la postulante que en definitiva resulte seleccionado/a en el cargo, será
destinado/a al Departamento de Mantención y Reparación de Vehículos y Maquinaria
Pesada, dependiente de la Dirección de Operaciones y Servicios.

8. CRONOGRAMA CONCURSO
ETAPA FECHA
Publicación del llamado a concurso en diario de circulación regional 05 de Junio de 2024
Entrega y publicación de bases 05 de Junio de 2024
Cierre de recepción de postulaciones 17 de Junio de 2024
Evaluación Primera Etapa 17 de Junio al 08 de Julio de 2024
Entrevistas Entre el 09 y 24 de Julio de 2024
Resolución del concurso 30 de Julio de 2024
Asunción al cargo 1 de Agosto de 2024
"""


def _bases():
    return B.parsear_bases(_DECRETO)


# ── Parseo del decreto ───────────────────────────────────────────────────────
def test_acto_administrativo():
    b = _bases()
    assert b["decreto_numero"] == "7738"
    assert b["decreto_fecha"] == date(2024, 6, 4)


def test_tabla_cargo_planta_grado_vacantes():
    b = _bases()
    assert b["planta"] == "Profesionales"
    assert b["grado"] == "11"
    assert b["vacantes"] == 1
    assert b["cargo_desempenar"] == "Profesional Maestranza"


def test_secciones_del_perfil():
    b = _bases()
    assert "área mecánica" in b["objetivo"]
    assert any("mantención y reparación de vehículos" in f for f in b["funciones"])
    assert any("liderazgo" in c.lower() for c in b["competencias"])
    assert any("mecánica automotriz" in c for c in b["conocimientos"])
    assert any("Google Workspace" in h for h in b["habilidades"])


def test_requisitos_y_titulo():
    b = _bases()
    assert "Ser ciudadano" in b["requisitos_generales"]
    assert any("8 semestres" in r for r in b["requisitos_especificos"])
    assert "Título Profesional" in b["titulo_requerido"]


def test_destinacion():
    assert "Mantención y Reparación de Vehículos" in _bases()["destinacion"]


def test_cronograma_completo():
    crono = _bases()["cronograma"]
    assert crono["publicacion"] == date(2024, 6, 5)
    assert crono["cierre_postulacion"] == date(2024, 6, 17)
    assert crono["evaluacion"] == date(2024, 7, 8)      # última fecha del rango
    assert crono["entrevistas"] == date(2024, 7, 24)
    assert crono["resolucion_concurso"] == date(2024, 7, 30)
    assert crono["asuncion_cargo"] == date(2024, 8, 1)


# ── Acta de RESULTADOS (terna/selección) en vez de bases ────────────────────
# Caso real: el PDF del D.A 7.737/2024 de Viña ya no es el llamado sino el
# acta que informa la terna y la selección — el proceso está RESUELTO, pero
# como el acta no trae cronograma caía en "sin_fecha" y se publicaba vigente.
_ACTA_TERNA = """
VIÑA DEL MAR, 04 JUN. 2024
ESTA ALCALDIA DECRETO HOY LO QUE SIGUE:
N° 7737 / VISTOS: ...
INFORMA TERNA LLAMADO A CONCURSO PÚBLICO D.A N° 7.737/2024, PARA PROVEER
EL CARGO PROFESIONAL GRADO 11° E.M. DIRECCIÓN DE CONTROL.
NOMBRE PUNTAJE FINAL
POSTULANTE UNO 85,3
POSTULANTE DOS 81,0
POSTULANTE TRES 78,4
De la terna informada, la Sra. Alcaldesa ha seleccionado a POSTULANTE UNO
para asumir el cargo concursado.
"""


def test_acta_de_resultados_se_detecta():
    b = B.parsear_bases(_ACTA_TERNA)
    assert b.get("es_resultado") is True


def test_acta_de_resultados_es_vencido_aunque_no_haya_cronograma():
    estado, motivo = B.calcular_vigencia(B.parsear_bases(_ACTA_TERNA), hoy=date(2026, 7, 6))
    assert estado == "vencido"
    assert motivo == ("El PDF corresponde a resultados del concurso "
                      "(terna/selección): proceso ya resuelto")


def test_bases_reales_no_se_confunden_con_resultados():
    # El decreto de bases menciona "Resolución del concurso" en el cronograma
    # y trae "Cierre de recepción de postulaciones": NO es un acta de resultados.
    assert "es_resultado" not in _bases()


# ── Decreto antiguo sin cierre detectable → vencido a los 90 días ────────────
def test_decreto_viejo_sin_cierre_es_vencido():
    b = {"decreto_fecha": date(2024, 6, 4)}
    estado, motivo = B.calcular_vigencia(b, hoy=date(2026, 7, 6))
    assert estado == "vencido"
    assert motivo == ("Las bases datan del 2024-06-04 (>90 días) "
                      "y no traen fecha de cierre detectable")


def test_decreto_reciente_sin_cierre_sigue_sin_fecha():
    b = {"decreto_fecha": date(2026, 6, 20)}
    estado, motivo = B.calcular_vigencia(b, hoy=date(2026, 7, 6))
    assert estado == "sin_fecha"
    assert "revisar manualmente" in motivo


# ── Jornada ──────────────────────────────────────────────────────────────────
def test_jornada_desde_el_texto():
    texto = ("BASES CONCURSO PÚBLICO. El cargo a proveer se desempeñará bajo una "
             "jornada laboral de 44 horas semanales, en dependencias municipales, "
             "conforme al Estatuto Administrativo para Funcionarios Municipales.")
    assert B.parsear_bases(texto)["jornada"] == "44 horas"


def test_enriquecer_copia_jornada_sin_pisar():
    texto = _DECRETO + "\nJornada de trabajo: 44 horas semanales."
    oferta = {"cargo": "Profesional Maestranza"}
    res = B.enriquecer_desde_texto(oferta, texto, "texto", hoy=date(2026, 7, 6))
    assert oferta["jornada"] == "44 horas"
    assert "jornada" in res["campos"]
    oferta2 = {"cargo": "Profesional Maestranza", "jornada": "33 horas"}
    B.enriquecer_desde_texto(oferta2, texto, "texto", hoy=date(2026, 7, 6))
    assert oferta2["jornada"] == "33 horas"      # el dato presente se respeta


# ── Vigencia calculada desde el PDF (no desde la tarjeta) ────────────────────
def test_vigencia_vencido_con_motivo():
    estado, motivo = B.calcular_vigencia(_bases(), hoy=date(2026, 7, 6))
    assert estado == "vencido"
    assert motivo == "La fecha de cierre de postulación fue 2024-06-17"


def test_vigencia_vigente():
    estado, motivo = B.calcular_vigencia(_bases(), hoy=date(2024, 6, 10))
    assert estado == "vigente"
    assert "2024-06-17" in motivo


def test_vigencia_sin_fecha():
    estado, motivo = B.calcular_vigencia({}, hoy=date(2026, 7, 6))
    assert estado == "sin_fecha"
    assert "revisar manualmente" in motivo


# ── Confianza ────────────────────────────────────────────────────────────────
def test_confianza():
    assert B.nivel_confianza(_bases(), "texto") == "alto"
    assert B.nivel_confianza({"fecha_cierre": date(2026, 8, 1), "texto_len": 500}, "texto") == "medio"
    assert B.nivel_confianza({"texto_len": 300}, "ocr") == "bajo"
    assert B.nivel_confianza({}, "sin_texto") == "manual"


# ── leer_pdf: sin capa de texto y sin OCR disponible → sin_texto ─────────────
def test_leer_pdf_escaneado_sin_ocr(monkeypatch):
    monkeypatch.setattr(B, "pytesseract", None)
    # bytes que parecen PDF pero sin texto extraíble
    texto, metodo = B.leer_pdf(b"%PDF-1.4 escaneo sin capa", allow_ocr=True)
    assert metodo == "sin_texto"


# ── Enriquecimiento del dict de oferta ───────────────────────────────────────
def test_enriquecer_oferta_completa_y_reemplaza_cargo_basura():
    oferta = {
        "cargo": "Profesionales EM, Para Maestranza Municipal, Cargo a Desempeñar: Profesional Maestranza, Según",
        "descripcion": None, "requisitos_texto": None,
        "fecha_cierre": None, "numero_vacantes": None,
    }
    res = B.enriquecer_desde_texto(oferta, _DECRETO, "ocr", hoy=date(2026, 7, 6))
    assert res["estado"] == "vencido"
    assert res["confianza"] == "alto"
    assert oferta["cargo"] == "Profesional Maestranza"          # título limpio
    assert oferta["fecha_cierre"] == date(2024, 6, 17)          # el PDF manda
    assert "Objetivo del cargo:" in oferta["descripcion"]
    assert "Funciones:" in oferta["descripcion"]
    assert "Grado 11" in oferta["descripcion"]
    assert "Título requerido:" in oferta["requisitos_texto"]
    assert "Competencias:" in oferta["requisitos_texto"]
    assert "fecha_cierre" in res["campos"] and "funciones" in res["campos"]


def test_enriquecer_no_pisa_datos_presentes():
    oferta = {"cargo": "Técnico Abastecimiento", "descripcion": "x" * 1900,
              "requisitos_texto": None, "numero_vacantes": 3}
    B.enriquecer_desde_texto(oferta, _DECRETO, "texto")
    assert oferta["cargo"] == "Técnico Abastecimiento"   # título corto/limpio se respeta
    assert oferta["numero_vacantes"] == 3                # no se pisa
    assert len(oferta["descripcion"]) >= 1900            # la más completa gana


def test_municipios_bases_estamento_grado_usa_lector_ocr(monkeypatch):
    """La Cisterna debe pasar por bases_pdf.leer_pdf(), no por _pdf_texto()."""
    import scrapers.municipios as M

    class Resp:
        content = b"%PDF-1.4 escaneado"

    fuente = {
        "bases_pdf": "https://example.test/bases.pdf",
        "url": "https://example.test/noticia",
    }
    llamadas = {}

    monkeypatch.setattr(M, "_get", lambda session, url: Resp())

    def fake_leer_pdf(contenido, *, allow_ocr=True, max_paginas=12):
        llamadas["allow_ocr"] = allow_ocr
        return (
            "5 cargos vacantes en Planta Profesionales Grado 9; "
            "2 cargos vacantes en Planta Auxiliares Grado 14",
            "ocr",
        )

    monkeypatch.setattr(B, "leer_pdf", fake_leer_pdf)
    items = M.extraer_bases_estamento_grado("", fuente, session=None, delay=0)

    assert llamadas == {"allow_ocr": True}
    assert [i["cargo"] for i in items] == ["Profesional Grado 9", "Auxiliar Grado 14"]
    assert all(i["url_bases"] == fuente["bases_pdf"] for i in items)
