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
