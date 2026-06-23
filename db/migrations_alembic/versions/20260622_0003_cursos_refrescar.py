"""Refresca descripciones (humanizadas) y enlaces de catálogo de los cursos sembrados

Los cursos se sembraron en `20260622_0002_cursos`. Esta migración actualiza, por
`curso_id`, la `descripcion` (redactadas más naturales, menos plantilla) y la
`url` (a la página de catálogo del organismo: la más directa y pública, ya que
los cursos puntuales se abren tras iniciar sesión con Clave Única).

Solo toca esas columnas y solo de los cursos del seed oficial; cualquier curso
creado o editado luego desde el admin no se ve afectado salvo coincidencia de
curso_id.

Revision ID: 20260622_0003_cursos_refr
Revises: 20260622_0002_cursos
Create Date: 2026-06-22
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


revision: str = "20260622_0003_cursos_refr"
down_revision: Union[str, None] = "20260622_0002_cursos"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_CEA = "https://www.ceacgr.cl/aulavirtual/course/index.php"

# (curso_id, url, descripcion)
_REFRESH = [
    ("cea-induccion-estado", _CEA, "Si recién entras al Estado, parte por acá: cómo se organiza la administración pública y qué se espera de un funcionario. Es el curso con el que la propia Contraloría pone a todos en la misma página."),
    ("cea-estatuto-administrativo", _CEA, "El reglamento que rige tu día a día en el Estado: ingreso, deberes y derechos, permisos, calificaciones y la carrera funcionaria. Casi obligado si eres planta o contrata."),
    ("cea-derecho-administrativo", _CEA, "Cómo actúa legalmente el Estado: el acto administrativo, los plazos de la Ley 19.880, las notificaciones y el control de Contraloría. Te ahorra tropezar con la forma."),
    ("cea-etica-transparencia-lobby", _CEA, "Dónde está la raya: probidad, conflictos de interés, qué se publica por transparencia y cómo se registran las reuniones de lobby. La base para cuidar tu cargo."),
    ("cea-ley-karin", _CEA, "Qué cambió con la Ley Karin y qué hacer frente al acoso o la violencia en el trabajo público: prevención, canales de denuncia y el rol de cada quien."),
    ("cea-ciberseguridad", _CEA, "Para no ser el eslabón débil: contraseñas, phishing, manejo de datos y resguardo de la información en un servicio público. Sin tecnicismos innecesarios."),
    ("cea-administracion-financiera", _CEA, "Cómo se mueve la plata del Estado: presupuesto, contabilidad gubernamental (NICSP) y rendición de cuentas. Para quien trabaja en finanzas o quiere entenderlas de una vez."),
    ("cea-induccion-municipal", _CEA, "La versión municipal de la inducción: cómo funciona un municipio, qué puede y qué no, y las reglas propias del personal municipal."),
    ("chilecompra-cert-basico", "https://www.chilecompra.cl/certificacion/", "El primer escalón para trabajar con Mercado Público: la Ley 19.886, cómo se usa la plataforma y la ética en las compras. Si recién partes en abastecimiento, este es el tuyo."),
    ("chilecompra-cert-intermedio", "https://www.chilecompra.cl/certificacion/", "Para quien ya opera y quiere subir de nivel: evaluación de ofertas y administración de contratos. Pensado para supervisores y gestores de contrato."),
    ("chilecompra-cert-avanzado", "https://www.chilecompra.cl/certificacion/", "El nivel experto: abastecimiento estratégico y análisis de datos para compras complejas. Apunta a abogados, auditores y administradores de contrato."),
    ("chilecompra-capacitacion", "https://capacitacion.chilecompra.cl/", "Aparte de la certificación, ChileCompra dicta charlas y cursos cortos online durante todo el año sobre cómo comprar y vender al Estado. Conviene mirarlo seguido."),
    ("serviciocivil-gestion-personas", "https://campus.serviciocivil.cl/", "Gestión de personas con mirada de Estado: liderazgo, clima y desarrollo de equipos en el sector público. Transversal, sirvas donde sirvas."),
    ("serviciocivil-inclusion", "https://campus.serviciocivil.cl/", "Cómo hacer del servicio público un lugar más inclusivo: discapacidad, diversidad y los ajustes que la ley exige. Muy útil para jefaturas y RR.HH."),
    ("serviciocivil-capacitacion", "https://campus.serviciocivil.cl/", "El cómo detrás de la capacitación: detectar necesidades, armar el plan y evaluar si de verdad sirvió. Para quienes gestionan formación en su servicio."),
    ("subdere-municipal-regional", "https://academia.subdere.gov.cl/?page_id=1880", "Diplomados y cursos gratis para funcionarios de municipios y gobiernos regionales, dictados por universidades acreditadas. Las becas salen por convocatoria; conviene estar atento al calendario del año."),
]

# Títulos que también cambiaron (más naturales).
_TITULOS = {
    "chilecompra-capacitacion": "Cursos y charlas gratis sobre Mercado Público",
}


def upgrade() -> None:
    bind = op.get_bind()
    upd = text(
        "UPDATE cursos SET descripcion = :desc, url = :url, actualizada_en = NOW() "
        "WHERE curso_id = :cid"
    )
    for cid, url, desc in _REFRESH:
        bind.execute(upd, {"cid": cid, "url": url, "desc": desc})
    upd_tit = text("UPDATE cursos SET titulo = :tit WHERE curso_id = :cid")
    for cid, tit in _TITULOS.items():
        bind.execute(upd_tit, {"cid": cid, "tit": tit})


def downgrade() -> None:
    # No se revierte el texto (no guardamos el anterior); es no-destructivo.
    pass
