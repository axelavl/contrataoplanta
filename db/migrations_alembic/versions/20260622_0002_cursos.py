"""Tabla `cursos` para el directorio de Cursos y especializaciones (admin-gestionable)

Mueve el directorio de cursos desde el archivo estático `web/cursos-data.js` a una
tabla en la DB, para poder administrarlo (crear/editar/borrar) desde el panel
admin. El frontend (`web/cursos.js`) lee `GET /api/cursos` y cae al archivo
estático si la API no responde.

Campos espejo de la estructura del aviso de cursos: curso_id (slug), titulo,
proveedor, categoria, modalidad, duracion, nivel (destacado|estandar), url,
descripcion, gratuito, demo, orden, activo.

Seed: inserta los cursos GRATUITOS oficiales del Estado (AcademiaCEA/Contraloría,
ChileCompra, Servicio Civil, SUBDERE). Idempotente vía ON CONFLICT (curso_id).

Revision ID: 20260622_0002_cursos
Revises: 20260622_0001_dedup_renta
Create Date: 2026-06-22
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


revision: str = "20260622_0002_cursos"
down_revision: Union[str, None] = "20260622_0001_dedup_renta"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS cursos (
            id              SERIAL PRIMARY KEY,
            curso_id        VARCHAR(80) UNIQUE,
            titulo          VARCHAR(300) NOT NULL,
            proveedor       VARCHAR(200),
            categoria       VARCHAR(40),
            modalidad       VARCHAR(80),
            duracion        VARCHAR(60),
            nivel           VARCHAR(20) DEFAULT 'estandar',
            url             TEXT,
            descripcion     TEXT,
            gratuito        BOOLEAN DEFAULT TRUE,
            demo            BOOLEAN DEFAULT FALSE,
            orden           INTEGER DEFAULT 100,
            activo          BOOLEAN DEFAULT TRUE,
            creado_en       TIMESTAMPTZ DEFAULT NOW(),
            actualizada_en  TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_cursos_activo_orden ON cursos (activo, orden)"
    )

    cursos = [
        # (curso_id, titulo, proveedor, categoria, modalidad, duracion, url, descripcion, orden)
        ("cea-induccion-estado", "Inducción General a la Administración del Estado", "AcademiaCEA · Contraloría (CGR)", "admin-publica", "Online · Clave Única", "A tu ritmo", "https://www.ceacgr.cl/aulavirtual/", "Curso base sobre el Estado, sus instituciones, principios y el rol del funcionario público. Punto de partida ideal para quien recién ingresa.", 10),
        ("cea-estatuto-administrativo", "Estatuto Administrativo", "AcademiaCEA · Contraloría (CGR)", "rrhh", "Online · Clave Única", "A tu ritmo", "https://www.ceacgr.cl/aulavirtual/", "Derechos, deberes, carrera funcionaria, calidades jurídicas (planta, contrata) y régimen disciplinario del personal del Estado.", 20),
        ("cea-derecho-administrativo", "Derecho Administrativo", "AcademiaCEA · Contraloría (CGR)", "derecho", "Online · Clave Única", "A tu ritmo", "https://www.ceacgr.cl/aulavirtual/", "Acto administrativo, procedimiento (Ley 19.880), plazos, notificaciones y control de la legalidad.", 30),
        ("cea-etica-transparencia-lobby", "Ética, Probidad, Transparencia y Lobby", "AcademiaCEA · Contraloría (CGR)", "derecho", "Online · Clave Única", "A tu ritmo", "https://www.ceacgr.cl/aulavirtual/", "Probidad administrativa, conflictos de interés, transparencia activa y Ley del Lobby. Anticorrupción en el ejercicio del cargo.", 40),
        ("cea-ley-karin", "Ley Karin: prevención del acoso laboral y sexual", "AcademiaCEA · Contraloría (CGR)", "rrhh", "Online · Clave Única", "A tu ritmo", "https://www.ceacgr.cl/aulavirtual/", "Marco de la Ley Karin, prevención del acoso y la violencia en el trabajo, y procedimientos de denuncia en el Estado.", 50),
        ("cea-ciberseguridad", "Ciberseguridad en el sector público", "AcademiaCEA · Contraloría (CGR)", "ti", "Online · Clave Única", "A tu ritmo", "https://www.ceacgr.cl/aulavirtual/", "Buenas prácticas de seguridad de la información, gestión de riesgos y protección de datos en instituciones del Estado.", 60),
        ("cea-administracion-financiera", "Administración Financiera del Estado", "AcademiaCEA · Contraloría (CGR)", "finanzas", "Online · Clave Única", "A tu ritmo", "https://www.ceacgr.cl/aulavirtual/", "Presupuesto público, contabilidad gubernamental (NICSP) y rendición de cuentas en el sector público.", 70),
        ("cea-induccion-municipal", "Inducción para el Sector Municipal", "AcademiaCEA · Contraloría (CGR)", "admin-publica", "Online · Clave Única", "A tu ritmo", "https://www.ceacgr.cl/aulavirtual/", "Funcionamiento municipal, competencias del municipio y particularidades del régimen aplicable a su personal.", 80),
        ("chilecompra-cert-basico", "Certificación en Compras Públicas — Nivel Básico", "ChileCompra · Mercado Público", "compras", "Online · Clave Única", "36 h", "https://www.chilecompra.cl/certificacion/", "Normativa (Ley 19.886), uso de la plataforma Mercado Público y ética en las compras. Para operar tareas diarias de abastecimiento.", 90),
        ("chilecompra-cert-intermedio", "Certificación en Compras Públicas — Nivel Intermedio", "ChileCompra · Mercado Público", "compras", "Online · Clave Única", "44 h", "https://www.chilecompra.cl/certificacion/", "Evaluación de ofertas y gestión de contratos, para supervisores y gestores de contratos del Estado.", 100),
        ("chilecompra-cert-avanzado", "Certificación en Compras Públicas — Nivel Avanzado", "ChileCompra · Mercado Público", "compras", "Online · Clave Única", "59 h", "https://www.chilecompra.cl/certificacion/", "Abastecimiento estratégico y análisis de datos para compras de alta complejidad. Para abogados, auditores y administradores.", 110),
        ("chilecompra-capacitacion", "Cursos gratuitos para compradores y proveedores del Estado", "ChileCompra · Mercado Público", "compras", "Online · Clave Única", "Todo el año", "https://capacitacion.chilecompra.cl/", "Catálogo permanente de cursos y charlas online y gratuitas sobre la operación de Mercado Público.", 120),
        ("serviciocivil-gestion-personas", "Gestión y Desarrollo de Personas en el Estado", "Campus · Servicio Civil", "rrhh", "Online", "A tu ritmo", "https://campus.serviciocivil.cl/", "Formación transversal para servidores públicos en gestión y desarrollo de personas, con mirada de Estado.", 130),
        ("serviciocivil-inclusion", "Inclusión laboral y gestión de la diversidad", "Campus · Servicio Civil", "rrhh", "Online", "A tu ritmo", "https://campus.serviciocivil.cl/", "Inclusión de personas con discapacidad y gestión inclusiva de la diversidad en los servicios públicos.", 140),
        ("serviciocivil-capacitacion", "Fundamentos para la Gestión de la Capacitación", "Campus · Servicio Civil", "rrhh", "Online", "A tu ritmo", "https://campus.serviciocivil.cl/", "Cómo planificar, ejecutar y evaluar la capacitación en un servicio público.", 150),
        ("subdere-municipal-regional", "Formación para funcionarios/as municipales y regionales", "Academia SUBDERE", "admin-publica", "Online", "20–40 h", "https://academia.subdere.gov.cl/", "Diplomados y cursos gratuitos para personal de municipios y gobiernos regionales, dictados por universidades acreditadas.", 160),
    ]
    bind = op.get_bind()
    insert_sql = text(
        """
        INSERT INTO cursos
          (curso_id, titulo, proveedor, categoria, modalidad, duracion,
           nivel, url, descripcion, gratuito, demo, orden, activo)
        VALUES
          (:cid, :tit, :prov, :cat, :mod, :dur,
           'estandar', :url, :desc, TRUE, FALSE, :ord, TRUE)
        ON CONFLICT (curso_id) DO NOTHING
        """
    )
    for c in cursos:
        bind.execute(insert_sql, {
            "cid": c[0], "tit": c[1], "prov": c[2], "cat": c[3], "mod": c[4],
            "dur": c[5], "url": c[6], "desc": c[7], "ord": c[8],
        })


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS cursos")
