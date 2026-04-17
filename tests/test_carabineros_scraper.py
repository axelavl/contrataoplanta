"""Tests para scrapers.plataformas.carabineros.

Los fixtures emulan la estructura observada empíricamente del portal
postulaciones.carabineros.cl (endpoints verificados vía índice público):

    GET /                                  → listado con <a href="/concursos/{id}">
    GET /concursos/{id}                    → detalle HTML con pares clave/valor
    GET /concursos/download/{id}/Perfil    → PDF con secciones canónicas

El fetch real NO se testea aquí (requiere red). Sí se testea la lógica
pura de parseo (_parse_listado, _parse_detalle_html, split_pdf_sections,
clean_pdf_text), que es donde falla el scraper cuando el layout cambia.
"""
from __future__ import annotations

import unittest

from bs4 import BeautifulSoup

from scrapers.plataformas.carabineros import (
    CarabinerosScraper,
    ConcursoRef,
    HEADERS_DESCRIPTOR,
    HEADERS_PERFIL,
    OfertaCarabineros,
    clean_pdf_text,
    split_pdf_sections,
)


LISTADO_HTML = """
<!doctype html>
<html><body>
  <main>
    <h1>Administrar Concursos</h1>
    <div class="grid">
      <article class="card">
        <a href="/concursos/1006">C-05-2025 — Administrativo Oficina de Partes</a>
        <p>Región Metropolitana de Santiago · Cierra 26/04/2026</p>
      </article>
      <article class="card">
        <a href="/concursos/1225">C-13-2026 — Conductor Planta</a>
        <p>Región de Tarapacá</p>
      </article>
      <article class="card">
        <a href="/concursos/download/1225/Descriptor">Descargar Descriptor</a>
      </article>
      <article class="card">
        <a href="/concursos/1006#detalle">Ver detalle (duplicado)</a>
      </article>
    </div>
    <nav class="pagination">
      <a href="/?page=2">Siguiente</a>
    </nav>
  </main>
</body></html>
"""


DETALLE_HTML = """
<!doctype html>
<html><body>
<main>
  <h1>Administrativo Oficina de Partes</h1>
  <h2>Concurso Público C-05-2025</h2>
  <dl>
    <dt>Región</dt><dd>Metropolitana de Santiago</dd>
    <dt>Comuna</dt><dd>Santiago</dd>
    <dt>Jornada</dt><dd>44 horas semanales</dd>
    <dt>Renta</dt><dd>$855.407</dd>
    <dt>N° de Vacantes</dt><dd>2</dd>
    <dt>Tipo de Contrato</dt><dd>Contrata</dd>
    <dt>Fecha de Publicación</dt><dd>10-04-2026</dd>
    <dt>Fecha de Cierre</dt><dd>26-04-2026</dd>
  </dl>
  <p>Postulaciones abiertas vía ClaveÚnica.</p>
  <a href="/concursos/download/1006/Descriptor">Descriptor</a>
  <a href="/concursos/download/1006/Perfil">Perfil</a>
</main>
</body></html>
"""


PDF_PERFIL_TEXT = """
PERFIL DEL CARGO — C-05-2025

OBJETIVO DEL CARGO
Dar soporte administrativo a la Oficina de Partes de la unidad.

FORMACIÓN EDUCACIONAL
Enseñanza Media completa. Deseable técnico administrativo.

EXPERIENCIA LABORAL
Mínimo 1 año en labores administrativas similares.

REQUISITOS LEGALES
Ser chileno, mayor de edad, sin antecedentes penales.

FUNCIONES DEL CARGO
- Recepcionar documentación oficial.
- Mantener registros físicos y digitales.
- Apoyar tareas de despacho.

ESPECIALIZACIÓN Y CAPACITACIÓN
Deseable curso Ley 19.880 y ofimática.

CONOCIMIENTOS Y COMPETENCIAS
Orientación al servicio, trabajo en equipo, Office nivel intermedio.

Página 1 de 2
"""


PDF_DESCRIPTOR_TEXT = """
DESCRIPTOR CONCURSO — C-05-2025

ANTECEDENTES GENERALES
Llamado a concurso público para proveer 2 cargos de Administrativo.

DOCUMENTACIÓN REQUERIDA
Currículum, copia CI, certificado de antecedentes, certificados estudios.

Página 2 de 2
"""


def _make_scraper() -> CarabinerosScraper:
    """Instancia el scraper en modo dry_run para no tocar la BD."""
    institucion = {
        "id": 161,
        "nombre": "Carabineros de Chile — Personal Civil",
        "sigla": "CARABINEROS",
        "region": "Nacional",
        "sitio_web": "https://www.carabineros.cl",
        "url_empleo": "https://postulaciones.carabineros.cl/",
    }
    return CarabinerosScraper(
        institucion=institucion,
        instituciones_catalogo=[institucion],
        dry_run=True,
        max_results=None,
    )


class ListadoParsingTests(unittest.TestCase):
    def test_extrae_ids_unicos_y_descarta_downloads(self):
        acc: dict[int, ConcursoRef] = {}
        nuevos = CarabinerosScraper._parse_listado(LISTADO_HTML, acc)
        self.assertEqual(nuevos, {1006, 1225})
        self.assertEqual(set(acc.keys()), {1006, 1225})
        self.assertIn("Administrativo", acc[1006].preview_title)
        self.assertTrue(acc[1006].url_detalle.endswith("/concursos/1006"))

    def test_segunda_llamada_no_duplica_ids(self):
        acc: dict[int, ConcursoRef] = {}
        CarabinerosScraper._parse_listado(LISTADO_HTML, acc)
        nuevos = CarabinerosScraper._parse_listado(LISTADO_HTML, acc)
        self.assertEqual(nuevos, set())  # no hay IDs nuevos en la segunda pasada

    def test_listado_vacio_no_rompe(self):
        acc: dict[int, ConcursoRef] = {}
        nuevos = CarabinerosScraper._parse_listado("<html><body></body></html>", acc)
        self.assertEqual(nuevos, set())


class DetalleParsingTests(unittest.TestCase):
    def test_parse_detalle_llena_campos_clave(self):
        scraper = _make_scraper()
        oferta = OfertaCarabineros(
            id=1006,
            url_detalle="https://postulaciones.carabineros.cl/concursos/1006",
        )
        scraper._parse_detalle_html(DETALLE_HTML, oferta)

        self.assertEqual(oferta.titulo_cargo, "Administrativo Oficina de Partes")
        self.assertEqual(oferta.codigo_concurso, "C-05-2025")
        self.assertEqual(oferta.region, "Metropolitana de Santiago")
        self.assertEqual(oferta.comuna, "Santiago")
        self.assertEqual(oferta.jornada, "44 horas semanales")
        self.assertEqual(oferta.vacantes, 2)
        self.assertIsNotNone(oferta.fecha_cierre)
        self.assertIsNotNone(oferta.fecha_publicacion)
        # El tipo de contrato pasa por normalize_tipo_contrato al momento de
        # construir el payload final; aquí basta con que se haya capturado.
        self.assertTrue(oferta.tipo_contrato)
        # Headings se preservan.
        self.assertIn("Administrativo Oficina de Partes", oferta.headings)


class PDFSectionTests(unittest.TestCase):
    def test_split_perfil_extrae_secciones_canonicas(self):
        secciones = split_pdf_sections(PDF_PERFIL_TEXT, HEADERS_PERFIL)
        # Todos los campos del Perfil deben aparecer.
        esperados = {"requisitos", "funciones", "formacion",
                     "experiencia", "capacitacion", "competencias"}
        self.assertTrue(esperados.issubset(secciones.keys()),
                        f"Faltan: {esperados - secciones.keys()}")
        self.assertIn("chileno", secciones["requisitos"].lower())
        self.assertIn("Recepcionar", secciones["funciones"])
        # No debe haber bleed-through: "funciones" no debe traer texto
        # de "requisitos" ni de "capacitación".
        self.assertNotIn("CHILENO", secciones["funciones"].upper())
        self.assertNotIn("LEY 19.880", secciones["funciones"])

    def test_split_descriptor_extrae_documentos_y_descripcion(self):
        secciones = split_pdf_sections(PDF_DESCRIPTOR_TEXT, HEADERS_DESCRIPTOR)
        self.assertIn("descripcion", secciones)
        self.assertIn("documentos", secciones)
        self.assertIn("Currículum", secciones["documentos"])

    def test_clean_pdf_text_colapsa_y_elimina_paginacion(self):
        raw = "Contenido importante.   \n\n\n\nPágina 3 de 5\n\nMás texto."
        out = clean_pdf_text(raw)
        self.assertNotIn("Página", out)
        self.assertIn("Contenido importante.", out)
        self.assertIn("Más texto.", out)

    def test_split_pdf_sections_vacio_devuelve_dict_vacio(self):
        self.assertEqual(split_pdf_sections("", HEADERS_PERFIL), {})
        self.assertEqual(split_pdf_sections("sin headers aquí", HEADERS_PERFIL), {})


class OfertaToRawTests(unittest.TestCase):
    def test_oferta_consolidada_integra_html_y_pdfs(self):
        scraper = _make_scraper()
        oferta = OfertaCarabineros(
            id=1006,
            url_detalle="https://postulaciones.carabineros.cl/concursos/1006",
            titulo_cargo="Administrativo",
            codigo_concurso="C-05-2025",
            html_text="Cuerpo del detalle HTML con información resumen.",
            pdf_perfil_text=PDF_PERFIL_TEXT,
            pdf_descriptor_text=PDF_DESCRIPTOR_TEXT,
            url_descriptor="https://postulaciones.carabineros.cl/concursos/download/1006/Descriptor",
            url_perfil="https://postulaciones.carabineros.cl/concursos/download/1006/Perfil",
        )
        raw = scraper._oferta_to_raw(oferta)

        self.assertEqual(raw["url"], oferta.url_detalle)
        self.assertEqual(raw["title"], "Administrativo")
        self.assertIn("FUNCIONES DEL CARGO", raw["content_text"])
        self.assertIn("DOCUMENTACIÓN REQUERIDA", raw["content_text"])
        self.assertEqual(len(raw["pdf_links"]), 2)
        self.assertEqual(len(raw["attachment_texts"]), 2)
        self.assertIs(raw["oferta"], oferta)


if __name__ == "__main__":
    unittest.main()
