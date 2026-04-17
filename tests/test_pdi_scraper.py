"""Tests para scrapers.plataformas.pdi.

Los fixtures emulan la estructura observada empíricamente del CMS
de www.pdichile.cl y los PDFs oficiales de PDI (perfiles y bases),
cuyos headers canónicos vienen indexados públicamente por Google.

No se prueba el fetch real (requiere red). Sí se prueban:
  - _parse_portada → detecta y empareja PDFs de perfil/bases.
  - split_pdf_sections con HEADERS_PERFIL_PDI / HEADERS_BASES_PDI.
  - _slug_from_url / _slug_to_title / _slugs_relacionados / _parsear_fechas_bases.
"""
from __future__ import annotations

import unittest

from scrapers.plataformas.pdi import (
    ConcursoPDI,
    HEADERS_BASES_PDI,
    HEADERS_PERFIL_PDI,
    PdiScraper,
    _extraer_titulo_desde_pdf,
    _parsear_fechas_bases,
    _slug_from_url,
    _slug_to_title,
    _slugs_relacionados,
    _stable_offer_url,
)
from scrapers.plataformas.carabineros import split_pdf_sections


PORTADA_HTML = """
<!doctype html>
<html><body>
<main>
  <h1>Concursos Públicos</h1>
  <section>
    <h2>Planta de Apoyo Científico Técnico</h2>
    <ul>
      <li>
        <a href="/docs/default-source/cargo/perfil-medico-criminalista-resolex-222.pdf?sfvrsn=58e48885_2">
          Médico Criminalista
        </a>
        <span>Santiago — cierre 13/03/2026</span>
      </li>
      <li>
        <a href="/docs/default-source/concurso-p%C3%BAblico---cargos/bases-medico-criminalista-resolex-222.pdf?sfvrsn=aa11_1">
          Bases Médico Criminalista
        </a>
      </li>
      <li>
        <a href="/docs/default-source/cargo/perfil-ingeniero-comercial-resolex-223.pdf?sfvrsn=9423a8ce_1">
          Ingeniero Comercial
        </a>
      </li>
      <li>
        <a href="/docs/default-source/concurso-p%C3%BAblico---cargos/bases-ingeniero-resolex-223.pdf?sfvrsn=bb22_1">
          Bases Ingeniero Comercial
        </a>
      </li>
    </ul>
  </section>
  <section>
    <h2>Documentos generales</h2>
    <a href="/docs/default-source/pdf/calendarizacion-2026-rev.pdf?sfvrsn=39db0a0f_1">
      Calendarización 2026
    </a>
    <a href="/institución/concursos-publicos/planta-oficiales">Planta de Oficiales</a>
  </section>
</main>
</body></html>
"""


PDF_PERFIL_PDI = """
PERFIL DE CARGO

I. IDENTIFICACIÓN DE LA OFERTA LABORAL
Cargo: Médico Criminalista
Grado E.U.S.: 5
Renta Bruta Aproximada: $1.940.577

II. PROPÓSITO DEL CARGO
Asesorar a la Policía de Investigaciones, Tribunales de Justicia y
Ministerio Público en materias médico-legales.

III. FUNCIONES DEL CARGO
- Emitir informes periciales médico-legales.
- Practicar autopsias médico-legales.
- Coordinar con equipos forenses del Ministerio Público.

IV. REQUISITOS LEGALES
Ser chileno, mayor de edad, sin antecedentes penales.

V. FORMACIÓN EDUCACIONAL
Título profesional de Médico Cirujano otorgado por universidad reconocida
por el Estado.

VI. EXPERIENCIA LABORAL
Mínimo 2 años en el ejercicio de la profesión.

VII. COMPETENCIAS
Responsabilidad, orientación a resultados, trabajo bajo presión,
confidencialidad.

VIII. DOCUMENTOS DE POSTULACIÓN
CV actualizado, copia de cédula de identidad, certificado de título,
declaración jurada de antecedentes.

Página 1 de 2
"""


PDF_BASES_PDI = """
BASES DE CONCURSO PÚBLICO RESOLEX 222

ANTECEDENTES GENERALES
Se llama a concurso público para proveer 1 cargo de Médico Criminalista
en Santiago. Modalidad: Contrata. Jornada: 44 horas semanales.

CALENDARIZACIÓN
Publicación del concurso: 03/03/2026
Recepción de antecedentes hasta: 13/03/2026
Evaluación curricular: 14-20/03/2026

DOCUMENTACIÓN REQUERIDA
Currículum, copia CI, certificado de antecedentes, certificados estudios.
"""


def _make_scraper() -> PdiScraper:
    inst = {
        "id": 162,
        "nombre": "Policía de Investigaciones — Personal Civil",
        "sigla": "PDI",
        "region": "Nacional",
        "sitio_web": "https://www.pdichile.cl",
        "url_empleo": "https://postulaciones.investigaciones.cl/",
    }
    return PdiScraper(
        institucion=inst,
        instituciones_catalogo=[inst],
        dry_run=True,
        max_results=None,
    )


class SlugHelpersTests(unittest.TestCase):
    def test_slug_from_url_quita_extension_y_querystring(self):
        url = (
            "https://www.pdichile.cl/docs/default-source/cargo/"
            "perfil-medico-criminalista-resolex-222.pdf?sfvrsn=58e48885_2"
        )
        self.assertEqual(
            _slug_from_url(url),
            "perfil-medico-criminalista-resolex-222",
        )

    def test_slug_from_url_decodifica_percent(self):
        url = (
            "https://www.pdichile.cl/docs/default-source/"
            "concurso-p%C3%BAblico---cargos/bases-medico.pdf"
        )
        self.assertEqual(_slug_from_url(url), "bases-medico")

    def test_slug_to_title_legible(self):
        self.assertEqual(
            _slug_to_title("perfil-medico-criminalista-resolex-222"),
            "Perfil Medico Criminalista",
        )

    def test_slugs_relacionados_por_resolex_comun(self):
        self.assertTrue(
            _slugs_relacionados(
                "perfil-medico-criminalista-resolex-222",
                "bases-medico-criminalista-resolex-222",
            )
        )

    def test_slugs_no_relacionados(self):
        self.assertFalse(
            _slugs_relacionados(
                "perfil-medico-criminalista-resolex-222",
                "bases-ingeniero-comercial-resolex-999",
            )
        )

    def test_slugs_no_relacionados_solo_por_anio(self):
        self.assertFalse(
            _slugs_relacionados(
                "perfil-analista-2026",
                "bases-abogado-2026",
            )
        )

    def test_slugs_relacionados_por_mes_y_anio(self):
        self.assertTrue(
            _slugs_relacionados(
                "perfil-analista-marzo-2026",
                "bases-analista-marzo-2026",
            )
        )

    def test_stable_offer_url_quita_query_y_fragment(self):
        self.assertEqual(
            _stable_offer_url(
                "https://www.pdichile.cl/docs/default-source/cargo/perfil.pdf?sfvrsn=58e4#top"
            ),
            "https://www.pdichile.cl/docs/default-source/cargo/perfil.pdf",
        )


class PortadaParsingTests(unittest.TestCase):
    def test_parse_portada_detecta_concursos_y_emparejamiento(self):
        scraper = _make_scraper()
        perfiles: dict[str, ConcursoPDI] = {}
        bases: dict[str, str] = {}
        subs: set[str] = set()
        scraper._parse_portada(
            PORTADA_HTML,
            "https://www.pdichile.cl/institución/concursos-publicos/portada",
            perfiles,
            bases,
            subs,
        )
        self.assertEqual(len(perfiles), 2)
        # 2 bases del concurso + 1 calendarización auxiliar que también
        # vive bajo /docs/default-source/pdf/ y que interesa rescatar.
        self.assertEqual(len(bases), 3)
        self.assertTrue(
            any("bases-medico-criminalista" in s for s in bases),
            f"Slugs detectados: {list(bases)}",
        )
        # La sub-sección "planta-oficiales" quedó en cola para recorrer.
        self.assertTrue(any("planta-oficiales" in u for u in subs))

    def test_enumerar_portada_empareja_perfil_con_bases(self):
        # Replica lo que hace _enumerar_portada pero sin red: alimenta
        # directo el HTML y ejecuta el pairing.
        scraper = _make_scraper()
        perfiles: dict[str, ConcursoPDI] = {}
        bases: dict[str, str] = {}
        subs: set[str] = set()
        scraper._parse_portada(
            PORTADA_HTML,
            "https://www.pdichile.cl/institución/concursos-publicos/portada",
            perfiles,
            bases,
            subs,
        )
        # Emparejar bases huérfanas (mismo algoritmo que en _enumerar_portada).
        for slug_bases, url_bases in bases.items():
            candidatos = [
                perfil
                for perfil in perfiles.values()
                if not perfil.url_bases and _slugs_relacionados(perfil.slug, slug_bases)
            ]
            if len(candidatos) == 1:
                candidatos[0].url_bases = url_bases

        medico = next(p for p in perfiles.values() if "medico" in p.slug)
        ingeniero = next(p for p in perfiles.values() if "ingeniero" in p.slug)
        self.assertIsNotNone(medico.url_bases)
        self.assertIn("bases-medico-criminalista", medico.url_bases)
        self.assertIsNotNone(ingeniero.url_bases)
        self.assertIn("bases-ingeniero", ingeniero.url_bases)


class PDFSectionTests(unittest.TestCase):
    def test_split_perfil_pdi_extrae_secciones(self):
        secciones = split_pdf_sections(PDF_PERFIL_PDI, HEADERS_PERFIL_PDI)
        esperados = {"descripcion", "funciones", "requisitos",
                     "formacion", "experiencia", "competencias", "documentos"}
        self.assertTrue(
            esperados.issubset(secciones.keys()),
            f"Faltan secciones: {esperados - secciones.keys()}",
        )
        self.assertIn("Asesorar", secciones["descripcion"])
        self.assertIn("autopsias", secciones["funciones"])
        self.assertIn("Médico Cirujano", secciones["formacion"])
        self.assertIn("CV actualizado", secciones["documentos"])
        # No debe haber sangrado de la sección siguiente.
        self.assertNotIn("CV actualizado", secciones["competencias"])

    def test_split_bases_pdi_detecta_calendario_y_documentos(self):
        secciones = split_pdf_sections(PDF_BASES_PDI, HEADERS_BASES_PDI)
        self.assertIn("calendario", secciones)
        self.assertIn("documentos_bases", secciones)
        self.assertIn("03/03/2026", secciones["calendario"])

    def test_parsear_fechas_bases_extrae_publicacion_y_cierre(self):
        concurso = ConcursoPDI(slug="bases-medico", bases_text=PDF_BASES_PDI)
        _parsear_fechas_bases(concurso)
        self.assertIsNotNone(concurso.fecha_publicacion)
        self.assertIsNotNone(concurso.fecha_cierre)
        self.assertEqual(str(concurso.fecha_publicacion), "2026-03-03")
        self.assertEqual(str(concurso.fecha_cierre), "2026-03-13")


class PDFHelperTests(unittest.TestCase):
    def test_extraer_titulo_desde_pdf_encuentra_linea_cargo(self):
        title = _extraer_titulo_desde_pdf(PDF_PERFIL_PDI)
        self.assertEqual(title, "Médico Criminalista")

    def test_extraer_titulo_desde_pdf_ninguno_si_no_hay(self):
        self.assertIsNone(_extraer_titulo_desde_pdf("Texto cualquiera sin etiqueta."))
        self.assertIsNone(_extraer_titulo_desde_pdf(""))


class OfertaToRawTests(unittest.TestCase):
    def test_concurso_to_raw_consolida_texto_y_pdfs(self):
        scraper = _make_scraper()
        concurso = ConcursoPDI(
            slug="perfil-medico-criminalista-resolex-222",
            url_perfil="https://www.pdichile.cl/docs/default-source/cargo/perfil-medico.pdf",
            url_bases="https://www.pdichile.cl/docs/default-source/concurso/bases-medico.pdf",
            titulo_cargo="Médico Criminalista",
            portada_snippet="Médico Criminalista — Santiago — cierre 13/03/2026",
            perfil_text=PDF_PERFIL_PDI,
            bases_text=PDF_BASES_PDI,
            secciones={
                "descripcion": "Asesorar en materias médico-legales.",
                "funciones": "Emitir informes periciales.",
                "requisitos": "Ser chileno, mayor de edad.",
            },
        )
        raw = scraper._concurso_to_raw(concurso)
        self.assertEqual(raw["url"], concurso.url_perfil)
        self.assertEqual(raw["title"], "Médico Criminalista")
        self.assertIn("Asesorar", raw["content_text"])
        self.assertIn("informes periciales", raw["content_text"])
        self.assertEqual(len(raw["pdf_links"]), 2)
        self.assertEqual(len(raw["attachment_texts"]), 2)
        self.assertIs(raw["concurso"], concurso)

    def test_id_estable_por_slug(self):
        c1 = ConcursoPDI(slug="perfil-medico-criminalista-resolex-222")
        c2 = ConcursoPDI(
            slug="perfil-medico-criminalista-resolex-222",
            url_perfil="otra-url-distinta.pdf?sfvrsn=11_2",
        )
        # El ID depende sólo del slug, no de la URL ni del sfvrsn.
        self.assertEqual(c1.id, c2.id)
        self.assertNotEqual(
            c1.id,
            ConcursoPDI(slug="perfil-ingeniero-comercial-resolex-223").id,
        )


if __name__ == "__main__":
    unittest.main()
