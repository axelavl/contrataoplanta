from __future__ import annotations

from datetime import datetime, timezone
import unittest

from classification.content_classifier import ContentClassifier
from extraction.field_extractors import extract_structured_fields
from models.raw_page import RawPage
from scrapers.job_pipeline import JobExtractionPipeline


def make_page(**kwargs) -> RawPage:
    base = dict(
        source_id="src-1",
        source_name="Municipio X",
        platform="wordpress",
        url="https://municipio.cl/concursos/oferta-1",
        final_url=None,
        title="Concurso Público Profesional",
        meta_description=None,
        breadcrumbs=["Inicio", "Concursos"],
        section_hint="concursos",
        html_text="Cargo: Profesional. Requisitos del cargo: título profesional. Fecha de cierre: 15 de abril de 2026.",
        tables_text=[],
        attachment_urls=[],
        attachment_texts=[],
        found_dates=[],
        discovered_at=datetime.now(timezone.utc),
        http_status=200,
        headings=["Concurso Público Profesional"],
    )
    base.update(kwargs)
    return RawPage(**base)


class PipelineTests(unittest.TestCase):
    def test_01_noticia_institucional_se_rechaza(self):
        page = make_page(
            url="https://municipio.cl/noticias/concurso-de-ideas",
            html_text="Noticia institucional sobre concurso de fotografía. Evento abierto.",
            title="Noticia: concurso de ideas",
        )
        result = ContentClassifier().classify(page)
        self.assertFalse(result.is_job_posting)

    def test_02_concurso_vigente_con_bases_pdf_acepta(self):
        page = make_page(
            attachment_urls=["https://municipio.cl/docs/bases-del-concurso.pdf"],
            attachment_texts=["Bases del concurso. Funciones del cargo..."],
        )
        posting, _ = JobExtractionPipeline().run(page)
        self.assertIsNotNone(posting)

    def test_03_aviso_historico_2022(self):
        page = make_page(
            html_text="Concurso público 2022. Proceso finalizado.",
            title="Concurso 2022",
        )
        posting, trace = JobExtractionPipeline().run(page)
        self.assertIsNone(posting)
        self.assertTrue(trace["rejection_reasons"])

    def test_04_resultados_nomina_rechaza(self):
        page = make_page(
            html_text="Resultados del concurso. Nómina de seleccionados y adjudicación final.",
            title="Resultados concurso",
        )
        result = ContentClassifier().classify(page)
        self.assertFalse(result.is_job_posting)
        self.assertIn(result.content_type, {"results_page", "news_article", "informational_page"})

    def test_05_funciones_requisitos_solo_pdf(self):
        page = make_page(
            html_text="Convocatoria abierta. Postulación vigente.",
            attachment_urls=["https://site.cl/perfil-cargo.pdf"],
            attachment_texts=[
                "Nombre del cargo: Analista. Funciones del cargo\n- Liderar reportes\n"
                "Requisitos del cargo\n- Título profesional\n- 3 años experiencia"
            ],
        )
        extracted = extract_structured_fields(page)
        self.assertTrue(extracted.functions)
        self.assertTrue(extracted.requirements)

    def test_06_index_sin_detalle_no_almacena(self):
        page = make_page(
            title="Trabaja con nosotros",
            html_text="Listado de oportunidades laborales. Ver detalles en tarjetas.",
            headings=["Trabaja con nosotros"],
        )
        posting, _ = JobExtractionPipeline().run(page)
        self.assertIsNone(posting)

    def test_07_tabla_sueldo_jornada_contrato(self):
        page = make_page(
            tables_text=["Cargo | Renta | Cierre | Jornada | Contrata", "Analista | $1.500.000 | 20/04/2026 | jornada completa | contrata"],
            html_text="Proceso de selección. Requisitos del cargo: experiencia.",
        )
        extracted = extract_structured_fields(page)
        self.assertEqual(extracted.contract_type, "contrata")
        self.assertEqual(extracted.workday, "completa")
        self.assertTrue(extracted.salary_amount)

    def test_08_ambiguo_usa_llm_fallback(self):
        page = make_page(
            title="Convocatoria",
            html_text="Cargo: Analista. Requisitos del cargo: experiencia. Postulación abierta. Comunicado institucional.",
            url="https://site.cl/convocatoria-2026",
        )
        result = ContentClassifier(accept_threshold=0.9, ambiguity_threshold=0.55).classify(page)
        self.assertTrue(result.used_llm)


if __name__ == "__main__":
    unittest.main()
