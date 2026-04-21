from __future__ import annotations

from datetime import datetime, timezone
import unittest

from classification.rule_engine import RuleEngine
from extraction.email_extractor import extract_and_classify_emails
from extraction.salary_extractor import extract_salary
from models.raw_page import RawPage


def _page(text: str, *, title: str = "Convocatoria", url: str = "https://muni.cl/concursos/1") -> RawPage:
    return RawPage(
        source_id="x",
        source_name="Muni X",
        platform="generic",
        url=url,
        final_url=url,
        title=title,
        meta_description=None,
        breadcrumbs=[],
        section_hint=None,
        html_text=text,
        tables_text=[],
        attachment_urls=[],
        attachment_texts=[],
        found_dates=[],
        discovered_at=datetime.now(timezone.utc),
        http_status=200,
        headings=[],
    )


class SalaryExtractorTests(unittest.TestCase):
    def test_descarta_monto_absurdo(self):
        sample = "Presupuesto total del programa: $18.450.000.000 para ejecución anual."
        result = extract_salary(sample)
        self.assertIsNone(result.amount)
        self.assertIn(result.validation_status, {"remuneracion_descartada", "remuneracion_no_confiable"})

    def test_detecta_monto_mensual_valido(self):
        sample = "Renta bruta mensual del cargo: $1.450.000. Jornada completa."
        result = extract_salary(sample)
        self.assertEqual(result.amount, 1450000.0)
        self.assertEqual(result.currency, "CLP")
        self.assertIsNone(result.validation_status)

    def test_descarta_monto_de_presupuesto_y_prioriza_renta(self):
        sample = (
            "Presupuesto del programa: $48.000.000 anual. "
            "Renta bruta mensual ofrecida: $1.200.000."
        )
        result = extract_salary(sample)
        self.assertEqual(result.amount, 1200000.0)
        self.assertIsNone(result.validation_status)


class EmailExtractorTests(unittest.TestCase):
    def test_clasifica_postulacion_y_consultas(self):
        sample = (
            "Enviar antecedentes a seleccion@muni.cl hasta el viernes. "
            "Para consultas sobre el proceso: consultas@muni.cl"
        )
        result = extract_and_classify_emails(sample)
        self.assertEqual(result.postulacion_channel, "postulacion_mixta")
        self.assertTrue(any(item.email == "seleccion@muni.cl" and "email_postulacion" in item.kinds for item in result.classified))
        self.assertTrue(any(item.email == "consultas@muni.cl" and "email_consultas" in item.kinds for item in result.classified))

    def test_mismo_correo_para_postulacion_y_consultas(self):
        sample = (
            "Enviar antecedentes y consultas del proceso a seleccion@muni.cl "
            "hasta el cierre."
        )
        result = extract_and_classify_emails(sample)
        email = next(item for item in result.classified if item.email == "seleccion@muni.cl")
        self.assertIn("email_postulacion", email.kinds)
        self.assertIn("email_consultas", email.kinds)
        self.assertIn("correo_postulacion", email.kinds)
        self.assertIn("correo_contacto", email.kinds)


class RuleEnginePrecisionTests(unittest.TestCase):
    def test_descarta_noticia_ambigua(self):
        page = _page(
            "Noticia institucional: convocatoria comunitaria para taller de empleo y feria.",
            title="Convocatoria comunitaria",
            url="https://muni.cl/noticias/convocatoria-comunitaria",
        )
        result = RuleEngine().classify_with_rules(page)
        self.assertFalse(result.is_job_posting)
        self.assertIn(result.content_type, {"news_article", "event", "informational_page"})

    def test_acepta_llamado_laboral_con_senales_fuertes(self):
        page = _page(
            "Concurso público. Perfil del cargo y requisitos del cargo. "
            "Postulaciones hasta el 20/04/2026. Enviar antecedentes a rrhh@muni.cl.",
            title="Llamado a postulación profesional",
            url="https://muni.cl/concursos-publicos/llamado-01",
        )
        result = RuleEngine().classify_with_rules(page)
        self.assertTrue(result.is_job_posting)


if __name__ == "__main__":
    unittest.main()
