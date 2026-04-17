"""Tests for the pure (no-I/O) helpers in db/database.py."""

from __future__ import annotations

import hashlib
import unittest

from db.database import (
    generar_id_estable,
    limpiar_texto,
    normalizar_area,
    normalizar_datos_oferta,
    normalizar_region,
    normalizar_tipo_cargo,
    truncar_texto,
    url_a_hash,
)


class UrlHashTests(unittest.TestCase):
    def test_hash_is_deterministic(self):
        url = "https://municipio.cl/concurso/1"
        self.assertEqual(url_a_hash(url), url_a_hash(url))

    def test_hash_is_sha256_hex(self):
        h = url_a_hash("https://x.cl/1")
        self.assertEqual(len(h), 64)
        int(h, 16)  # no ValueError ⇒ valid hex

    def test_hash_is_case_insensitive(self):
        self.assertEqual(
            url_a_hash("https://Site.cl/Oferta"),
            url_a_hash("https://site.cl/oferta"),
        )

    def test_hash_strips_surrounding_whitespace(self):
        self.assertEqual(
            url_a_hash("  https://site.cl/oferta  "),
            url_a_hash("https://site.cl/oferta"),
        )

    def test_hash_matches_sha256_of_normalized_url(self):
        url = "  https://SITE.cl/foo "
        expected = hashlib.sha256("https://site.cl/foo".encode()).hexdigest()
        self.assertEqual(url_a_hash(url), expected)

    def test_different_urls_produce_different_hashes(self):
        self.assertNotEqual(url_a_hash("https://a.cl/1"), url_a_hash("https://a.cl/2"))


class LimpiarTextoTests(unittest.TestCase):
    def test_collapses_whitespace(self):
        self.assertEqual(limpiar_texto("  foo   bar\tbaz\n"), "foo bar baz")

    def test_replaces_nbsp(self):
        self.assertEqual(limpiar_texto("foo\xa0bar"), "foo bar")

    def test_none_returns_empty_string(self):
        self.assertEqual(limpiar_texto(None), "")

    def test_empty_returns_empty(self):
        self.assertEqual(limpiar_texto(""), "")

    def test_non_string_is_coerced(self):
        self.assertEqual(limpiar_texto(123), "123")


class GenerarIdEstableTests(unittest.TestCase):
    def test_same_inputs_yield_same_id(self):
        self.assertEqual(
            generar_id_estable("fuente", "oferta", "1"),
            generar_id_estable("fuente", "oferta", "1"),
        )

    def test_length_respected(self):
        self.assertEqual(len(generar_id_estable("a", "b", largo=8)), 8)
        self.assertEqual(len(generar_id_estable("a", "b", largo=40)), 40)

    def test_default_length_is_20(self):
        self.assertEqual(len(generar_id_estable("a", "b")), 20)

    def test_none_parts_are_ignored(self):
        self.assertEqual(
            generar_id_estable("a", None, "b"),
            generar_id_estable("a", "b"),
        )

    def test_case_insensitive(self):
        self.assertEqual(
            generar_id_estable("FOO", "Bar"),
            generar_id_estable("foo", "bar"),
        )

    def test_whitespace_normalized_before_hashing(self):
        self.assertEqual(
            generar_id_estable("  foo  ", "bar"),
            generar_id_estable("foo", "bar"),
        )

    def test_different_inputs_yield_different_ids(self):
        self.assertNotEqual(
            generar_id_estable("a", "b"),
            generar_id_estable("a", "c"),
        )


class TruncarTextoTests(unittest.TestCase):
    def test_none_returns_none(self):
        self.assertIsNone(truncar_texto(None, 10))

    def test_empty_after_clean_returns_none(self):
        self.assertIsNone(truncar_texto("   ", 10))

    def test_shorter_than_max_returns_whole_string(self):
        self.assertEqual(truncar_texto("hola", 10), "hola")

    def test_longer_than_max_is_truncated(self):
        self.assertEqual(truncar_texto("hola mundo largo", 5), "hola ")

    def test_truncation_happens_after_cleaning(self):
        # Whitespace is collapsed first, then truncated.
        self.assertEqual(truncar_texto("foo   bar", 7), "foo bar")


class NormalizarDatosOfertaTests(unittest.TestCase):
    def _datos(self, **overrides):
        base = {
            "id_externo": "X-001",
            "cargo": "Analista",
            "institucion_nombre": "Municipalidad de Prueba",
            "sector": "Municipal",
            "area_profesional": "Administración",
            "tipo_cargo": "contrata",
            "nivel": "Profesional",
            "region": "Metropolitana",
            "ciudad": "Santiago",
            "renta_texto": "$1.500.000",
            "url_original": "https://site.cl/oferta",
        }
        base.update(overrides)
        return base

    def test_truncates_long_cargo(self):
        data = self._datos(cargo="A" * 1000)
        self.assertEqual(len(normalizar_datos_oferta(data)["cargo"]), 500)

    def test_truncates_each_limited_field(self):
        data = self._datos(
            id_externo="X" * 300,
            institucion_nombre="I" * 400,
            sector="S" * 100,
            area_profesional="A" * 200,
            tipo_cargo="T" * 80,
            nivel="N" * 120,
            region="R" * 200,
            ciudad="C" * 200,
            renta_texto="$" * 300,
        )
        normalized = normalizar_datos_oferta(data)
        self.assertEqual(len(normalized["id_externo"]), 200)
        self.assertEqual(len(normalized["institucion_nombre"]), 300)
        self.assertEqual(len(normalized["sector"]), 80)
        self.assertEqual(len(normalized["area_profesional"]), 100)
        self.assertEqual(len(normalized["tipo_cargo"]), 50)
        self.assertEqual(len(normalized["nivel"]), 80)
        self.assertEqual(len(normalized["region"]), 80)
        self.assertEqual(len(normalized["ciudad"]), 80)
        self.assertEqual(len(normalized["renta_texto"]), 200)

    def test_does_not_mutate_input(self):
        data = self._datos(cargo="A" * 1000)
        snapshot = dict(data)
        normalizar_datos_oferta(data)
        self.assertEqual(data, snapshot)

    def test_preserves_non_limited_fields(self):
        data = self._datos()
        data["url_original"] = "https://site.cl/oferta"
        normalized = normalizar_datos_oferta(data)
        self.assertEqual(normalized["url_original"], "https://site.cl/oferta")

    def test_missing_field_becomes_none(self):
        data = self._datos()
        data.pop("ciudad")
        normalized = normalizar_datos_oferta(data)
        self.assertIsNone(normalized["ciudad"])


class NormalizarRegionTests(unittest.TestCase):
    def test_none_input_returns_none(self):
        self.assertIsNone(normalizar_region(None))

    def test_empty_string_returns_none(self):
        self.assertIsNone(normalizar_region(""))

    def test_metropolitana_variants(self):
        for value in ["Metropolitana", "RM", "Santiago", "metropolitana de santiago"]:
            self.assertEqual(normalizar_region(value), "Metropolitana de Santiago")

    def test_accent_insensitive(self):
        self.assertEqual(normalizar_region("Valparaíso"), "Valparaíso")
        self.assertEqual(normalizar_region("Valparaiso"), "Valparaíso")

    def test_tarapaca_with_accent(self):
        self.assertEqual(normalizar_region("Tarapacá"), "Tarapacá")
        self.assertEqual(normalizar_region("tarapaca"), "Tarapacá")

    def test_ohiggins_variants(self):
        self.assertEqual(
            normalizar_region("Región del Libertador General Bernardo O'Higgins"),
            "O'Higgins",
        )
        self.assertEqual(normalizar_region("ohiggins"), "O'Higgins")

    def test_nuble(self):
        self.assertEqual(normalizar_region("Ñuble"), "Ñuble")
        self.assertEqual(normalizar_region("nuble"), "Ñuble")

    def test_biobio(self):
        self.assertEqual(normalizar_region("Biobío"), "Biobío")

    def test_araucania(self):
        self.assertEqual(normalizar_region("Araucanía"), "La Araucanía")

    def test_los_rios_vs_los_lagos(self):
        self.assertEqual(normalizar_region("Los Ríos"), "Los Ríos")
        self.assertEqual(normalizar_region("Los Lagos"), "Los Lagos")

    def test_unknown_region_title_cased(self):
        self.assertEqual(normalizar_region("galapagos"), "Galapagos")

    def test_case_insensitive_match(self):
        self.assertEqual(normalizar_region("AYSEN"), "Aysén")
        self.assertEqual(normalizar_region("magallanes"), "Magallanes")


class NormalizarTipoCargoTests(unittest.TestCase):
    def test_none_returns_none(self):
        self.assertIsNone(normalizar_tipo_cargo(None))

    def test_empty_returns_none(self):
        self.assertIsNone(normalizar_tipo_cargo(""))

    def test_planta(self):
        self.assertEqual(normalizar_tipo_cargo("Planta"), "Planta")
        self.assertEqual(normalizar_tipo_cargo("planta titular"), "Planta")

    def test_contrata(self):
        self.assertEqual(normalizar_tipo_cargo("Contrata"), "Contrata")
        self.assertEqual(normalizar_tipo_cargo("a contrata"), "Contrata")

    def test_honorarios(self):
        self.assertEqual(normalizar_tipo_cargo("Honorarios"), "Honorarios")
        self.assertEqual(normalizar_tipo_cargo("honorario asimilado"), "Honorarios")

    def test_adp(self):
        self.assertEqual(normalizar_tipo_cargo("ADP"), "ADP")
        self.assertEqual(normalizar_tipo_cargo("Alta Dirección Pública"), "ADP")

    def test_codigo_del_trabajo_accented_and_plain(self):
        self.assertEqual(
            normalizar_tipo_cargo("Código del Trabajo"), "Código del Trabajo"
        )
        self.assertEqual(
            normalizar_tipo_cargo("codigo del trabajo"), "Código del Trabajo"
        )

    def test_planta_takes_precedence_over_contrata(self):
        # The rules check "planta" first; if both words appear, planta wins.
        self.assertEqual(normalizar_tipo_cargo("planta o contrata"), "Planta")

    def test_unknown_title_cased(self):
        self.assertEqual(normalizar_tipo_cargo("suplencia"), "Suplencia")


class NormalizarAreaTests(unittest.TestCase):
    def test_default_is_administracion(self):
        self.assertEqual(normalizar_area("algo raro no clasificable"), "Administración")

    def test_derecho(self):
        for c in ["Abogado Municipal", "Asesor Jurídico", "Fiscal Regional"]:
            self.assertEqual(normalizar_area(c), "Derecho")

    def test_salud(self):
        # "médic" matches "médico" via substring "médic"; "enfermer", "matron"
        # match unaccented roots. "kinesiólogo"/"psicólogo" with accented "ó"
        # do NOT match "kinesiol"/"psicolog" — see test_accent_mismatch_known_quirk.
        for c in ["Médico General", "Enfermera", "Matrona", "Psiquiatra"]:
            self.assertEqual(normalizar_area(c), "Salud")

    def test_ingenieria(self):
        self.assertEqual(normalizar_area("Ingeniero Civil"), "Ingeniería")
        self.assertEqual(normalizar_area("Técnico Eléctrico"), "Ingeniería")

    def test_ciencias_sociales(self):
        self.assertEqual(normalizar_area("Trabajador Social"), "Ciencias Sociales")
        self.assertEqual(normalizar_area("Asistente social"), "Ciencias Sociales")

    def test_psicologia_unaccented_root(self):
        # The keyword "psicolog" only matches inputs without the "ó" accent.
        self.assertEqual(normalizar_area("Psicologo Clinico"), "Psicología")

    def test_accent_mismatch_known_quirk(self):
        # The keyword table uses unaccented roots ("psicolog", "kinesiol")
        # while real inputs often carry accents ("psicólogo", "kinesiólogo").
        # These fall through to the default bucket today.
        self.assertEqual(normalizar_area("Psicólogo Clínico"), "Administración")
        self.assertEqual(normalizar_area("Kinesiólogo"), "Administración")

    def test_finanzas(self):
        self.assertEqual(normalizar_area("Contador Auditor"), "Finanzas")
        self.assertEqual(normalizar_area("Analista de Finanzas"), "Finanzas")

    def test_economia(self):
        self.assertEqual(normalizar_area("Economista"), "Economía")

    def test_ti(self):
        self.assertEqual(normalizar_area("Ingeniero en Sistemas"), "Ingeniería")
        self.assertEqual(normalizar_area("Analista de Datos"), "TI")
        self.assertEqual(normalizar_area("Desarrollador Software"), "TI")

    def test_educacion(self):
        self.assertEqual(normalizar_area("Docente de Matemática"), "Educación")
        self.assertEqual(normalizar_area("Educadora de Párvulos"), "Educación")

    def test_comunicaciones(self):
        self.assertEqual(normalizar_area("Periodista Institucional"), "Comunicaciones")

    def test_agropecuario(self):
        self.assertEqual(normalizar_area("Médico Veterinario"), "Salud")
        self.assertEqual(normalizar_area("Ingeniero Agrónomo"), "Ingeniería")
        self.assertEqual(normalizar_area("Técnico Forestal"), "Ingeniería")

    def test_medioambiente(self):
        self.assertEqual(normalizar_area("Analista Ambiental"), "Medioambiente")

    def test_fiscalizacion(self):
        self.assertEqual(normalizar_area("Inspector Municipal"), "Fiscalización")

    def test_case_insensitive(self):
        self.assertEqual(normalizar_area("MÉDICO"), "Salud")
        self.assertEqual(normalizar_area("abogado"), "Derecho")


if __name__ == "__main__":
    unittest.main()
