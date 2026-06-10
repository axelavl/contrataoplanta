"""Tests para scrapers/frequency_policy.py."""

from __future__ import annotations

import unittest

from scrapers.frequency_policy import (
    FrequencyTier,
    TIER_HOURS,
    TIER_PROFILES,
    default_tier_for,
    hours_for_tier,
    profile_for_tier,
    resolve_tier,
)
from scrapers.source_status import ScraperKind, SourceStatus


class TierMappingsTests(unittest.TestCase):
    def test_critical_es_mas_frecuente_que_high(self):
        self.assertLess(TIER_HOURS[FrequencyTier.CRITICAL], TIER_HOURS[FrequencyTier.HIGH])

    def test_low_es_menos_frecuente_que_medium(self):
        self.assertGreater(TIER_HOURS[FrequencyTier.LOW], TIER_HOURS[FrequencyTier.MEDIUM])

    def test_eventual_es_la_mas_espaciada(self):
        # Eventual y exploratory son ambas muy espaciadas; eventual >= 1 semana
        self.assertGreaterEqual(TIER_HOURS[FrequencyTier.EVENTUAL], 168)

    def test_helpers_devuelven_lo_mismo_que_dicts(self):
        for tier in FrequencyTier:
            self.assertEqual(hours_for_tier(tier), TIER_HOURS[tier])
            self.assertEqual(profile_for_tier(tier), TIER_PROFILES[tier])


class TierProfilesTests(unittest.TestCase):
    def test_critical_open_pdf_y_playwright(self):
        prof = profile_for_tier(FrequencyTier.CRITICAL)
        self.assertTrue(prof.open_pdf)
        self.assertTrue(prof.use_playwright_fallback)

    def test_low_no_abre_pdf_ni_playwright(self):
        prof = profile_for_tier(FrequencyTier.LOW)
        self.assertFalse(prof.open_pdf)
        self.assertFalse(prof.use_playwright_fallback)

    def test_critical_tiene_mas_candidate_urls_que_low(self):
        self.assertGreater(
            profile_for_tier(FrequencyTier.CRITICAL).max_candidate_urls,
            profile_for_tier(FrequencyTier.LOW).max_candidate_urls,
        )

    def test_critical_tiene_mas_retries_que_low(self):
        self.assertGreater(
            profile_for_tier(FrequencyTier.CRITICAL).max_retries,
            profile_for_tier(FrequencyTier.LOW).max_retries,
        )


class DefaultTierTests(unittest.TestCase):
    def test_status_no_active_es_exploratory(self):
        for status in SourceStatus:
            if status == SourceStatus.ACTIVE:
                continue
            self.assertEqual(
                default_tier_for(
                    kind=ScraperKind.GENERIC, sector="Ejecutivo", status=status
                ),
                FrequencyTier.EXPLORATORY,
                status,
            )

    def test_empleos_publicos_es_critical(self):
        self.assertEqual(
            default_tier_for(
                kind=ScraperKind.EMPLEOS_PUBLICOS,
                sector="Ejecutivo",
                status=SourceStatus.ACTIVE,
            ),
            FrequencyTier.CRITICAL,
        )

    def test_municipios_son_low(self):
        self.assertEqual(
            default_tier_for(
                kind=ScraperKind.GENERIC,
                sector="Municipal",
                status=SourceStatus.ACTIVE,
            ),
            FrequencyTier.LOW,
        )

    def test_plataformas_centralizadas_son_high(self):
        for kind in (
            ScraperKind.CUSTOM_TRABAJANDO,
            ScraperKind.CUSTOM_HIRINGROOM,
            ScraperKind.CUSTOM_BUK,
        ):
            self.assertEqual(
                default_tier_for(
                    kind=kind, sector="Empresa Pública", status=SourceStatus.ACTIVE
                ),
                FrequencyTier.HIGH,
                kind,
            )

    def test_ffaa_y_policia_son_medium(self):
        for kind in (ScraperKind.CUSTOM_FFAA, ScraperKind.CUSTOM_POLICIA):
            self.assertEqual(
                default_tier_for(
                    kind=kind, sector="FF.AA. y Orden", status=SourceStatus.ACTIVE
                ),
                FrequencyTier.MEDIUM,
            )

    def test_sitio_propio_que_publica_en_ep_baja_a_low(self):
        # Si la fuente ya está cubierta por el batch de Empleos Públicos,
        # podemos espaciar la corrida del sitio propio.
        self.assertEqual(
            default_tier_for(
                kind=ScraperKind.GENERIC,
                sector="Ejecutivo",
                status=SourceStatus.ACTIVE,
                publica_en_empleospublicos="si",
            ),
            FrequencyTier.LOW,
        )

    def test_sitio_propio_que_no_publica_en_ep_es_medium(self):
        self.assertEqual(
            default_tier_for(
                kind=ScraperKind.GENERIC,
                sector="Ejecutivo",
                status=SourceStatus.ACTIVE,
                publica_en_empleospublicos="no",
            ),
            FrequencyTier.MEDIUM,
        )


class ResolveTierTests(unittest.TestCase):
    def test_override_gana(self):
        institucion = {"sector": "Municipal"}
        tier = resolve_tier(
            institucion,
            kind=ScraperKind.GENERIC,
            status=SourceStatus.ACTIVE,
            override={"frequency_tier": "critical"},
        )
        self.assertEqual(tier, FrequencyTier.CRITICAL)

    def test_override_invalido_se_ignora(self):
        tier = resolve_tier(
            {"sector": "Ejecutivo"},
            kind=ScraperKind.EMPLEOS_PUBLICOS,
            status=SourceStatus.ACTIVE,
            override={"frequency_tier": "ultra_critical"},
        )
        # Cae al default automático: empleos_publicos = critical
        self.assertEqual(tier, FrequencyTier.CRITICAL)

    def test_sin_override_usa_default(self):
        tier = resolve_tier(
            {"sector": "Municipal"},
            kind=ScraperKind.GENERIC,
            status=SourceStatus.ACTIVE,
        )
        self.assertEqual(tier, FrequencyTier.LOW)


if __name__ == "__main__":
    unittest.main()
