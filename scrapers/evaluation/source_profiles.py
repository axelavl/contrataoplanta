from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urlparse

from .models import ExtractorKind, PageType, RetryPolicy, SourceProfile


PROFILES: tuple[SourceProfile, ...] = (
    SourceProfile(
        name="empleos_publicos",
        threshold_family="trusted_portal",
        domains=("empleospublicos.cl",),
        platform_markers=("empleospublicos",),
        trusted_job_source=True,
        page_type_priors={PageType.LISTING_PAGE: 0.8, PageType.DETAIL_PAGE: 0.9},
        retry_policy=RetryPolicy.CRITICAL,
        extractor_hint=ExtractorKind.SCRAPER_EMPLEOS_PUBLICOS,
        extract_threshold=0.65,
        manual_threshold=0.45,
        notes="Fuente centralizada y confiable del Servicio Civil.",
    ),
    SourceProfile(
        name="carabineros_pdf_first",
        threshold_family="pdf_first_waf",
        domains=("postulaciones.carabineros.cl", "carabineros.cl"),
        institution_ids=(161,),
        candidate_urls=(
            "https://postulaciones.carabineros.cl/",
            "https://www.carabineros.cl/transparencia/concursos/",
        ),
        warmup_required=True,
        supports_pdf_enrichment=True,
        page_type_priors={PageType.DETAIL_PAGE: 0.8, PageType.DOCUMENT_PAGE: 0.95},
        retry_policy=RetryPolicy.HIGH,
        extractor_hint=ExtractorKind.SCRAPER_PDF_JOBS,
        extract_threshold=0.7,
        manual_threshold=0.5,
        notes="Usa descriptor y perfil PDF como fuente de verdad.",
    ),
    SourceProfile(
        name="pdi_pdf_first",
        threshold_family="pdf_first_waf",
        domains=("pdichile.cl", "postulaciones.investigaciones.cl"),
        institution_ids=(162,),
        candidate_urls=(
            "https://www.pdichile.cl/institucion/concursos-publicos/portada",
            "https://postulaciones.investigaciones.cl/",
        ),
        warmup_required=True,
        supports_pdf_enrichment=True,
        page_type_priors={PageType.DETAIL_PAGE: 0.75, PageType.DOCUMENT_PAGE: 0.95},
        retry_policy=RetryPolicy.HIGH,
        extractor_hint=ExtractorKind.SCRAPER_PDF_JOBS,
        extract_threshold=0.7,
        manual_threshold=0.5,
        notes="PDFs de perfil y bases pesan mas que el HTML.",
    ),
    SourceProfile(
        name="policia_waf",
        threshold_family="waf_protected",
        domains=("postulaciones.carabineros.cl", "postulaciones.investigaciones.cl"),
        warmup_required=True,
        supports_pdf_enrichment=True,
        page_type_priors={PageType.DETAIL_PAGE: 0.7},
        retry_policy=RetryPolicy.HIGH,
        extractor_hint=ExtractorKind.SCRAPER_CUSTOM_DETAIL,
        extract_threshold=0.72,
        manual_threshold=0.52,
        notes="Portales policiales con warmup y senales de WAF.",
    ),
    SourceProfile(
        name="ffaa_waf",
        threshold_family="waf_protected",
        domains=("ingreso.ejercito.cl", "admisionarmada.cl", "ejercito.cl", "armada.cl"),
        institution_ids=(157, 158),
        warmup_required=True,
        page_type_priors={PageType.LISTING_PAGE: 0.7, PageType.DETAIL_PAGE: 0.75},
        retry_policy=RetryPolicy.MEDIUM,
        extractor_hint=ExtractorKind.SCRAPER_CUSTOM_DETAIL,
        extract_threshold=0.72,
        manual_threshold=0.52,
        notes="Portales militares con rutas candidatas especificas y warmup.",
    ),
    SourceProfile(
        name="ats_trabajando",
        threshold_family="external_ats",
        domains=("trabajando.cl",),
        platform_markers=("trabajando.cl", "trabajando cl"),
        page_type_priors={PageType.ATS_EXTERNAL: 0.95},
        retry_policy=RetryPolicy.HIGH,
        extractor_hint=ExtractorKind.SCRAPER_EXTERNAL_ATS,
        extract_threshold=0.65,
        manual_threshold=0.45,
        notes="Portal ATS conocido: Trabajando.cl.",
    ),
    SourceProfile(
        name="ats_hiringroom",
        threshold_family="external_ats",
        domains=("hiringroom.com", "hiringroomcampus.com"),
        platform_markers=("hiringroom",),
        page_type_priors={PageType.ATS_EXTERNAL: 0.95},
        retry_policy=RetryPolicy.HIGH,
        extractor_hint=ExtractorKind.SCRAPER_EXTERNAL_ATS,
        extract_threshold=0.65,
        manual_threshold=0.45,
        notes="Portal ATS conocido: HiringRoom.",
    ),
    SourceProfile(
        name="ats_buk",
        threshold_family="external_ats",
        domains=("buk.cl",),
        platform_markers=("buk",),
        page_type_priors={PageType.ATS_EXTERNAL: 0.95},
        retry_policy=RetryPolicy.HIGH,
        extractor_hint=ExtractorKind.SCRAPER_EXTERNAL_ATS,
        extract_threshold=0.65,
        manual_threshold=0.45,
        notes="Portal ATS conocido: Buk.",
    ),
    SourceProfile(
        name="playwright_js",
        threshold_family="js_intensive",
        institution_ids=(145, 275, 280),
        domains=("bcentral.cl", "codelco.com", "tvn.cl"),
        supports_playwright=True,
        retry_policy=RetryPolicy.HIGH,
        extractor_hint=ExtractorKind.SCRAPER_PLAYWRIGHT,
        notes="Fuentes JS intensivas como Banco Central, Codelco y TVN.",
    ),
    SourceProfile(
        name="wordpress",
        threshold_family="wordpress",
        platform_markers=("wordpress", "wp-json", "wp-content"),
        page_type_priors={PageType.WORDPRESS_POST: 0.9, PageType.WORDPRESS_LISTING: 0.85},
        retry_policy=RetryPolicy.LOW,
        extractor_hint=ExtractorKind.SCRAPER_WORDPRESS_JOBS,
        extract_threshold=0.8,
        manual_threshold=0.6,
        notes="WordPress municipal o institucional con REST API y fallback HTML.",
    ),
    SourceProfile(
        name="generic_site",
        threshold_family="generic",
        page_type_priors={PageType.GENERAL_PAGE: 0.5, PageType.LISTING_PAGE: 0.5},
        retry_policy=RetryPolicy.MEDIUM,
        max_candidate_urls=4,
        extractor_hint=ExtractorKind.SCRAPER_GENERIC_FALLBACK,
        extract_threshold=0.78,
        manual_threshold=0.58,
        notes="Fallback defensivo para sitios propios y estructuras no clasificadas.",
    ),
)


@dataclass(slots=True)
class SourceProfileMatch:
    profile: SourceProfile
    matched_by: str
    evidence: list[str] = field(default_factory=list)
    source_requires_override: bool = False
    backlog_severity: str | None = None


_RUNTIME_PROFILE_HINTS: dict[str, str] = {
    "ats_trabajando": "ats_trabajando",
    "ats_hiringroom": "ats_hiringroom",
    "ats_buk": "ats_buk",
    "cms_wordpress": "wordpress",
}


def _generic_profile() -> SourceProfile:
    return next(profile for profile in PROFILES if profile.name == "generic_site")


def _find_profile_by_name(name: str) -> SourceProfile:
    return next(profile for profile in PROFILES if profile.name == name)


def classify_source_profile(
    source: dict[str, object],
    *,
    runtime_hints: tuple[str, ...] = (),
) -> SourceProfileMatch:
    inst_id = source.get("id")
    platform = str(source.get("plataforma_empleo") or "").lower()
    url_candidates = [str(source.get("url_empleo") or ""), str(source.get("sitio_web") or "")]
    host_candidates = [urlparse(url).netloc.lower() for url in url_candidates if url]

    for profile in PROFILES:
        if inst_id in profile.institution_ids:
            return SourceProfileMatch(profile=profile, matched_by="institution_id", evidence=[str(inst_id)])

    for profile in PROFILES:
        if any(domain and domain in host for domain in profile.domains for host in host_candidates):
            return SourceProfileMatch(profile=profile, matched_by="domain", evidence=list(host_candidates))

    for hint in runtime_hints:
        profile_name = _RUNTIME_PROFILE_HINTS.get(hint)
        if profile_name:
            return SourceProfileMatch(profile=_find_profile_by_name(profile_name), matched_by="runtime", evidence=[hint])

    # Override manual del catalogo: usar solo como ultimo recurso.
    for profile in PROFILES:
        if any(marker and marker in platform for marker in profile.platform_markers):
            severity = "high" if profile.name.startswith("ats_") else "medium"
            return SourceProfileMatch(
                profile=profile,
                matched_by="override",
                evidence=[platform],
                source_requires_override=True,
                backlog_severity=severity,
            )

    return SourceProfileMatch(profile=_generic_profile(), matched_by="fallback")


def match_source_profile(source: dict[str, object], *, runtime_hints: tuple[str, ...] = ()) -> SourceProfile:
    return classify_source_profile(source, runtime_hints=runtime_hints).profile
