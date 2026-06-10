from classification.policy import RULESET_VERSION, classify_offer_candidate


def test_policy_accepts_job_like_candidate() -> None:
    result = classify_offer_candidate(
        title="Concurso público para Analista",
        content_text="Postulaciones hasta 20/05/2026. Requisitos del cargo y renta bruta mensual.",
        url="https://example.cl/trabaja-con-nosotros/oferta-1",
    )
    assert result.likely_offer is True
    assert result.score > 0
    assert "policy_accept" in result.reason_codes
    assert result.ruleset_version == RULESET_VERSION


def test_policy_rejects_news_like_candidate() -> None:
    result = classify_offer_candidate(
        title="Noticias institucionales",
        content_text="Comunicado de cuenta pública anual",
        url="https://example.cl/noticias/comunicado",
    )
    assert result.likely_offer is False
    assert "negative_url_pattern" in result.reason_codes
    assert "policy_reject" in result.reason_codes
