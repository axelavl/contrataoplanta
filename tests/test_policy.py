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


def test_policy_rejects_transparencia_personal_planta() -> None:
    # Caso real: Empresa Portuaria Coquimbo publicaba su sección de
    # transparencia "Directorio y Personal Planta" y quedaba como oferta
    # porque "planta" era keyword positiva a secas (auditoría 2026-06-11).
    result = classify_offer_candidate(
        title="Directorio y Personal Planta",
        content_text="Directorio y Personal Planta",
        url="https://puertocoquimbo.cl/transparencia/directorio-personal-planta/",
    )
    assert result.likely_offer is False
    assert "policy_reject" in result.reason_codes


def test_policy_rejects_transparencia_index() -> None:
    result = classify_offer_candidate(
        title="Transparencia",
        content_text=(
            "Transparencia Marco Normativo Estructura Orgánica Filiales o "
            "Coligadas Compras y Adquisiciones Estados Financieros Memorias "
            "Anuales y Reportabilidad Remuneración y Antecedentes Directorio "
            "y Personal Planta"
        ),
        url="https://puertocoquimbo.cl/",
    )
    assert result.likely_offer is False


def test_policy_still_accepts_transparencia_trabaje_con_nosotros() -> None:
    # "/transparencia" completo NO se penaliza: ahí viven páginas legítimas.
    result = classify_offer_candidate(
        title="Trabaje con nosotros — Concurso público Analista de Operaciones",
        content_text="Convocatoria vigente. Requisitos del cargo y postulaciones hasta el cierre.",
        url="https://institucion.cl/transparencia/trabaje-con-nosotros/concurso-analista",
    )
    assert result.likely_offer is True
