"""Dashboard operativo del scraper usando Streamlit.

Uso:
    streamlit run scripts/streamlit_scraper_dashboard.py

Variables opcionales:
    SCRAPER_API_BASE_URL=http://localhost:8000
"""

from __future__ import annotations

import os
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import requests
import streamlit as st

DEFAULT_API_BASE = os.getenv("SCRAPER_API_BASE_URL", "http://localhost:8000").rstrip("/")
TIMEOUT_SECONDS = 20


def _safe_get_json(url: str) -> dict[str, Any]:
    try:
        response = requests.get(url, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            return payload
        return {}
    except requests.RequestException as exc:
        st.error(f"No se pudo consultar {url}: {exc}")
        return {}


def _fmt_now() -> str:
    now = datetime.now(timezone.utc).astimezone()
    return now.strftime("%Y-%m-%d %H:%M:%S %Z")


def _cards_resumen(resumen: dict[str, Any]) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total fuentes", str(resumen.get("total", 0)))
    c2.metric("Activas", str(resumen.get("activas", 0)))
    c3.metric("Cobertura activa", f"{resumen.get('cobertura_activa_pct', 0.0)}%")
    c4.metric("Disponibilidad", "Sí" if resumen.get("disponible") else "No")


def _status_chart(resumen: dict[str, Any]) -> None:
    st.subheader("Distribución por status")
    por_status = resumen.get("por_status", {})
    if not por_status:
        st.info("No hay datos de status para mostrar.")
        return
    st.bar_chart(por_status)


def _kind_chart(resumen: dict[str, Any]) -> None:
    st.subheader("Distribución por tipo de scraper")
    por_kind = resumen.get("por_kind", {})
    if not por_kind:
        st.info("No hay datos de kind para mostrar.")
        return
    st.bar_chart(por_kind)


def _render_fuentes_table(fuentes: list[dict[str, Any]]) -> None:
    st.subheader("Fuentes (muestra)")
    if not fuentes:
        st.warning("No se recibieron fuentes para mostrar.")
        return

    compact_rows = []
    for row in fuentes:
        compact_rows.append(
            {
                "id": row.get("id"),
                "nombre": row.get("nombre"),
                "status": row.get("status"),
                "kind": row.get("kind"),
                "extractor": row.get("recommended_extractor"),
                "perfil": row.get("profile_name"),
                "url_empleo": row.get("url_empleo"),
            }
        )

    st.dataframe(compact_rows, use_container_width=True, hide_index=True)


def _render_alertas_basicas(fuentes: list[dict[str, Any]]) -> None:
    st.subheader("Alertas rápidas")
    if not fuentes:
        st.info("Sin fuentes para evaluar alertas.")
        return

    status_counter = Counter(str(f.get("status", "desconocido")) for f in fuentes)
    blocked = status_counter.get("blocked", 0)
    broken = status_counter.get("broken", 0)
    manual = status_counter.get("manual_review", 0)

    if blocked:
        st.error(f"Fuentes bloqueadas: {blocked}")
    if broken:
        st.error(f"Fuentes rotas: {broken}")
    if manual:
        st.warning(f"Fuentes en revisión manual: {manual}")

    if not (blocked or broken or manual):
        st.success("No hay alertas críticas en la muestra consultada.")


def main() -> None:
    st.set_page_config(page_title="Scraper QA Dashboard", layout="wide")
    st.title("Dashboard QA — Scrapers EmpleoEstado")
    st.caption("Monitoreo rápido de cobertura y salud operacional del scraper.")

    with st.sidebar:
        st.header("Configuración")
        api_base = st.text_input("API base URL", value=DEFAULT_API_BASE)
        limit = st.slider("Cantidad máxima de fuentes", min_value=50, max_value=1000, value=200, step=50)
        only_status = st.text_input("Filtrar status (opcional)", value="")
        only_kind = st.text_input("Filtrar kind (opcional)", value="")
        refresh = st.button("Actualizar")

    st.caption(f"Última actualización local: {_fmt_now()}")

    resumen_url = f"{api_base.rstrip('/')}/api/scraper/resumen"
    fuentes_url = f"{api_base.rstrip('/')}/api/scraper/fuentes?limit={limit}"
    if only_status.strip():
        fuentes_url += f"&status={only_status.strip()}"
    if only_kind.strip():
        fuentes_url += f"&kind={only_kind.strip()}"

    if refresh or "resumen_cache" not in st.session_state:
        st.session_state["resumen_cache"] = _safe_get_json(resumen_url)
        st.session_state["fuentes_cache"] = _safe_get_json(fuentes_url)

    resumen = st.session_state.get("resumen_cache", {})
    payload_fuentes = st.session_state.get("fuentes_cache", {})
    fuentes = payload_fuentes.get("fuentes", []) if isinstance(payload_fuentes, dict) else []

    _cards_resumen(resumen)
    col_left, col_right = st.columns(2)
    with col_left:
        _status_chart(resumen)
    with col_right:
        _kind_chart(resumen)

    _render_alertas_basicas(fuentes)
    _render_fuentes_table(fuentes)


if __name__ == "__main__":
    main()
