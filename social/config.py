"""Configuración central de la automatización de RRSS.

Un solo lugar para tocar: cadencia, hashtags, plantillas de copy, UTM y
rutas de la cola. El resto de los módulos (`copy_writer`, `generar_cola`)
importan desde aquí. Mantener en español (convención del repo).
"""
from __future__ import annotations

import os
from pathlib import Path

# ── Identidad / destino ──────────────────────────────────────────────────
SITE_URL = os.getenv("SITE_URL", "https://contrataoplanta.cl").rstrip("/")
MARCA = "contrataoplanta"

# ── Rutas de la cola ─────────────────────────────────────────────────────
# La cola es el "buzón de salida": cada corrida deja aquí las imágenes + el
# manifest que la UI de revisión (revisar.html) lee para aprobar/rechazar.
ROOT = Path(__file__).resolve().parent
COLA_DIR = Path(os.getenv("RRSS_COLA_DIR", str(ROOT / "cola")))

# ── Selección de avisos ──────────────────────────────────────────────────
# Cuántos destacados como máximo por corrida y umbral de "cierra pronto".
MAX_POR_CORRIDA = int(os.getenv("RRSS_MAX", "8"))
# Solo avisos cuyo cierre esté dentro de esta ventana (días). None = sin tope.
VENTANA_CIERRE_DIAS = int(os.getenv("RRSS_VENTANA_DIAS", "21"))

# ── Plataformas activas ──────────────────────────────────────────────────
# Cada plataforma define el formato de imagen que consume render_offer_card.
PLATAFORMAS = {
    "instagram": {"formato": "square", "etiqueta": "Instagram"},
    "linkedin": {"formato": "horizontal", "etiqueta": "LinkedIn"},
}

# ── UTM (para medir clics reales desde RRSS en Umami/Analytics) ───────────
UTM_CAMPAIGN = os.getenv("RRSS_UTM_CAMPAIGN", "destacados")
UTM_MEDIUM = "social"

# ── Hashtags por plataforma ──────────────────────────────────────────────
# IG tolera muchos; LinkedIn rinde mejor con 3–5 muy específicos.
HASHTAGS_BASE = ["#EmpleoPúblico", "#Chile", "#SectorPúblico", "#ConcursosPúblicos"]
HASHTAGS_IG_EXTRA = ["#TrabajoEstatal", "#EmpleosChile", "#Postula", "#Vacantes"]
HASHTAGS_LINKEDIN_EXTRA = ["#EmpleoEstatal", "#Reclutamiento"]

# Mapa región → hashtag local (suma alcance geolocalizado). Se agrega solo
# si la oferta trae región reconocible.
HASHTAG_REGION = {
    "metropolitana": "#Santiago",
    "valparaíso": "#Valparaíso",
    "valparaiso": "#Valparaíso",
    "biobío": "#Concepción",
    "biobio": "#Concepción",
    "araucanía": "#Temuco",
    "araucania": "#Temuco",
    "los lagos": "#PuertoMontt",
    "antofagasta": "#Antofagasta",
    "coquimbo": "#LaSerena",
    "maule": "#Talca",
    "ohiggins": "#Rancagua",
    "o'higgins": "#Rancagua",
}

# ── Emojis (configurables: ponlos en "" para desactivar) ─────────────────
EMOJI = {
    "cargo": "🔹",
    "institucion": "🏛️",
    "ubicacion": "📍",
    "renta": "💰",
    "cierre": "⏳",
    "cta": "👉",
    "alerta": "🚨",
}
