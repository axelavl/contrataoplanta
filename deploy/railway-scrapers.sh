#!/usr/bin/env bash
# =============================================================================
# railway-scrapers.sh — comando del servicio/cron de SCRAPERS en Railway.
#
# Cómo usarlo:
#   En el servicio de scrapers de Railway (el que corre por cron), poner como
#   "Custom Start Command":
#       bash deploy/railway-scrapers.sh
#
#   Así queda versionado en el repo en vez de pegado en el dashboard. Equivale
#   a lo que en un VPS hace la unit systemd contrataoplanta-scrapers.service.
#
# Qué hace:
#   1. Una corrida de scrapers en modo producción CON OCR de Carabineros
#      (--ocr; requiere el binario tesseract, que instala nixpacks.toml).
#      Al final run_all.py revalida url_oferta / url_bases y aplica la
#      retención de datos (db/mantenimiento.py).
#
# Requisitos de entorno (los inyecta Railway): DATABASE_URL (o DB_*). No
# necesita ADMIN_PASSWORD.
# =============================================================================
set -euo pipefail

# Hora de Chile para el proceso: muchos scrapers usan date.today() como
# fecha de publicación y el contenedor corre en UTC (de 21:00 a 24:00 de
# Chile daba "mañana"). db/sanear.py también lo corrige al guardar.
export TZ="America/Santiago"

echo "[railway-scrapers] Iniciando corrida de scrapers (--ocr)…"
python scrapers/run_all.py --mode production --ocr

# La revalidación de URLs (validate_offer_urls) y la retención de datos
# (db/mantenimiento.py) ya las hace run_all.py al final de la corrida; antes
# este script volvía a validar todas las URLs (trabajo y tráfico duplicados).

echo "[railway-scrapers] Listo."
