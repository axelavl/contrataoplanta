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
#   2. Revalida url_oferta / url_bases de las filas recientes para que el
#      frontend habilite "Ver bases" / "Postular" (run_all.py no lo hace solo).
#
# Requisitos de entorno (los inyecta Railway): DATABASE_URL (o DB_*). No
# necesita ADMIN_PASSWORD.
# =============================================================================
set -euo pipefail

echo "[railway-scrapers] Iniciando corrida de scrapers (--ocr)…"
python scrapers/run_all.py --mode production --ocr

# La revalidación es best-effort: una URL externa intermitente no debe marcar
# como fallida toda la corrida (igual criterio que el ExecStartPost del VPS).
echo "[railway-scrapers] Revalidando URLs de ofertas recientes…"
python validate_offer_urls.py --workers 20 --max-edad-h 24 \
    || echo "[railway-scrapers] validate_offer_urls terminó con error (no crítico)"

echo "[railway-scrapers] Listo."
