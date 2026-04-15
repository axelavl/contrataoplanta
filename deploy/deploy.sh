#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Actualizar código y reiniciar servicios
# Ejecutar como root o como el usuario contrata:
#   bash deploy/deploy.sh
# =============================================================================
set -euo pipefail

APP_DIR="/opt/contrataoplanta"
APP_USER="contrata"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }

info "Actualizando código..."
sudo -u "$APP_USER" git -C "$APP_DIR" pull --ff-only

info "Actualizando dependencias Python..."
sudo -u "$APP_USER" "$APP_DIR/venv/bin/pip" install --quiet -r "$APP_DIR/requirements.txt"

info "Reiniciando API..."
systemctl restart contrataoplanta-api
sleep 2

if systemctl is-active --quiet contrataoplanta-api; then
    info "API corriendo correctamente."
else
    warn "La API no arrancó. Revisa: journalctl -u contrataoplanta-api -n 50"
    exit 1
fi

info "Deploy completado."
