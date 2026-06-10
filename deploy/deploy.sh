#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Actualizar código y reiniciar servicios
# Ejecutar como root o como el usuario contrata:
#   bash deploy/deploy.sh                    # deploy estándar (no corre scrapers)
#   bash deploy/deploy.sh --run-scrapers     # tras el deploy, dispara una corrida
# =============================================================================
set -euo pipefail

APP_DIR="/opt/contrataoplanta"
APP_USER="contrata"
SCRAPERS_UNIT="contrataoplanta-scrapers.service"
SCRAPERS_TIMER="contrataoplanta-scrapers.timer"

RUN_SCRAPERS=0
for arg in "$@"; do
    case "$arg" in
        --run-scrapers) RUN_SCRAPERS=1 ;;
        *) echo "Argumento desconocido: $arg" >&2; exit 2 ;;
    esac
done

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info() { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err()  { echo -e "${RED}[✗]${NC} $1" >&2; }

info "Actualizando código..."
sudo -u "$APP_USER" git -C "$APP_DIR" pull --ff-only

info "Actualizando dependencias Python..."
sudo -u "$APP_USER" "$APP_DIR/venv/bin/pip" install --quiet -r "$APP_DIR/requirements.txt"

# Recargar systemd: si los archivos .service/.timer en deploy/systemd/ cambiaron,
# este paso garantiza que la próxima ejecución use la unit nueva.
info "Recargando systemd..."
systemctl daemon-reload

info "Reiniciando API..."
systemctl restart contrataoplanta-api
sleep 2

if systemctl is-active --quiet contrataoplanta-api; then
    info "API corriendo correctamente."
else
    warn "La API no arrancó. Revisa: journalctl -u contrataoplanta-api -n 50"
    exit 1
fi

# Validación del timer de scrapers: la unit oneshot levanta cada 12h y
# fácilmente queda desactivada sin que nadie se entere (el síntoma final
# son ofertas que no se actualizan por días). Verificar explícitamente.
info "Verificando timer de scrapers..."
if ! systemctl is-enabled --quiet "$SCRAPERS_TIMER"; then
    warn "$SCRAPERS_TIMER NO está habilitado. Habilitando..."
    systemctl enable "$SCRAPERS_TIMER"
fi

if ! systemctl is-active --quiet "$SCRAPERS_TIMER"; then
    warn "$SCRAPERS_TIMER NO está activo. Iniciando..."
    systemctl start "$SCRAPERS_TIMER"
fi

# Reportar próxima ejecución y última invocación de la unit, para detectar
# si el scraper viene fallando crónicamente.
NEXT_RUN=$(systemctl show -p NextElapseUSecRealtime --value "$SCRAPERS_TIMER" || echo "desconocida")
LAST_RUN=$(systemctl show -p LastTriggerUSec --value "$SCRAPERS_TIMER" || echo "desconocida")
info "Timer scrapers: próxima=${NEXT_RUN:-desconocida} | última=${LAST_RUN:-desconocida}"

# Si la última corrida del scraper terminó en error, advertir.
LAST_RESULT=$(systemctl show -p Result --value "$SCRAPERS_UNIT" 2>/dev/null || echo "")
if [[ -n "$LAST_RESULT" && "$LAST_RESULT" != "success" ]]; then
    warn "Última corrida de $SCRAPERS_UNIT terminó con result=$LAST_RESULT"
    warn "Ver: journalctl -u $SCRAPERS_UNIT -n 100 --no-pager"
fi

if [[ "$RUN_SCRAPERS" == "1" ]]; then
    info "Disparando corrida manual de scrapers (--run-scrapers)..."
    # `start` con oneshot bloquea hasta que termina; usamos --no-block para
    # devolver el control al deploy y que el operador siga vía journalctl.
    systemctl start --no-block "$SCRAPERS_UNIT"
    info "Corrida en curso. Seguir con: journalctl -u $SCRAPERS_UNIT -f"
fi

info "Deploy completado."
