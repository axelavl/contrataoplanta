#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Actualizar código, migrar la DB, reiniciar servicios y validar
# Ejecutar como root (necesita systemctl):
#   bash deploy/deploy.sh                    # deploy estándar (no corre scrapers)
#   bash deploy/deploy.sh --run-scrapers     # tras el deploy, dispara una corrida
#   bash deploy/deploy.sh --skip-smoke       # omite el smoke test de endpoints
#
# Pasos: git pull → pip install → alembic upgrade head → daemon-reload →
#        restart API → healthcheck → smoke test (/health, /api/ofertas, SSR de
#        /oferta/{id} y 404 amable) → verificación del timer de scrapers.
# =============================================================================
set -euo pipefail

APP_DIR="/opt/contrataoplanta"
APP_USER="contrata"
API_BASE="http://127.0.0.1:8000"   # uvicorn local (ver contrataoplanta-api.service)
SCRAPERS_UNIT="contrataoplanta-scrapers.service"
SCRAPERS_TIMER="contrataoplanta-scrapers.timer"

RUN_SCRAPERS=0
SKIP_SMOKE=0
for arg in "$@"; do
    case "$arg" in
        --run-scrapers) RUN_SCRAPERS=1 ;;
        --skip-smoke)   SKIP_SMOKE=1 ;;
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

# Migraciones de base de datos. `alembic upgrade head` es el paso de deploy del
# schema (ver docs/MIGRATIONS.md): idempotente (no-op si ya está al día). Se
# corre ANTES de reiniciar la API para que el código nuevo no arranque contra un
# schema desfasado. alembic/env.py lee la DB de db.config → hay que cargar .env.
info "Aplicando migraciones (alembic upgrade head)..."
if sudo -u "$APP_USER" bash -c "set -a; . '$APP_DIR/.env'; set +a; cd '$APP_DIR' && '$APP_DIR/venv/bin/alembic' upgrade head"; then
    info "Schema al día."
else
    err "Falló 'alembic upgrade head'. Aborto para no arrancar con schema desfasado."
    err "Inspecciona: cd $APP_DIR && sudo -u $APP_USER venv/bin/alembic current"
    exit 1
fi

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

# ── Smoke test de endpoints clave ────────────────────────────────────────────
# Valida que la API responde de verdad tras el deploy. Útil sobre todo cuando
# cambian dependencias mayores (p. ej. fastapi/starlette) o el schema.
#   - /health, /api/ofertas → CRÍTICOS: si fallan, el deploy se considera roto.
#   - SSR /oferta/{id} y /oferta inexistente → enlaces compartibles; sólo avisan.
smoke() {  # smoke <nombre> <url> <códigos-esperados-separados-por-espacio>
    local nombre="$1" url="$2" esperados="$3" code
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 12 "$url" || echo "000")
    if [[ " $esperados " == *" $code "* ]]; then
        info "smoke ${nombre} → HTTP ${code}"
        return 0
    fi
    warn "smoke ${nombre} → HTTP ${code} (esperaba: ${esperados})"
    return 1
}

if [[ "$SKIP_SMOKE" == "1" ]]; then
    warn "Smoke test omitido (--skip-smoke)."
else
    info "Smoke test de endpoints..."
    SMOKE_CRIT_FAIL=0
    smoke "health"      "$API_BASE/health"                          "200" || SMOKE_CRIT_FAIL=1
    smoke "api/ofertas" "$API_BASE/api/ofertas?pagina=1&por_pagina=1" "200" || SMOKE_CRIT_FAIL=1

    # SSR de una oferta real (valida el deep-link compartible + el SSR tras el
    # salto de fastapi). Tomamos el primer id del listado. /oferta/{id} responde
    # 301 al slug canónico (o 200 si ya lo es).
    PRIMER_ID=$(curl -s --max-time 12 "$API_BASE/api/ofertas?pagina=1&por_pagina=1" \
        | grep -oE '"id"[: ]*[0-9]+' | head -1 | grep -oE '[0-9]+' || true)
    if [[ -n "${PRIMER_ID:-}" ]]; then
        smoke "oferta/${PRIMER_ID} (SSR)" "$API_BASE/oferta/${PRIMER_ID}" "200 301" || true
    else
        warn "smoke oferta: no pude extraer un id del listado (¿sin ofertas activas?)."
    fi
    # Oferta inexistente → 404 amable (HTML del sitio, no JSON crudo).
    smoke "oferta inexistente" "$API_BASE/oferta/999999999" "404" || true

    if [[ "$SMOKE_CRIT_FAIL" == "1" ]]; then
        err "Smoke test CRÍTICO falló (/health o /api/ofertas). La API arrancó pero no responde bien."
        err "Logs: journalctl -u contrataoplanta-api -n 80 --no-pager"
        exit 1
    fi
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
