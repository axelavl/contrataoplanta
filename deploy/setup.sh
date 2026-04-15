#!/usr/bin/env bash
# =============================================================================
# setup.sh — Configuración inicial del servidor (Ubuntu 22.04 LTS)
# Ejecutar como root: bash deploy/setup.sh
# =============================================================================
set -euo pipefail

# ── CONFIGURACIÓN ─────────────────────────────────────────────────────────────
APP_USER="contrata"
APP_DIR="/opt/contrataoplanta"
REPO_URL="https://github.com/TU_USUARIO/contrataoplanta.git"   # <── CAMBIAR
DOMAIN="contrataoplanta.cl"
API_DOMAIN="api.contrataoplanta.cl"
DB_NAME="empleospublicos"
DB_USER="contrata"
DB_PASSWORD=""   # Se pedirá interactivamente si está vacío
# ─────────────────────────────────────────────────────────────────────────────

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[✓]${NC} $1"; }
warn()  { echo -e "${YELLOW}[!]${NC} $1"; }
error() { echo -e "${RED}[✗]${NC} $1"; exit 1; }

[[ $EUID -ne 0 ]] && error "Ejecutar como root (sudo bash setup.sh)"

# Pedir contraseña de DB si no está configurada
if [[ -z "$DB_PASSWORD" ]]; then
    read -rsp "Contraseña para la base de datos PostgreSQL: " DB_PASSWORD
    echo
    [[ -z "$DB_PASSWORD" ]] && error "La contraseña no puede estar vacía"
fi

# ── 1. SISTEMA ─────────────────────────────────────────────────────────────────
info "Actualizando sistema..."
apt-get update -qq && apt-get upgrade -y -qq

info "Instalando dependencias del sistema..."
apt-get install -y -qq \
    git curl wget gnupg2 software-properties-common \
    nginx postgresql postgresql-contrib \
    python3.12 python3.12-venv python3.12-dev python3-pip \
    certbot python3-certbot-nginx \
    ufw fail2ban

# ── 2. USUARIO ─────────────────────────────────────────────────────────────────
info "Creando usuario de aplicación: $APP_USER"
if ! id "$APP_USER" &>/dev/null; then
    useradd --system --shell /bin/bash --home-dir "$APP_DIR" --create-home "$APP_USER"
fi

# ── 3. POSTGRESQL ──────────────────────────────────────────────────────────────
info "Configurando PostgreSQL..."
systemctl enable postgresql --now

sudo -u postgres psql -c "CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD';" 2>/dev/null || \
    sudo -u postgres psql -c "ALTER USER $DB_USER WITH PASSWORD '$DB_PASSWORD';"

sudo -u postgres psql -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;" 2>/dev/null || \
    warn "Base de datos $DB_NAME ya existe"

sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;"

# ── 4. CÓDIGO ──────────────────────────────────────────────────────────────────
info "Clonando repositorio..."
if [[ -d "$APP_DIR/.git" ]]; then
    warn "Repositorio ya existe, haciendo pull..."
    sudo -u "$APP_USER" git -C "$APP_DIR" pull
else
    sudo -u "$APP_USER" git clone "$REPO_URL" "$APP_DIR"
fi

# ── 5. ENTORNO PYTHON ──────────────────────────────────────────────────────────
info "Creando entorno virtual Python..."
sudo -u "$APP_USER" python3.12 -m venv "$APP_DIR/venv"

info "Instalando dependencias Python..."
sudo -u "$APP_USER" "$APP_DIR/venv/bin/pip" install --quiet --upgrade pip
sudo -u "$APP_USER" "$APP_DIR/venv/bin/pip" install --quiet -r "$APP_DIR/requirements.txt"

# ── 6. VARIABLES DE ENTORNO ───────────────────────────────────────────────────
info "Configurando variables de entorno..."
if [[ ! -f "$APP_DIR/.env" ]]; then
    cp "$APP_DIR/.env.example" "$APP_DIR/.env"
    # Preconfigura los valores de DB
    sed -i "s/^DB_HOST=.*/DB_HOST=localhost/"     "$APP_DIR/.env"
    sed -i "s/^DB_PORT=.*/DB_PORT=5432/"          "$APP_DIR/.env"
    sed -i "s/^DB_NAME=.*/DB_NAME=$DB_NAME/"      "$APP_DIR/.env"
    sed -i "s/^DB_USER=.*/DB_USER=$DB_USER/"      "$APP_DIR/.env"
    sed -i "s/^DB_PASSWORD=.*/DB_PASSWORD=$DB_PASSWORD/" "$APP_DIR/.env"
    chmod 600 "$APP_DIR/.env"
    chown "$APP_USER:$APP_USER" "$APP_DIR/.env"
    warn ".env creado. Revisa $APP_DIR/.env para completar el resto de variables."
fi

# ── 7. SYSTEMD ─────────────────────────────────────────────────────────────────
info "Instalando servicios systemd..."
cp "$APP_DIR/deploy/systemd/"*.service /etc/systemd/system/
cp "$APP_DIR/deploy/systemd/"*.timer   /etc/systemd/system/

# Reemplaza el usuario en los archivos de servicio
sed -i "s|APP_DIR|$APP_DIR|g"   /etc/systemd/system/contrataoplanta-*.service
sed -i "s|APP_USER|$APP_USER|g" /etc/systemd/system/contrataoplanta-*.service

systemctl daemon-reload
systemctl enable --now contrataoplanta-api
systemctl enable --now contrataoplanta-scrapers.timer

# ── 8. NGINX ───────────────────────────────────────────────────────────────────
info "Configurando nginx..."
cp "$APP_DIR/deploy/nginx/contrataoplanta.conf" /etc/nginx/sites-available/contrataoplanta

sed -i "s|APP_DIR|$APP_DIR|g"         /etc/nginx/sites-available/contrataoplanta
sed -i "s|DOMAIN|$DOMAIN|g"           /etc/nginx/sites-available/contrataoplanta
sed -i "s|API_DOMAIN|$API_DOMAIN|g"   /etc/nginx/sites-available/contrataoplanta

ln -sf /etc/nginx/sites-available/contrataoplanta /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl enable --now nginx

# ── 9. FIREWALL ────────────────────────────────────────────────────────────────
info "Configurando firewall (ufw)..."
ufw allow OpenSSH
ufw allow 'Nginx Full'
ufw --force enable

# ── 10. SSL ────────────────────────────────────────────────────────────────────
info "Para activar SSL (HTTPS), ejecuta:"
echo "  certbot --nginx -d $DOMAIN -d www.$DOMAIN -d $API_DOMAIN"
echo ""

info "═══ Setup completado ═══"
echo ""
echo "  App:  http://$DOMAIN"
echo "  API:  http://$API_DOMAIN"
echo "  Logs: journalctl -u contrataoplanta-api -f"
echo "        journalctl -u contrataoplanta-scrapers -f"
echo ""
warn "Recuerda apuntar los DNS de $DOMAIN y $API_DOMAIN a la IP de este servidor."
