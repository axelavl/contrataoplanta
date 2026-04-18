"""Generador de imágenes OG / Twitter / redes sociales para ofertas.

Renderiza dos variantes server-side (Pillow-only):

- ``horizontal`` (1200x630): Open Graph / Twitter / LinkedIn / WhatsApp.
- ``square``     (1080x1080): Instagram (stories/feed), mensajería cuadrada.

Elegimos Pillow (no @vercel/og / Satori / Playwright) porque:

- Ya está desplegado y probado en Railway (requirements.txt).
- No requiere un runtime de Node ni Chromium embebido (~300MB extra).
- El layout es suficientemente simple para primitivas de dibujo, sin el costo
  de arrancar un navegador por cada request.

Jerarquía visual (prioridad de arriba a abajo, siempre visible aunque falten
campos secundarios):

1. **Cargo** (titular, hasta 3 líneas, peso bold máximo).
2. **Institución** (subtítulo, hasta 2 líneas).
3. **Cierre** o alerta *Cierra pronto* cuando ``dias_restantes <= 3`` o el
   estado es ``closing_today``.
4. Pills de contexto (región/comuna, tipo contractual, remuneración cuando
   existe).
5. CTA: "Postula en estadoemplea.pages.dev".

Fallbacks:

- Sin logo institucional → se dibuja un disco navy con la sigla (o las dos
  iniciales del nombre) en dorado.
- Sin región/tipo/renta → la pill simplemente no se dibuja; el layout se
  reacomoda.
- Sin fecha de cierre → se muestra el estado textual si lo hay, y si no se
  omite la fila completa.
- Si Pillow falla cargando una fuente TrueType se cae al default bitmap
  (ilegible pero nunca rompe el endpoint).

Sin llamadas de red en la ruta crítica por defecto. El logo remoto
(`logo.clearbit.com`) sólo se intenta si ``OG_FETCH_LOGOS`` está habilitado
(default: on) y con timeout corto; el resultado queda en un LRU en proceso.
Cualquier error de red cae silenciosamente al fallback de iniciales.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from typing import Any, Literal

from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

Format = Literal["horizontal", "square"]

# ── Paleta (idéntica a favicon.svg / frontend) ───────────────────────────
NAVY = (10, 46, 110)
NAVY_DEEP = (7, 32, 78)
NAVY_SOFT = (20, 60, 135)
GOLD = (232, 168, 32)
GOLD_SOFT = (245, 196, 85)
WHITE = (250, 250, 248)
WHITE_DIM = (220, 222, 230)
TEXT_DIM = (180, 190, 210)
ALERT = (228, 79, 79)
ALERT_SOFT = (255, 110, 110)

# ── Dimensiones por variante ─────────────────────────────────────────────
_SPEC: dict[Format, dict[str, int]] = {
    "horizontal": {"w": 1200, "h": 630, "pad": 64, "title": 72, "sub": 38},
    "square":     {"w": 1080, "h": 1080, "pad": 80, "title": 78, "sub": 42},
}

_LOGO_CACHE_ENABLED = os.getenv("OG_FETCH_LOGOS", "1").strip().lower() not in {"0", "false", "no"}
_LOGO_TIMEOUT_S = float(os.getenv("OG_LOGO_TIMEOUT_S", "1.5") or 1.5)


# ── Fonts ────────────────────────────────────────────────────────────────
@lru_cache(maxsize=1)
def _font_paths() -> dict[str, list[Path]]:
    """Busca TTF sans-serif proporcionales. Prioriza Inter > DejaVu Sans.

    Excluimos variantes mono (`*Mono*`, `*Condensed*`) porque rinden mal como
    titular y destruyen la proporción del layout. Si nada existe, Pillow cae
    al bitmap default y el render sigue funcionando (más feo, pero vivo).
    """
    candidates = [
        Path("/usr/share/fonts/truetype/inter"),
        Path("/usr/share/fonts/truetype/dejavu"),
        Path("/usr/share/fonts"),
    ]
    found: dict[str, list[Path]] = {"bold": [], "regular": []}
    for base in candidates:
        if not base.exists():
            continue
        for p in base.rglob("*.ttf"):
            name = p.name.lower()
            # Saltamos familias no proporcionales o condensadas.
            if "mono" in name or "condensed" in name or "oblique" in name:
                continue
            if "bold" in name and "semibold" not in name and "extrabold" not in name:
                found["bold"].append(p)
            elif "regular" in name or "book" in name:
                found["regular"].append(p)
    # Orden estable: Inter primero, luego DejaVu Sans (no Mono).
    def _rank(path: Path) -> tuple[int, str]:
        name = path.name.lower()
        if "inter" in name:
            return (0, name)
        if "dejavusans" in name.replace("-", ""):
            return (1, name)
        return (2, name)
    found["bold"].sort(key=_rank)
    found["regular"].sort(key=_rank)
    return found


_FONT_CACHE: dict[tuple[str, int], ImageFont.FreeTypeFont] = {}


def _load(weight: str, size: int) -> ImageFont.FreeTypeFont:
    key = (weight, size)
    cached = _FONT_CACHE.get(key)
    if cached is not None:
        return cached
    paths = _font_paths().get(weight) or _font_paths().get("regular") or []
    font: ImageFont.FreeTypeFont
    for path in paths:
        try:
            font = ImageFont.truetype(str(path), size)
            break
        except Exception:  # pragma: no cover — font listado pero corrupto
            continue
    else:
        font = ImageFont.load_default()
    _FONT_CACHE[key] = font
    return font


# ── Helpers de texto ─────────────────────────────────────────────────────
def _wrap(text: str, font: ImageFont.FreeTypeFont, max_w: int, draw: ImageDraw.ImageDraw) -> list[str]:
    words = (text or "").split()
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = (current + " " + word).strip()
        if draw.textlength(candidate, font=font) <= max_w or not current:
            current = candidate
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines


def _truncate_lines(lines: list[str], max_lines: int) -> list[str]:
    if len(lines) <= max_lines:
        return lines
    out = lines[:max_lines]
    tail = out[-1].rstrip()
    out[-1] = (tail[:78] + "…") if len(tail) > 78 else (tail + "…")
    return out


def _sigla_fallback(nombre: str | None, sigla: str | None) -> str:
    if sigla and sigla.strip():
        s = sigla.strip()
        return s[:3].upper()
    tokens = [t for t in (nombre or "").split() if t and t[0].isalpha()]
    # Ignora stopwords frecuentes en nombres de organismos chilenos.
    skip = {"de", "del", "la", "las", "los", "y", "el", "en"}
    letras = [t[0].upper() for t in tokens if t.lower() not in skip]
    if not letras:
        return "CL"
    return ("".join(letras[:2]) or "CL")[:3]


# ── Logo institucional ───────────────────────────────────────────────────
def _fetch_logo(domain: str) -> Image.Image | None:
    """Descarga el logo desde Clearbit. Silencioso ante cualquier fallo."""
    try:
        import requests

        r = requests.get(
            f"https://logo.clearbit.com/{domain}",
            params={"size": "256"},
            timeout=_LOGO_TIMEOUT_S,
        )
        if r.status_code != 200 or not r.content:
            return None
        logo = Image.open(BytesIO(r.content)).convert("RGBA")
        return logo
    except Exception as exc:  # requests, Pillow o red — todos terminan igual
        logger.debug("OG logo fetch falló para %s: %s", domain, exc)
        return None


@lru_cache(maxsize=512)
def _cached_logo(domain: str) -> bytes | None:
    if not _LOGO_CACHE_ENABLED:
        return None
    img = _fetch_logo(domain)
    if img is None:
        return None
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _load_institution_logo(domain: str | None) -> Image.Image | None:
    if not domain:
        return None
    data = _cached_logo(domain)
    if not data:
        return None
    try:
        return Image.open(BytesIO(data)).convert("RGBA")
    except Exception:
        return None


# ── Estado cierre ────────────────────────────────────────────────────────
def _cierra_pronto(oferta: dict[str, Any]) -> bool:
    estado = str(oferta.get("estado") or oferta.get("estado_normalizado") or "").lower()
    if estado == "closing_today":
        return True
    dias = oferta.get("dias_restantes")
    if isinstance(dias, int) and 0 <= dias <= 3:
        return True
    return False


def _format_cierre(oferta: dict[str, Any]) -> tuple[str, bool]:
    """Devuelve (texto, es_alerta). Si no hay info de cierre devuelve ("", False)."""
    dias = oferta.get("dias_restantes")
    estado = str(oferta.get("estado") or "").lower()
    if estado == "closing_today":
        return ("Cierra HOY", True)
    if isinstance(dias, int):
        if dias < 0:
            return ("", False)
        if dias == 0:
            return ("Cierra HOY", True)
        if dias == 1:
            return ("Cierra mañana", True)
        if dias <= 3:
            return (f"Cierra en {dias} días — postula ya", True)
        if dias <= 14:
            return (f"Quedan {dias} días para postular", False)
        return (f"{dias} días para postular", False)
    fecha = oferta.get("fecha_cierre")
    if isinstance(fecha, date):
        meses = (
            "ene", "feb", "mar", "abr", "may", "jun",
            "jul", "ago", "sep", "oct", "nov", "dic",
        )
        return (f"Cierra {fecha.day} {meses[fecha.month - 1]} {fecha.year}", False)
    return ("", False)


def _format_renta(oferta: dict[str, Any]) -> str | None:
    rmin = oferta.get("renta_bruta_min")
    rmax = oferta.get("renta_bruta_max")

    def _cl(value: int) -> str:
        return "$" + f"{value:,.0f}".replace(",", ".")

    if isinstance(rmin, int) and isinstance(rmax, int) and rmin > 0 and rmax > 0:
        if rmin == rmax:
            return _cl(rmin)
        return f"{_cl(rmin)} – {_cl(rmax)}"
    if isinstance(rmax, int) and rmax > 0:
        return f"Hasta {_cl(rmax)}"
    if isinstance(rmin, int) and rmin > 0:
        return f"Desde {_cl(rmin)}"
    return None


# ── Primitivas de dibujo ─────────────────────────────────────────────────
def _gradient_background(draw: ImageDraw.ImageDraw, w: int, h: int) -> None:
    # Gradiente vertical navy → navy_deep (cheap, una línea por y).
    for y in range(h):
        t = y / max(1, h - 1)
        r = int(NAVY[0] * (1 - t) + NAVY_DEEP[0] * t)
        g = int(NAVY[1] * (1 - t) + NAVY_DEEP[1] * t)
        b = int(NAVY[2] * (1 - t) + NAVY_DEEP[2] * t)
        draw.line([(0, y), (w, y)], fill=(r, g, b))


def _draw_logo_mark(img: Image.Image, domain: str | None, sigla: str, center: tuple[int, int], radius: int) -> None:
    """Dibuja un logo redondo. Usa Clearbit si hay dominio, si no una disc+sigla."""
    cx, cy = center
    draw = ImageDraw.Draw(img)
    # Anillo dorado siempre como marco: refuerza branding y esconde bordes feos.
    draw.ellipse([cx - radius - 6, cy - radius - 6, cx + radius + 6, cy + radius + 6], fill=GOLD)
    draw.ellipse([cx - radius, cy - radius, cx + radius, cy + radius], fill=WHITE)

    logo = _load_institution_logo(domain)
    if logo is not None:
        # Ajusta el logo dentro del círculo y aplica máscara circular.
        size = radius * 2 - 12
        logo = logo.resize((size, size), Image.LANCZOS)
        mask = Image.new("L", (size, size), 0)
        ImageDraw.Draw(mask).ellipse([0, 0, size, size], fill=255)
        img.paste(logo, (cx - size // 2, cy - size // 2), mask)
        return

    # Fallback: sigla centrada en dorado sobre el disco blanco.
    text = sigla
    size = int(radius * 0.95) if len(text) <= 2 else int(radius * 0.72)
    f = _load("bold", max(24, size))
    tw = draw.textlength(text, font=f)
    # ascent/descent aproximados para centrar verticalmente.
    th = f.size
    draw.text(
        (cx - tw / 2, cy - th / 2 - th * 0.1),
        text,
        font=f,
        fill=NAVY,
    )


def _draw_pill(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    text: str,
    font: ImageFont.FreeTypeFont,
    icon: str,
    *,
    outline: tuple[int, int, int] = GOLD,
    fg: tuple[int, int, int] = WHITE,
    pad_x: int = 22,
    height: int = 56,
) -> int:
    """Dibuja una pill con icono vectorial + label. Devuelve el ancho usado."""
    x, y = xy
    label = text
    tw = draw.textlength(label, font=font)
    icon_w = 28
    gap = 10
    inner_w = int(icon_w + gap + tw)
    pill_w = inner_w + pad_x * 2
    draw.rounded_rectangle(
        [x, y, x + pill_w, y + height],
        radius=height // 2,
        outline=outline,
        width=2,
    )
    icon_cx = x + pad_x + icon_w // 2
    icon_cy = y + height // 2
    _draw_icon(draw, icon, (icon_cx, icon_cy), outline)
    # Aproxima centrado vertical del texto (Pillow carece de line-height trivial).
    draw.text((x + pad_x + icon_w + gap, y + height / 2 - font.size * 0.62), label, font=font, fill=fg)
    return pill_w


def _draw_icon(draw: ImageDraw.ImageDraw, kind: str, center: tuple[int, int], color: tuple[int, int, int]) -> None:
    """Iconos vectoriales simples dibujados con Pillow (evitan fuentes emoji)."""
    cx, cy = center
    if kind == "pin":
        # Gota invertida (ubicación).
        draw.pieslice([cx - 10, cy - 14, cx + 10, cy + 6], 180, 360, fill=color)
        draw.polygon([(cx - 10, cy - 4), (cx + 10, cy - 4), (cx, cy + 12)], fill=color)
        draw.ellipse([cx - 4, cy - 8, cx + 4, cy], fill=WHITE)
    elif kind == "briefcase":
        draw.rounded_rectangle([cx - 12, cy - 6, cx + 12, cy + 10], radius=2, fill=color)
        draw.rectangle([cx - 6, cy - 10, cx + 6, cy - 6], fill=color)
        draw.line([cx - 12, cy + 2, cx + 12, cy + 2], fill=WHITE, width=2)
    elif kind == "clock":
        draw.ellipse([cx - 12, cy - 12, cx + 12, cy + 12], outline=color, width=2)
        draw.line([cx, cy, cx, cy - 8], fill=color, width=2)
        draw.line([cx, cy, cx + 6, cy + 2], fill=color, width=2)
    elif kind == "alert":
        draw.polygon([(cx, cy - 14), (cx + 14, cy + 10), (cx - 14, cy + 10)], fill=color)
        draw.line([cx, cy - 6, cx, cy + 4], fill=WHITE, width=3)
        draw.ellipse([cx - 2, cy + 6, cx + 2, cy + 10], fill=WHITE)
    elif kind == "money":
        draw.ellipse([cx - 12, cy - 12, cx + 12, cy + 12], outline=color, width=2)
        f = _load("bold", 18)
        tw = draw.textlength("$", font=f)
        draw.text((cx - tw / 2, cy - 12), "$", font=f, fill=color)


# ── Renderizador principal ───────────────────────────────────────────────
def render_offer_card(oferta: dict[str, Any], fmt: Format = "horizontal") -> bytes:
    """Renderiza la tarjeta OG y devuelve bytes PNG.

    ``fmt``:
      - ``"horizontal"`` → 1200x630 (Open Graph / Twitter / LinkedIn / WhatsApp).
      - ``"square"``     → 1080x1080 (Instagram, mensajería cuadrada).
    """
    spec = _SPEC.get(fmt) or _SPEC["horizontal"]
    W, H, PAD = spec["w"], spec["h"], spec["pad"]
    title_size, sub_size = spec["title"], spec["sub"]

    img = Image.new("RGB", (W, H), NAVY)
    d = ImageDraw.Draw(img)
    _gradient_background(d, W, H)

    # Banda superior de marca (navy_soft) para separar el kicker del titular.
    band_h = int(H * 0.11) if fmt == "horizontal" else int(H * 0.09)
    d.rectangle([0, 0, W, band_h], fill=NAVY_DEEP)

    # Logo institucional / disco con sigla, en esquina superior derecha.
    sigla_text = _sigla_fallback(
        oferta.get("institucion"),
        oferta.get("sigla"),
    )
    logo_domain = oferta.get("institucion_sitio_web")
    # Radio proporcional al ancho de la banda.
    logo_r = int(band_h * 0.55)
    logo_cx = W - PAD - logo_r
    logo_cy = band_h + logo_r - int(band_h * 0.25)
    _draw_logo_mark(img, logo_domain, sigla_text, (logo_cx, logo_cy), logo_r)

    # Kicker de marca.
    f_kicker = _load("bold", 26 if fmt == "horizontal" else 28)
    kicker_text = "ESTADOEMPLEA · EMPLEO PÚBLICO CHILE"
    d.text((PAD, int(band_h / 2 - 16)), kicker_text, font=f_kicker, fill=GOLD)

    # Zona de texto principal (evita pisar el logo/disco).
    content_left = PAD
    content_right = W - PAD
    # Reserva visual para el logo en el área derecha (solo aplica al titular).
    title_right = logo_cx - logo_r - 24
    title_max_w = max(200, title_right - content_left)

    # ── Cargo (titular) ──
    cargo = (oferta.get("cargo") or "Oferta laboral").strip()
    f_cargo = _load("bold", title_size)
    max_title_lines = 3 if fmt == "horizontal" else 4
    cargo_lines = _truncate_lines(_wrap(cargo, f_cargo, title_max_w, d), max_title_lines)
    # Reduce tamaño si el cargo sigue sin caber en max_title_lines a title_size.
    while len(cargo_lines) >= max_title_lines and f_cargo.size > 44:
        smaller = f_cargo.size - 6
        f_cargo = _load("bold", smaller)
        cargo_lines = _truncate_lines(_wrap(cargo, f_cargo, title_max_w, d), max_title_lines)

    title_y0 = band_h + int(PAD * 0.9)
    y = title_y0
    line_h = int(f_cargo.size * 1.14)
    for line in cargo_lines:
        d.text((content_left, y), line, font=f_cargo, fill=WHITE)
        y += line_h

    # ── Institución ──
    institucion = (oferta.get("institucion") or "Institución pública").strip()
    f_inst = _load("regular", sub_size)
    inst_max_w = content_right - content_left
    inst_lines = _truncate_lines(_wrap(institucion, f_inst, inst_max_w, d), 2)
    y += int(sub_size * 0.4)
    for line in inst_lines:
        d.text((content_left, y), line, font=f_inst, fill=WHITE_DIM)
        y += int(sub_size * 1.25)

    # ── Cierre / alerta ──
    cierre_text, alerta = _format_cierre(oferta)
    if cierre_text:
        y += int(sub_size * 0.5)
        pill_h = 56 if fmt == "horizontal" else 62
        f_cierre = _load("bold", 26 if fmt == "horizontal" else 30)
        if alerta:
            # Badge rojo sólido — debe cortar visualmente el layout.
            tw = d.textlength(cierre_text, font=f_cierre)
            icon_w = 28
            gap = 12
            pad_x = 24
            badge_w = int(tw + icon_w + gap + pad_x * 2)
            d.rounded_rectangle(
                [content_left, y, content_left + badge_w, y + pill_h],
                radius=pill_h // 2,
                fill=ALERT,
                outline=ALERT_SOFT,
                width=2,
            )
            icon_cx = content_left + pad_x + icon_w // 2
            _draw_icon(d, "alert", (icon_cx, y + pill_h // 2), WHITE)
            d.text(
                (content_left + pad_x + icon_w + gap, y + pill_h / 2 - f_cierre.size * 0.62),
                cierre_text,
                font=f_cierre,
                fill=WHITE,
            )
            y += pill_h + 16
        else:
            _draw_pill(
                d,
                (content_left, y),
                cierre_text,
                f_cierre,
                icon="clock",
                outline=GOLD,
                fg=WHITE,
                height=pill_h,
            )
            y += pill_h + 16

    # ── Pills secundarias (región · tipo · renta) ──
    f_pill = _load("bold", 24 if fmt == "horizontal" else 28)
    region_text_parts = []
    region = (oferta.get("region") or "").strip()
    ciudad = (oferta.get("ciudad") or "").strip()
    if ciudad and (not region or ciudad.lower() not in region.lower()):
        region_text_parts.append(ciudad)
    if region:
        region_text_parts.append(region)
    region_label = " · ".join(region_text_parts)

    pills: list[tuple[str, str]] = []
    if region_label:
        pills.append(("pin", region_label))
    tipo = (oferta.get("tipo_contrato") or "").strip()
    if tipo:
        pills.append(("briefcase", tipo.title()))
    renta = _format_renta(oferta)
    if renta:
        pills.append(("money", renta))

    # La CTA queda fija en el pie: reserva ese espacio y coloca las pills
    # inmediatamente encima para no chocar con el borde inferior.
    cta_h = 48 if fmt == "horizontal" else 58
    cta_bottom = H - PAD
    cta_top = cta_bottom - cta_h
    pill_h = 52 if fmt == "horizontal" else 58
    pills_y = cta_top - pill_h - 22

    # Si el texto ya invadió esa zona (cargo muy largo + institución),
    # sacrifica las pills; el CTA y el cierre son más importantes.
    if pills and y < pills_y - 8:
        px = content_left
        max_row_w = content_right - content_left
        for icon, label in pills:
            # Trunca labels excesivamente largos para no romper la fila.
            while f_pill.getlength(label) > max_row_w - 100 and len(label) > 20:
                label = label[:-2].rstrip(" ,.-") + "…"
            width = _draw_pill(d, (px, pills_y), label, f_pill, icon=icon, height=pill_h)
            px += width + 12
            if px > content_right - 120:
                break

    # ── CTA inferior ──
    f_cta = _load("bold", 26 if fmt == "horizontal" else 30)
    cta_text = "postula en estadoemplea.pages.dev  →"
    d.text((content_left, cta_top + cta_h / 2 - f_cta.size * 0.62), cta_text, font=f_cta, fill=GOLD)

    # Línea dorada fina como acento de marca.
    d.rectangle([0, H - 6, W, H], fill=GOLD)

    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


__all__ = ["render_offer_card", "Format"]
