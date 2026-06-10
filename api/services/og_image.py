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

# ── Paleta ──────────────────────────────────────────────────────────────
# Basada en favicon.svg + tokens del frontend, con agregados para chips y
# capas (el glow superior, sombras simuladas, bg de cards).
NAVY = (10, 46, 110)          # primario
NAVY_DEEP = (6, 28, 70)       # fondo inferior del gradiente
NAVY_SOFT = (22, 62, 135)     # highlight superior
NAVY_ELEV = (18, 54, 118)     # fondo de info-cards (sutilmente más claro que navy)
NAVY_EDGE = (32, 78, 160)     # borde de info-cards
GOLD = (232, 168, 32)
GOLD_SOFT = (245, 196, 85)
GOLD_DEEP = (178, 124, 18)    # hover/bottom del botón CTA
WHITE = (250, 250, 248)
WHITE_DIM = (220, 222, 230)
TEXT_DIM = (170, 184, 214)    # labels dentro de info-cards
ALERT = (228, 79, 79)
ALERT_SOFT = (255, 110, 110)
ALERT_DEEP = (170, 42, 42)

# ── Dimensiones por variante ────────────────────────────────────────────
# Ajustadas para que el titular convive con las info-cards + CTA en ambos
# formatos sin pisarse. Si el cargo es muy largo el shrink lo baja en vez
# de sumar líneas.
_SPEC: dict[Format, dict[str, int]] = {
    "horizontal": {"w": 1200, "h": 630, "pad": 52, "title": 64, "sub": 28},
    "square":     {"w": 1080, "h": 1080, "pad": 68, "title": 82, "sub": 36},
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
def _gradient_background(img: Image.Image) -> None:
    """Fondo con gradiente vertical + halo superior izquierdo.

    El halo simula iluminación de estudio: da profundidad sin recurrir a
    texturas complejas. Se pinta sobre una capa RGBA y se compone encima.
    """
    w, h = img.size
    draw = ImageDraw.Draw(img)
    # Gradiente vertical navy soft (arriba) → navy deep (abajo).
    for y in range(h):
        t = y / max(1, h - 1)
        r = int(NAVY_SOFT[0] * (1 - t) + NAVY_DEEP[0] * t)
        g = int(NAVY_SOFT[1] * (1 - t) + NAVY_DEEP[1] * t)
        b = int(NAVY_SOFT[2] * (1 - t) + NAVY_DEEP[2] * t)
        draw.line([(0, y), (w, y)], fill=(r, g, b))

    # Halo suave arriba a la izquierda — se logra con un ellipse blur-like
    # dibujado en varias capas concéntricas de opacidad decreciente. Evita
    # ImageFilter.GaussianBlur por costo; 6 anillos quedan suficientemente
    # suaves a la resolución del card.
    halo = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    hd = ImageDraw.Draw(halo)
    cx, cy, radio = int(w * 0.18), int(h * 0.10), int(min(w, h) * 0.55)
    for i in range(6):
        alpha = int(18 * (1 - i / 6))
        r_i = radio - i * int(radio * 0.12)
        hd.ellipse([cx - r_i, cy - r_i, cx + r_i, cy + r_i], fill=(255, 255, 255, alpha))
    img.alpha_composite(halo)


def _draw_logo_chip(
    img: Image.Image,
    domain: str | None,
    sigla: str,
    *,
    top_left: tuple[int, int],
    size: int,
) -> None:
    """Tarjeta cuadrada blanca redondeada con el logo de la institución.

    Reemplaza el disco circular anterior. La tarjeta se percibe como un
    "elevado" sobre el fondo, con sombra simulada por un rect gris debajo
    desplazado 2-3 px. Si no hay logo, se pinta la sigla en navy centrada.
    """
    x, y = top_left
    draw = ImageDraw.Draw(img)
    radius = int(size * 0.22)

    # Sombra simulada: rectángulo más oscuro, desplazado.
    draw.rounded_rectangle(
        [x + 3, y + 5, x + size + 3, y + size + 5],
        radius=radius,
        fill=(0, 0, 0, 70),
    )
    # Tarjeta blanca.
    draw.rounded_rectangle(
        [x, y, x + size, y + size],
        radius=radius,
        fill=WHITE,
        outline=GOLD,
        width=3,
    )

    logo = _load_institution_logo(domain)
    if logo is not None:
        inner = size - int(size * 0.22)
        logo = logo.resize((inner, inner), Image.LANCZOS)
        # Máscara redondeada alineada con el chip para no tapar el borde dorado.
        inner_radius = max(0, radius - int(size * 0.08))
        mask = Image.new("L", (inner, inner), 0)
        ImageDraw.Draw(mask).rounded_rectangle([0, 0, inner, inner], radius=inner_radius, fill=255)
        img.paste(logo, (x + (size - inner) // 2, y + (size - inner) // 2), mask)
        return

    # Fallback: sigla bold centrada en navy.
    letras = sigla[:3] if len(sigla) <= 3 else sigla[:3]
    f_size = int(size * 0.42) if len(letras) >= 3 else int(size * 0.52)
    f = _load("bold", max(24, f_size))
    tw = draw.textlength(letras, font=f)
    th = f.size
    draw.text(
        (x + size / 2 - tw / 2, y + size / 2 - th * 0.62),
        letras,
        font=f,
        fill=NAVY,
    )


def _draw_icon(
    draw: ImageDraw.ImageDraw,
    kind: str,
    center: tuple[int, int],
    color: tuple[int, int, int],
    *,
    scale: float = 1.0,
) -> None:
    """Iconos vectoriales simples dibujados con Pillow (evitan fuentes emoji).

    ``scale`` multiplica todas las medidas para que el mismo icono rinda
    bien dentro de chips pequeños (36px) o en el centro del badge de
    alerta (48px).
    """
    cx, cy = center
    s = scale

    def _s(v: float) -> int:
        return int(round(v * s))

    if kind == "pin":
        # Gota invertida (ubicación).
        draw.pieslice(
            [cx - _s(10), cy - _s(14), cx + _s(10), cy + _s(6)],
            180, 360, fill=color,
        )
        draw.polygon(
            [(cx - _s(10), cy - _s(4)), (cx + _s(10), cy - _s(4)), (cx, cy + _s(12))],
            fill=color,
        )
        draw.ellipse([cx - _s(4), cy - _s(8), cx + _s(4), cy], fill=WHITE)
    elif kind == "briefcase":
        draw.rounded_rectangle(
            [cx - _s(12), cy - _s(6), cx + _s(12), cy + _s(10)], radius=_s(3), fill=color,
        )
        draw.rectangle([cx - _s(6), cy - _s(10), cx + _s(6), cy - _s(6)], fill=color)
        draw.line([cx - _s(12), cy + _s(2), cx + _s(12), cy + _s(2)], fill=WHITE, width=max(2, _s(2)))
    elif kind == "clock":
        draw.ellipse(
            [cx - _s(12), cy - _s(12), cx + _s(12), cy + _s(12)], outline=color, width=max(2, _s(2)),
        )
        draw.line([cx, cy, cx, cy - _s(8)], fill=color, width=max(2, _s(2)))
        draw.line([cx, cy, cx + _s(6), cy + _s(2)], fill=color, width=max(2, _s(2)))
    elif kind == "alert":
        # Triángulo con ! interno, silueta interna blanca para contraste.
        draw.polygon(
            [(cx, cy - _s(14)), (cx + _s(14), cy + _s(10)), (cx - _s(14), cy + _s(10))],
            fill=color,
        )
        draw.line([cx, cy - _s(6), cx, cy + _s(4)], fill=WHITE, width=max(3, _s(3)))
        draw.ellipse([cx - _s(2), cy + _s(6), cx + _s(2), cy + _s(10)], fill=WHITE)
    elif kind == "money":
        draw.ellipse(
            [cx - _s(12), cy - _s(12), cx + _s(12), cy + _s(12)], outline=color, width=max(2, _s(2)),
        )
        f = _load("bold", max(14, _s(20)))
        tw = draw.textlength("$", font=f)
        draw.text((cx - tw / 2, cy - _s(13)), "$", font=f, fill=color)
    elif kind == "arrow":
        # Flecha de CTA (línea + cabeza).
        draw.line([cx - _s(10), cy, cx + _s(10), cy], fill=color, width=max(3, _s(3)))
        draw.polygon(
            [(cx + _s(10), cy - _s(7)), (cx + _s(18), cy), (cx + _s(10), cy + _s(7))],
            fill=color,
        )
    elif kind == "check":
        draw.line(
            [cx - _s(10), cy, cx - _s(2), cy + _s(8)], fill=color, width=max(3, _s(3)),
        )
        draw.line(
            [cx - _s(2), cy + _s(8), cx + _s(12), cy - _s(8)], fill=color, width=max(3, _s(3)),
        )


def _draw_icon_chip(
    img: Image.Image,
    kind: str,
    center: tuple[int, int],
    *,
    chip_r: int,
    fill: tuple[int, int, int],
    icon_color: tuple[int, int, int] = WHITE,
    icon_scale: float = 1.1,
) -> None:
    """Círculo relleno con un icono vectorial dentro. Usado en info-cards."""
    cx, cy = center
    draw = ImageDraw.Draw(img)
    draw.ellipse([cx - chip_r, cy - chip_r, cx + chip_r, cy + chip_r], fill=fill)
    _draw_icon(draw, kind, (cx, cy), icon_color, scale=icon_scale)


def _draw_info_card(
    img: Image.Image,
    *,
    xy: tuple[int, int],
    size: tuple[int, int],
    icon: str,
    label: str,
    value: str,
    value_font: ImageFont.FreeTypeFont,
    label_font: ImageFont.FreeTypeFont,
    accent: tuple[int, int, int] = GOLD,
) -> None:
    """Tarjeta de información con icono-chip + label + valor.

    Layout interno:
      ┌──────────────────────────────┐
      │ ⬤icon    UBICACIÓN           │  label (11–15pt, gold)
      │          Puente Alto · RM    │  valor (18–28pt, bold, blanco)
      └──────────────────────────────┘
    El chip tiene fondo dorado; el card tiene fondo navy_elev con borde
    navy_edge. El valor encoge su tipografía si no entra a la primera, y
    sólo como último recurso se trunca con "…".
    """
    x, y = xy
    w, h = size
    draw = ImageDraw.Draw(img)

    draw.rounded_rectangle(
        [x, y, x + w, y + h],
        radius=18,
        fill=NAVY_ELEV,
        outline=NAVY_EDGE,
        width=2,
    )

    chip_r = int(h * 0.22)
    chip_cx = x + chip_r + 16
    chip_cy = y + h // 2
    _draw_icon_chip(img, icon, (chip_cx, chip_cy), chip_r=chip_r, fill=accent, icon_color=NAVY, icon_scale=1.0)

    text_left = chip_cx + chip_r + 14
    text_right = x + w - 16
    max_text_w = max(40, text_right - text_left)

    # Label en mayúsculas.
    label_up = label.upper()
    draw.text(
        (text_left, y + h * 0.22 - label_font.size * 0.5),
        label_up,
        font=label_font,
        fill=accent,
    )

    # Auto-fit del valor: probamos el tamaño original; si no cabe, bajamos
    # hasta 16pt. Sólo si ni a 16pt entra, truncamos con "…".
    v_font = value_font
    min_size = 16
    while draw.textlength(value, font=v_font) > max_text_w and v_font.size > min_size:
        v_font = _load("bold", max(min_size, v_font.size - 2))
    truncated = value
    while draw.textlength(truncated, font=v_font) > max_text_w and len(truncated) > 6:
        truncated = truncated[:-2].rstrip(" ,.-") + "…"
    draw.text(
        (text_left, y + h * 0.58 - v_font.size * 0.5),
        truncated,
        font=v_font,
        fill=WHITE,
    )


def _draw_cta_button(
    img: Image.Image,
    *,
    xy: tuple[int, int],
    text: str,
    font: ImageFont.FreeTypeFont,
) -> tuple[int, int]:
    """Botón CTA dorado con flecha. Devuelve (width, height) del botón."""
    x, y = xy
    draw = ImageDraw.Draw(img)
    pad_x = 30
    height = int(font.size * 2.0)
    arrow_w = 28
    gap = 14
    text_w = int(draw.textlength(text, font=font))
    width = text_w + arrow_w + gap + pad_x * 2

    # Sombra sutil por debajo (simula elevación).
    draw.rounded_rectangle(
        [x + 2, y + 4, x + width + 2, y + height + 4],
        radius=height // 2,
        fill=(0, 0, 0, 120),
    )
    # Botón gold con borde deep gold.
    draw.rounded_rectangle(
        [x, y, x + width, y + height],
        radius=height // 2,
        fill=GOLD,
        outline=GOLD_DEEP,
        width=2,
    )
    # Texto navy (mejor contraste en gold).
    draw.text(
        (x + pad_x, y + height / 2 - font.size * 0.62),
        text,
        font=font,
        fill=NAVY_DEEP,
    )
    # Flecha a la derecha.
    _draw_icon(
        draw, "arrow",
        (x + pad_x + text_w + gap + arrow_w // 2, y + height // 2),
        NAVY_DEEP, scale=1.0,
    )
    return width, height


def _draw_status_pill(
    img: Image.Image,
    *,
    xy: tuple[int, int],
    text: str,
    font: ImageFont.FreeTypeFont,
    alerta: bool,
) -> tuple[int, int]:
    """Pill de estado en el header (superior derecha).

    - Alerta (cierra pronto): fondo rojo sólido con icono de alerta.
    - Normal: fondo navy elevado con borde dorado + icono reloj.
    """
    x, y = xy
    draw = ImageDraw.Draw(img)
    pad_x = 20
    icon_w = 22
    gap = 10
    height = int(font.size * 1.9)
    text_w = int(draw.textlength(text, font=font))
    width = text_w + icon_w + gap + pad_x * 2

    if alerta:
        bg, outline, fg, icon_kind = ALERT, ALERT_SOFT, WHITE, "alert"
    else:
        bg, outline, fg, icon_kind = NAVY_ELEV, GOLD, WHITE, "clock"

    draw.rounded_rectangle(
        [x, y, x + width, y + height],
        radius=height // 2,
        fill=bg,
        outline=outline,
        width=2,
    )
    _draw_icon(draw, icon_kind, (x + pad_x + icon_w // 2, y + height // 2), fg, scale=0.85)
    draw.text(
        (x + pad_x + icon_w + gap, y + height / 2 - font.size * 0.62),
        text,
        font=font,
        fill=fg,
    )
    return width, height


def _draw_brand_mark(img: Image.Image, top_left: tuple[int, int], *, size: int = 44) -> int:
    """Disco navy con anillo dorado + wordmark "estadoemplea"."""
    x, y = top_left
    draw = ImageDraw.Draw(img)
    # Disco con anillo dorado (versión simplificada del favicon).
    r = size // 2
    cx, cy = x + r, y + r
    draw.ellipse([cx - r, cy - r, cx + r, cy + r], fill=GOLD)
    draw.ellipse([cx - r + 4, cy - r + 4, cx + r - 4, cy + r - 4], fill=NAVY_DEEP)
    draw.ellipse([cx - 6, cy - 6, cx + 6, cy + 6], fill=WHITE)

    # Wordmark.
    f_brand = _load("bold", int(size * 0.48))
    f_kicker = _load("regular", int(size * 0.32))
    brand_x = x + size + 14
    draw.text((brand_x, y + size * 0.06), "estadoemplea", font=f_brand, fill=WHITE)
    draw.text(
        (brand_x, y + size * 0.60),
        "empleo público · Chile",
        font=f_kicker,
        fill=TEXT_DIM,
    )
    # Retorna el alto ocupado para que el caller posicione la banda.
    return size


def _shrink_cargo(
    draw: ImageDraw.ImageDraw,
    cargo: str,
    max_w: int,
    *,
    start_size: int,
    min_size: int,
    max_lines: int,
) -> tuple[ImageFont.FreeTypeFont, list[str]]:
    """Elige el tamaño de fuente más grande que quepa en max_lines líneas."""
    size = start_size
    while size >= min_size:
        font = _load("bold", size)
        lines = _wrap(cargo, font, max_w, draw)
        if len(lines) <= max_lines:
            return font, lines
        size -= 4
    font = _load("bold", min_size)
    return font, _truncate_lines(_wrap(cargo, font, max_w, draw), max_lines)


# ── Renderizador principal ───────────────────────────────────────────────
def render_offer_card(oferta: dict[str, Any], fmt: Format = "horizontal") -> bytes:
    """Renderiza la tarjeta OG/RRSS y devuelve bytes PNG.

    ``fmt``:
      - ``"horizontal"`` → 1200x630 (Open Graph / Twitter / LinkedIn / WhatsApp).
      - ``"square"``     → 1080x1080 (Instagram, mensajería cuadrada).

    Composición visual (top → bottom):

      1. **Acento dorado** vertical en el borde izquierdo (8–12 px).
      2. **Header**: brand mark + wordmark a la izquierda, pill de estado a
         la derecha (alerta roja si cierra en ≤3 días, navy+gold si normal).
      3. **Logo-chip** institucional (tarjeta blanca con borde dorado), en
         la esquina superior derecha del bloque de título.
      4. **Kicker** "OFERTA VIGENTE" / "CONCURSO PÚBLICO" en dorado, caps.
      5. **Cargo** titular (bold, autoshrink, hasta 3 líneas en horizontal
         / 4 en cuadrada).
      6. **Institución** en blanco tenue.
      7. **Info-cards** (2 o 3 según formato): icono-chip dorado + label
         en mayúsculas + valor bold. Cubre ubicación, modalidad y
         remuneración.
      8. **CTA**: botón dorado con flecha + dominio al costado.
      9. **Línea dorada inferior** de marca.
    """
    spec = _SPEC.get(fmt) or _SPEC["horizontal"]
    W, H, PAD = spec["w"], spec["h"], spec["pad"]
    title_size, sub_size = spec["title"], spec["sub"]

    # Canvas RGBA para permitir compositing del halo + sombras.
    img = Image.new("RGBA", (W, H), NAVY_DEEP)
    _gradient_background(img)
    d = ImageDraw.Draw(img)

    # ── 1. Acento dorado izquierdo ──────────────────────────────────────
    accent_w = 10 if fmt == "horizontal" else 14
    d.rectangle([0, 0, accent_w, H], fill=GOLD)

    content_left = accent_w + PAD
    content_right = W - PAD

    # ── 2. Header: brand (izq) + status pill (der) ──────────────────────
    brand_y = PAD - 10 if fmt == "horizontal" else PAD - 6
    brand_size = 44 if fmt == "horizontal" else 56
    _draw_brand_mark(img, (content_left, brand_y), size=brand_size)

    cierre_text, alerta = _format_cierre(oferta)
    if cierre_text:
        f_status = _load("bold", 20 if fmt == "horizontal" else 24)
        short = _shorten_cierre(cierre_text)
        # Medimos primero para anclar el pill desde la derecha (evita pisar
        # el logo-chip que vive en la esquina superior derecha del título).
        pill_w = _measure_status_pill(d, short, f_status)
        pill_h = int(f_status.size * 1.9)
        pill_x = content_right - pill_w
        pill_y = brand_y + (brand_size - pill_h) // 2
        _draw_status_pill(img, xy=(pill_x, pill_y), text=short, font=f_status, alerta=alerta)

    # ── 3. Logo-chip institucional ──────────────────────────────────────
    sigla_text = _sigla_fallback(oferta.get("institucion"), oferta.get("sigla"))
    logo_domain = oferta.get("institucion_sitio_web")
    chip_size = 132 if fmt == "horizontal" else 168
    chip_x = content_right - chip_size
    chip_y = brand_y + brand_size + (28 if fmt == "horizontal" else 40)
    _draw_logo_chip(img, logo_domain, sigla_text, top_left=(chip_x, chip_y), size=chip_size)

    # ── 4. Kicker ───────────────────────────────────────────────────────
    # Pequeño eyebrow label arriba del titular; señala urgencia o tipo.
    kicker_label = _pick_kicker(oferta)
    f_kicker = _load("bold", 18 if fmt == "horizontal" else 22)
    kicker_y = brand_y + brand_size + (28 if fmt == "horizontal" else 48)
    d.text((content_left, kicker_y), kicker_label, font=f_kicker, fill=GOLD)

    # ── 5. Cargo (titular) ──────────────────────────────────────────────
    cargo = (oferta.get("cargo") or "Oferta laboral").strip()
    # Área del título: respeta el logo-chip en ambos formatos. El chip
    # vive en la esquina superior derecha; el título se trunca antes de
    # llegar a él para no chocar con el borde dorado.
    chip_guard_x = chip_x - 28
    title_right = chip_guard_x
    title_max_w = max(240, title_right - content_left)
    max_title_lines = 2 if fmt == "horizontal" else 3
    f_cargo, cargo_lines = _shrink_cargo(
        d, cargo,
        title_max_w,
        start_size=title_size,
        min_size=40 if fmt == "horizontal" else 52,
        max_lines=max_title_lines,
    )
    # Si todavía no entra en las líneas target, permitimos una línea extra
    # en horizontal (casos extremos) antes de truncar.
    if len(cargo_lines) > max_title_lines and fmt == "horizontal":
        max_title_lines = 3
        f_cargo, cargo_lines = _shrink_cargo(
            d, cargo, title_max_w, start_size=58, min_size=40, max_lines=3,
        )
    title_y = kicker_y + (30 if fmt == "horizontal" else 40)
    y = title_y
    line_h = int(f_cargo.size * 1.08)
    for line in cargo_lines:
        d.text((content_left, y), line, font=f_cargo, fill=WHITE)
        y += line_h

    # ── 6. Institución ──────────────────────────────────────────────────
    institucion = (oferta.get("institucion") or "Institución pública").strip()
    f_inst = _load("regular", sub_size)
    # La institución también respeta el logo-chip en horizontal si éste
    # baja más que el título.
    inst_max_w = max(240, title_right - content_left) if fmt == "horizontal" and y < chip_y + chip_size else content_right - content_left
    inst_lines = _truncate_lines(_wrap(institucion, f_inst, inst_max_w, d), 2)
    y += int(sub_size * 0.3)
    for line in inst_lines:
        d.text((content_left, y), line, font=f_inst, fill=TEXT_DIM)
        y += int(sub_size * 1.20)

    # ── 7. Info-cards (ubicación · modalidad · remuneración) ────────────
    card_entries = _build_info_cards(oferta)

    # Calcula espacio disponible entre `y` y la zona del CTA.
    cta_block_h = (88 if fmt == "horizontal" else 108)
    bottom_guard = PAD - 8
    cards_top_min = y + (20 if fmt == "horizontal" else 32)
    cards_bottom_max = H - bottom_guard - cta_block_h - 18

    if card_entries and cards_bottom_max - cards_top_min >= 80:
        card_h = 92 if fmt == "horizontal" else 126
        gap = 14 if fmt == "horizontal" else 20
        available_w = content_right - content_left
        n = min(len(card_entries), 3)
        card_w = (available_w - gap * (n - 1)) // n
        cards_y = cards_bottom_max - card_h
        f_value = _load("bold", 22 if fmt == "horizontal" else 28)
        f_label = _load("bold", 12 if fmt == "horizontal" else 15)
        cx = content_left
        for icon, label, value in card_entries[:n]:
            _draw_info_card(
                img,
                xy=(cx, cards_y),
                size=(card_w, card_h),
                icon=icon,
                label=label,
                value=value,
                value_font=f_value,
                label_font=f_label,
            )
            cx += card_w + gap

    # ── 8. CTA dorado + dominio ─────────────────────────────────────────
    f_cta = _load("bold", 24 if fmt == "horizontal" else 30)
    cta_y = H - bottom_guard - int(f_cta.size * 2.0)
    cta_width, _ = _draw_cta_button(
        img,
        xy=(content_left, cta_y),
        text="Postula ahora",
        font=f_cta,
    )
    # Dominio al costado del botón, alineado al centro vertical.
    f_domain = _load("regular", 18 if fmt == "horizontal" else 22)
    dom_x = content_left + cta_width + 22
    dom_text = "estadoemplea.pages.dev"
    d.text(
        (dom_x, cta_y + f_cta.size * 0.30),
        dom_text,
        font=f_domain,
        fill=TEXT_DIM,
    )

    # ── 9. Línea dorada inferior ────────────────────────────────────────
    d.rectangle([0, H - 4, W, H], fill=GOLD)

    # Pasamos de RGBA a RGB al exportar (PNG puede ser RGB; ahorra peso).
    buf = BytesIO()
    img.convert("RGB").save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def _pick_kicker(oferta: dict[str, Any]) -> str:
    """Mini eyebrow label encima del titular, según estado de la oferta."""
    estado = str(oferta.get("estado") or "").lower()
    if estado == "closing_today":
        return "CIERRA HOY · POSTULA AHORA"
    dias = oferta.get("dias_restantes")
    if isinstance(dias, int) and 0 <= dias <= 3:
        return "ÚLTIMOS DÍAS · OFERTA VIGENTE"
    tipo = (oferta.get("tipo_contrato") or "").strip().upper()
    if tipo:
        return f"OFERTA VIGENTE · {tipo}"
    return "OFERTA VIGENTE · SECTOR PÚBLICO"


def _shorten_cierre(text: str) -> str:
    """Versión abreviada (mayúsculas, 2–4 palabras) para el pill del header."""
    upper = text.upper()
    if "HOY" in upper:
        return "CIERRA HOY"
    if "MAÑANA" in upper:
        return "CIERRA MAÑANA"
    # "Cierra en N días — postula ya" → "CIERRA EN N DÍAS"
    if "—" in text:
        text = text.split("—", 1)[0].strip()
    lower = text.lower()
    # "Quedan N días para postular" → "N DÍAS RESTAN"
    if lower.startswith("quedan ") and "para postular" in lower:
        num_part = text[7:].lower().replace("para postular", "").strip()
        return f"{num_part} restan".upper()
    # "N días para postular" → "N DÍAS RESTAN"
    if "para postular" in lower:
        return text.lower().replace("para postular", "").strip().upper() + " RESTAN"
    if lower.startswith("cierra "):
        return text.upper()
    return text.upper()


def _measure_status_pill(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont) -> int:
    """Mide el ancho que tomará el pill de estado con _draw_status_pill."""
    pad_x = 20
    icon_w = 22
    gap = 10
    text_w = int(draw.textlength(text, font=font))
    return text_w + icon_w + gap + pad_x * 2


def _build_info_cards(oferta: dict[str, Any]) -> list[tuple[str, str, str]]:
    """Ensambla las 2–3 info-cards que aparecen sobre el CTA.

    Devuelve tuples ``(icon, LABEL, valor)``. Omite tarjetas sin datos —
    el layout se adapta al total de cards recibidas.
    """
    cards: list[tuple[str, str, str]] = []

    # Ubicación
    region = (oferta.get("region") or "").strip()
    ciudad = (oferta.get("ciudad") or "").strip()
    ubic_parts: list[str] = []
    if ciudad and (not region or ciudad.lower() not in region.lower()):
        ubic_parts.append(ciudad)
    if region:
        ubic_parts.append(region)
    if ubic_parts:
        cards.append(("pin", "Ubicación", " · ".join(ubic_parts)))

    # Modalidad / tipo de contrato
    tipo = (oferta.get("tipo_contrato") or "").strip()
    if tipo:
        cards.append(("briefcase", "Modalidad", tipo.title()))

    # Remuneración
    renta = _format_renta(oferta)
    if renta:
        cards.append(("money", "Remuneración", renta))

    return cards


__all__ = ["render_offer_card", "Format"]
