"""OG/Twitter card image generator for individual offers.

Renders a 1200x630 PNG with the brand palette (navy + gold) showing the
job title, institution and key meta. Used by the `/api/og/{id}.png`
endpoint to give each shared offer its own preview, instead of the static
`og-default.jpg`.

Pillow-only — no external network calls. Falls back to a default font if
Inter is not present on the system.
"""
from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

W, H = 1200, 630
PAD = 64
NAVY = (10, 46, 110)
NAVY_DEEP = (7, 32, 78)
GOLD = (232, 168, 32)
WHITE = (250, 250, 248)
WHITE_DIM = (220, 222, 230)
TEXT_DIM = (180, 190, 210)


def _font_paths() -> dict[str, list[Path]]:
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
            if "bold" in name:
                found["bold"].append(p)
            elif "regular" in name or "book" in name:
                found["regular"].append(p)
    return found


_FONT_CACHE: dict[tuple[str, int], ImageFont.FreeTypeFont] = {}


def _load(weight: str, size: int) -> ImageFont.FreeTypeFont:
    key = (weight, size)
    if key in _FONT_CACHE:
        return _FONT_CACHE[key]
    paths = _font_paths().get(weight) or _font_paths().get("regular") or []
    font: ImageFont.FreeTypeFont
    for path in paths:
        try:
            font = ImageFont.truetype(str(path), size)
            break
        except Exception:
            continue
    else:
        font = ImageFont.load_default()
    _FONT_CACHE[key] = font
    return font


def _wrap(text: str, font: ImageFont.FreeTypeFont, max_w: int, draw: ImageDraw.ImageDraw) -> list[str]:
    words = (text or "").split()
    lines: list[str] = []
    current = ""
    for w in words:
        candidate = (current + " " + w).strip()
        if draw.textlength(candidate, font=font) <= max_w:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = w
    if current:
        lines.append(current)
    return lines


def _truncate_lines(lines: list[str], max_lines: int) -> list[str]:
    if len(lines) <= max_lines:
        return lines
    out = lines[:max_lines]
    out[-1] = (out[-1].rstrip() + "…")[:80]
    return out


def render_offer_card(oferta: dict[str, Any]) -> bytes:
    """Render the OG card and return PNG bytes."""
    img = Image.new("RGB", (W, H), NAVY)
    d = ImageDraw.Draw(img)

    # Background gradient (cheap two-tone diagonal)
    for y in range(H):
        ratio = y / H
        r = int(NAVY[0] * (1 - ratio) + NAVY_DEEP[0] * ratio)
        g = int(NAVY[1] * (1 - ratio) + NAVY_DEEP[1] * ratio)
        b = int(NAVY[2] * (1 - ratio) + NAVY_DEEP[2] * ratio)
        d.line([(0, y), (W, y)], fill=(r, g, b))

    # Brand mark — navy disc + gold ring + inner white dot, mirroring favicon.svg
    cx, cy, r0 = W - 170, 170, 90
    d.ellipse([cx - r0, cy - r0, cx + r0, cy + r0], fill=GOLD)
    d.ellipse([cx - r0 // 2, cy - r0 // 2, cx + r0 // 2, cy + r0 // 2], fill=WHITE)

    # Kicker
    f_kicker = _load("bold", 28)
    d.text((PAD, PAD), "contrataoplanta.cl", font=f_kicker, fill=GOLD)

    # Cargo (job title) — large, up to 3 lines
    cargo = (oferta.get("cargo") or "Oferta laboral").strip()
    f_cargo = _load("bold", 72)
    cargo_lines = _truncate_lines(_wrap(cargo, f_cargo, W - PAD * 2 - 220, d), 3)
    y = PAD + 80
    for line in cargo_lines:
        d.text((PAD, y), line, font=f_cargo, fill=WHITE)
        y += 86

    # Institución
    institucion = (oferta.get("institucion") or "Institución pública").strip()
    f_inst = _load("regular", 38)
    inst_lines = _truncate_lines(_wrap(institucion, f_inst, W - PAD * 2, d), 2)
    y += 16
    for line in inst_lines:
        d.text((PAD, y), line, font=f_inst, fill=WHITE_DIM)
        y += 48

    # Pills row at the bottom (region · tipo · plazo)
    f_pill = _load("bold", 26)
    pills = []
    if oferta.get("region"):
        pills.append(("📍", str(oferta["region"])))
    if oferta.get("tipo_contrato"):
        pills.append(("📋", str(oferta["tipo_contrato"]).title()))
    dias = oferta.get("dias_restantes")
    if isinstance(dias, int) and dias >= 0:
        pills.append(("⏱", f"{dias} día" + ("s" if dias != 1 else "") + " para postular"))

    pill_y = H - PAD - 56
    pill_x = PAD
    for icon, label in pills:
        text = f"{icon}  {label}"
        tw = d.textlength(text, font=f_pill)
        pill_w = int(tw + 36)
        d.rounded_rectangle(
            [pill_x, pill_y, pill_x + pill_w, pill_y + 48],
            radius=24,
            fill=(255, 255, 255, 25),
            outline=GOLD,
            width=2,
        )
        d.text((pill_x + 18, pill_y + 8), text, font=f_pill, fill=WHITE)
        pill_x += pill_w + 14
        if pill_x > W - PAD - 200:
            break

    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()
