"""Labelled sign plates: icon + Chinese (Huninn) + English (Jost), text as outlines."""
import os
from fontTools.ttLib import TTFont
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen

from icons import ICONS, COLORWAYS, BLUE, INK

FONT_DIR = os.environ.get("ICON_FONT_DIR", os.path.join(os.path.dirname(__file__), "fonts"))
_fonts = {}


def font(name):
    if name not in _fonts:
        f = TTFont(os.path.join(FONT_DIR, name))
        _fonts[name] = (f, f.getGlyphSet(), f.getBestCmap(), f["head"].unitsPerEm, f["hmtx"])
    return _fonts[name]


def text_path(text, fontfile, size, x, y, tracking=0.0):
    """Return (svg path d, advance width) for text with baseline at (x, y)."""
    f, gs, cmap, upm, hmtx = font(fontfile)
    s = size / upm
    pen = SVGPathPen(gs)
    cx = x
    for ch in text:
        g = cmap.get(ord(ch))
        if g is None:
            raise KeyError(f"{ch!r} not in {fontfile}")
        gs[g].draw(TransformPen(pen, (s, 0, 0, -s, cx, y)))
        cx += hmtx[g][0] * s + tracking * size
    width = cx - x - (tracking * size if text else 0)
    return pen.getCommands(), width


SIGN_W, SIGN_H = 500, 160


def sign_svg(key, theme, icon_svg_fn):
    spec = ICONS[key]
    if theme == "light":
        plate, zh_c, en_c, cw = "#FFFFFF", INK, INK, "color"
    else:
        plate, zh_c, en_c, cw = BLUE, "#FFFFFF", "#FFFFFF", "white"
    icon = icon_svg_fn(key, cw, size=120)
    icon_inner = icon[icon.index("</title>") + 8:-6].replace(f'id="{key}-', f'id="s-{key}-').replace(f'url(#{key}-', f'url(#s-{key}-')
    zh, _ = text_path(spec["zh"], "Huninn-Regular.ttf", 46, 168, 84, tracking=0.04)
    en, _ = text_path(spec["en"].upper(), "Jost-Regular.ttf", 16.5, 170, 120, tracking=0.14)
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {SIGN_W} {SIGN_H}" width="{SIGN_W}" height="{SIGN_H}">'
        f'<title>{spec["zh"]} {spec["en"]}</title>'
        f'<rect width="{SIGN_W}" height="{SIGN_H}" rx="24" fill="{plate}"/>'
        f'<g transform="translate(20 20)">{icon_inner}</g>'
        f'<path d="{zh}" fill="{zh_c}"/>'
        f'<path d="{en}" fill="{en_c}"/>'
        "</svg>"
    )
