"""Build the Taoyuan Children's Art Center icon set (frameless, no text).

    python3 tools/build.py            # writes svg/<colourway>/ and overview.html
    node tools/render.js              # writes png/<colourway>/ and overview.png
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from icons import ICONS, SLASH, SW, STROKE_SCALE, COLORWAYS, BLUE  # noqa: E402

ROOT = os.path.dirname(HERE)
VIEWBOX = "10 10 100 100"  # pictograms are drawn on a 120 grid; crop the margin


def fill_colors(s, cw):
    for k, v in cw.items():
        s = s.replace("{" + k + "}", v)
    return s


def mask(mid, cutouts):
    return (f'<mask id="{mid}" maskUnits="userSpaceOnUse" x="-60" y="-60" width="240" height="240">'
            '<rect x="-60" y="-60" width="240" height="240" fill="#fff"/>'
            f'<g fill="#000" stroke="#000" stroke-width="0" stroke-linecap="round" stroke-linejoin="round">{cutouts}</g>'
            '</mask>')


def icon_svg(key, cwname, size=120):
    spec = ICONS[key]
    cw = COLORWAYS[cwname]
    layers = spec["draw"]()
    if isinstance(layers, str):
        layers = [(layers, None)]
    prohibit = spec["kind"] == "prohibit"
    x1, y1, x2, y2 = SLASH
    # gap that separates the slash from whatever it crosses
    slash_cut = f'<path d="M{x1} {y1} L{x2} {y2}" stroke-width="{SW + 6}" stroke-linecap="butt"/>'
    defs, body = [], []
    for i, (content, cut) in enumerate(layers):
        if cut:
            mid = f"{key}-{cwname}-m{i}"
            defs.append(mask(mid, cut))
            content = f'<g mask="url(#{mid})">{content}</g>'
        body.append(content)
    pict = "".join(body)
    if spec.get("xf"):
        pict = f'<g transform="{spec["xf"]}">{pict}</g>'
    if prohibit:  # slash gap is applied outside any pictogram transform
        mid = f"{key}-{cwname}-slash"
        defs.append(mask(mid, slash_cut))
        pict = f'<g mask="url(#{mid})">{pict}</g>'
    slash = f'<path d="M{x1} {y1} L{x2} {y2}" stroke="{{slash}}"/>' if prohibit else ""
    inner = (
        (f'<defs>{"".join(defs)}</defs>' if defs else "")
        + f'<g fill="none" stroke-width="{SW}" stroke-linecap="round" stroke-linejoin="round">'
        + f'<g stroke="{{fg}}">{pict}</g>'
        + slash
        + "</g>"
    )
    # thin every stroke (pictogram, mask gaps and slash) by the same factor
    inner = re.sub(r'stroke-width="([\d.]+)"',
                   lambda m: f'stroke-width="{float(m.group(1)) * STROKE_SCALE:.2f}"', inner)
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{VIEWBOX}" width="{size}" height="{size}">'
            f'<title>{spec["zh"]} {spec["en"]}</title>' + fill_colors(inner, cw) + "</svg>")


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text + "\n")


def overview_html():
    grid = lambda cw, size: "".join(f'<div class="cell">{icon_svg(k, cw, size)}</div>' for k in ICONS)
    return f'''<!doctype html><html><head><meta charset="utf-8"><style>
body{{margin:0;background:#fff;width:1200px}}
.grid{{display:grid;grid-template-columns:repeat(5,1fr);gap:56px 0;padding:72px 60px}}
.cell{{display:flex;justify-content:center}}
.small{{display:grid;grid-template-columns:repeat(10,1fr);gap:28px 0;padding:36px 40px}}
.dark{{background:{BLUE}}} .line{{height:1px;background:#e4e9f1;margin:0 60px}}
</style></head><body>
<div class="grid">{grid("color", 150)}</div>
<div class="line"></div>
<div class="small">{grid("blue", 72)}</div>
<div class="small dark">{grid("white", 72)}</div>
</body></html>'''


def main():
    for cw in COLORWAYS:
        for i, key in enumerate(ICONS, 1):
            write(os.path.join(ROOT, "svg", cw, f"{i:02d}-{key}.svg"), icon_svg(key, cw))
    write(os.path.join(ROOT, "overview.html"), overview_html())


if __name__ == "__main__":
    main()
