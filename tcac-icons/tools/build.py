"""Build the Taoyuan Children's Art Center icon set.

    python3 tools/build.py            # writes svg/, signs/, overview.html
    node tools/render.js              # writes png/ and overview.png from those

Sign plates need Huninn-Regular.ttf and Jost-Regular.ttf (both SIL OFL, from
Google Fonts) in tools/fonts/ or in the folder named by ICON_FONT_DIR.
"""
import base64
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from icons import ICONS, FRAMES, SLASH, SW, COLORWAYS, BLUE, INK  # noqa: E402

ROOT = os.path.dirname(HERE)


def fill_colors(s, cw):
    for k, v in cw.items():
        s = s.replace("{" + k + "}", v)
    return s


def mask(mid, cutouts):
    return (f'<mask id="{mid}" maskUnits="userSpaceOnUse" x="0" y="0" width="120" height="120">'
            '<rect width="120" height="120" fill="#fff"/>'
            f'<g fill="#000" stroke="#000" stroke-width="0" stroke-linecap="round" stroke-linejoin="round">{cutouts}</g>'
            '</mask>')


def icon_svg(key, cwname, size=120):
    spec = ICONS[key]
    cw = COLORWAYS[cwname]
    layers = spec["draw"]()
    if isinstance(layers, str):
        layers = [(layers, None)]
    prohibit = spec["frame"] == "prohibit"
    x1, y1, x2, y2 = SLASH
    # gap that separates the slash from whatever it crosses
    slash_cut = f'<path d="M{x1} {y1} L{x2} {y2}" stroke-width="{SW + 6}" stroke-linecap="butt"/>'
    defs, body = [], []
    for i, (content, cut) in enumerate(layers):
        cuts = (cut or "") + (slash_cut if prohibit else "")
        if cuts:
            mid = f"{key}-{cwname}-m{i}"
            defs.append(mask(mid, cuts))
            body.append(f'<g mask="url(#{mid})">{content}</g>')
        else:
            body.append(content)
    slash = f'<path d="M{x1} {y1} L{x2} {y2}" stroke="{{prohibit}}"/>' if prohibit else ""
    inner = (
        (f'<defs>{"".join(defs)}</defs>' if defs else "")
        + f'<g fill="none" stroke-width="{SW}" stroke-linecap="round" stroke-linejoin="round">'
        + FRAMES[spec["frame"]]()
        + f'<g stroke="{{fg}}">{"".join(body)}</g>'
        + slash
        + "</g>"
    )
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 120 120" width="{size}" height="{size}">'
            f'<title>{spec["zh"]} {spec["en"]}</title>' + fill_colors(inner, cw) + "</svg>")


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text + "\n")


def overview_html(logo_path):
    logo = ""
    if logo_path and os.path.exists(logo_path):
        logo = base64.b64encode(open(logo_path, "rb").read()).decode()
    cards = "".join(
        f'<div class="card">{icon_svg(k, "color", 150)}<div class="zh">{s["zh"]}</div>'
        f'<div class="en">{s["en"].upper()}</div></div>' for k, s in ICONS.items())
    strip = lambda cw: "".join(icon_svg(k, cw, 84) for k in ICONS)
    return f'''<!doctype html><html><head><meta charset="utf-8"><style>
body{{margin:0;background:#fff;font-family:Huninn,sans-serif;color:{INK};width:1200px}}
header{{padding:56px 64px 28px;border-bottom:1px solid #e4e9f1}} header img{{width:620px;display:block}}
header p{{font-family:Jost,sans-serif;letter-spacing:.16em;font-size:14px;margin:18px 0 0;color:{BLUE}}}
.grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:22px;padding:36px 64px}}
.card{{border:1.5px solid #e4e9f1;border-radius:24px;padding:26px 10px 22px;display:flex;flex-direction:column;align-items:center}}
.zh{{font-size:28px;margin-top:14px;letter-spacing:.04em}} .en{{font-family:Jost,sans-serif;font-size:13px;letter-spacing:.16em;margin-top:6px}}
.strip{{display:flex;justify-content:space-between;padding:18px 64px}} .strip.dark{{background:{BLUE};padding:22px 64px;margin:0 64px 56px;border-radius:24px}}
.label{{font-family:Jost,sans-serif;font-size:12px;letter-spacing:.16em;color:#8a96a8;padding:8px 64px 0}}
</style></head><body>
<header>{f'<img src="data:image/jpeg;base64,{logo}">' if logo else ''}<p>WAYFINDING ICONS · 館內標示圖示</p></header>
<div class="grid">{cards}</div>
<div class="label">BLUE · 單色</div><div class="strip">{strip("blue")}</div>
<div class="label">WHITE · 反白</div><div class="strip dark" style="margin-top:10px">{strip("white")}</div>
</body></html>'''


def main():
    from signs import sign_svg
    for cw in COLORWAYS:
        for i, key in enumerate(ICONS, 1):
            write(os.path.join(ROOT, "svg", cw, f"{i:02d}-{key}.svg"), icon_svg(key, cw))
    for theme in ("light", "dark"):
        for i, key in enumerate(ICONS, 1):
            write(os.path.join(ROOT, "signs", theme, f"{i:02d}-{key}.svg"), sign_svg(key, theme, icon_svg))
    write(os.path.join(ROOT, "overview.html"), overview_html(os.environ.get("LOGO")))


if __name__ == "__main__":
    main()
