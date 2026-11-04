"""Generate Tucheng District Office packaging mock-up pages (HTML + inline SVG).

Usage:
    python3 build.py            # writes html/page-*.html
    node render.js              # renders html/*.html to output/*.png

Only the original artwork is used: the logo and the horizontal lockup
(logo + 新北市土城區公所 wordmark) in the four original colour combinations.
The logo, its silhouette and the wordmark were vectorised from the supplied
artwork and live in ./assets as single-path SVGs.
"""
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
ASSETS = os.path.join(HERE, "assets")
HTML_DIR = os.path.join(HERE, "html")

# ------------------------------------------------- colours of the original ---
C = {
    "maroon": "#9A3439",
    "olive": "#80843C",
    "sage": "#C4DCBF",
    "cream": "#FCF5E3",
}
# the four original banners: background -> logo/wordmark colour
BANNER_INK = {
    "sage": C["olive"],
    "olive": C["sage"],
    "cream": C["maroon"],
    "maroon": C["cream"],
}
INK = "#575757"      # line-art stroke
SHADOW = "#cfcfcf"   # flat drop shadow


def asset_path(name):
    txt = open(os.path.join(ASSETS, name + ".svg"), encoding="utf-8").read()
    return re.search(r' d="([^"]+)"', txt).group(1)


LOGO_D = asset_path("logo")
SIL_D = asset_path("logo-silhouette")
WORD_D = asset_path("wordmark")
LOGO_AR = 231 / 251   # width / height of the logo symbol
LOCKUP_AR = 634 / 112  # banner proportions measured from the original


def f(v):
    return f"{v:.2f}".rstrip("0").rstrip(".")


# ---------------------------------------------------------- primitives ---
def logo(cx, cy, h, fill, extra=""):
    w = h * LOGO_AR
    return (f'<use href="#logo" x="{f(cx - w / 2)}" y="{f(cy - h / 2)}" width="{f(w)}" '
            f'height="{f(h)}" fill="{fill}" {extra}/>')


def sil(cx, cy, h, fill="none", stroke=None, sw=1.0):
    w = h * LOGO_AR
    s = f'stroke="{stroke}" stroke-width="{f(sw * 251 / h)}" stroke-linejoin="round"' if stroke else ""
    return (f'<use href="#sil" x="{f(cx - w / 2)}" y="{f(cy - h / 2)}" width="{f(w)}" '
            f'height="{f(h)}" fill="{fill}" {s}/>')


def banner(cx, cy, w, bg):
    """One of the four original horizontal banners, pasted as-is."""
    h = w / LOCKUP_AR
    return (f'<use href="#lockup" x="{f(cx - w / 2)}" y="{f(cy - h / 2)}" width="{f(w)}" '
            f'height="{f(h)}" fill="{BANNER_INK[bg]}" color="{C[bg]}"/>')


def text(x, y, s, size, fill, weight=900, anchor="middle", extra=""):
    return (f'<text x="{f(x)}" y="{f(y)}" font-size="{f(size)}" font-weight="{weight}" fill="{fill}" '
            f'text-anchor="{anchor}" dominant-baseline="central" {extra}>{s}</text>')


def defs():
    return ("<defs>"
            f'<path id="wordpath" fill-rule="evenodd" d="{WORD_D}"/>'
            f'<symbol id="logo" viewBox="0 0 231 251"><path fill-rule="evenodd" d="{LOGO_D}"/></symbol>'
            f'<symbol id="sil" viewBox="0 0 231 251"><path d="{SIL_D}"/></symbol>'
            '<symbol id="word" viewBox="0 0 496 84"><use href="#wordpath"/></symbol>'
            # lockup laid out exactly like the original banners (634 x 112)
            '<symbol id="lockup" viewBox="0 0 634 112"><rect width="634" height="112" fill="currentColor"/>'
            '<use href="#logo" x="13.3" y="4.3" width="97.3" height="105.7"/>'
            '<use href="#word" x="133" y="12" width="496" height="84"/></symbol>'
            '<filter id="soft" x="-20%" y="-50%" width="140%" height="200%"><feGaussianBlur stdDeviation="4"/></filter>'
            '<linearGradient id="cyl" x1="0" x2="1"><stop offset="0" stop-color="#ececec"/>'
            '<stop offset=".28" stop-color="#fff"/><stop offset=".7" stop-color="#fff"/>'
            '<stop offset="1" stop-color="#dedede"/></linearGradient>'
            '<linearGradient id="gloss" x1="0" x2="1"><stop offset="0" stop-color="#000" stop-opacity=".10"/>'
            '<stop offset=".22" stop-color="#fff" stop-opacity=".18"/><stop offset=".4" stop-color="#fff" stop-opacity="0"/>'
            '<stop offset=".85" stop-color="#000" stop-opacity="0"/><stop offset="1" stop-color="#000" stop-opacity=".14"/></linearGradient>'
            "</defs>")


def poly(pts, fill="#fff", stroke=INK, sw=2):
    p = " ".join(f"{f(x)},{f(y)}" for x, y in pts)
    s = f'stroke="{stroke}" stroke-width="{sw}" stroke-linejoin="round"' if stroke else ""
    return f'<polygon points="{p}" fill="{fill}" {s}/>'


def path(d, fill="#fff", stroke=INK, sw=2, extra=""):
    s = f'stroke="{stroke}" stroke-width="{sw}" stroke-linejoin="round" stroke-linecap="round"' if stroke else ""
    return f'<path d="{d}" fill="{fill}" {s} {extra}/>'


def R(x, y, w, h, fill, rx=0, extra=""):
    return f'<rect x="{f(x)}" y="{f(y)}" width="{f(w)}" height="{f(h)}" rx="{rx}" fill="{fill}" {extra}/>'


def ground(cx, cy, rx, ry=13):
    return f'<ellipse cx="{f(cx)}" cy="{f(cy)}" rx="{f(rx)}" ry="{f(ry)}" fill="#d6d6d6" filter="url(#soft)"/>'


_clip = [0]


def flat(d, layers, shadow=True, outline=None):
    """Flat item: offset drop shadow + layers clipped to the item outline."""
    _clip[0] += 1
    cid = f"c{_clip[0]}"
    out = []
    if shadow:
        out.append(f'<path d="{d}" fill="{SHADOW}" transform="translate(13,13)"/>')
    out.append(f'<clipPath id="{cid}"><path d="{d}"/></clipPath>')
    out.append(f'<g clip-path="url(#{cid})">' + "".join(layers) + "</g>")
    if outline:
        out.append(f'<path d="{d}" fill="none" stroke="{outline}" stroke-width="1.5"/>')
    return "".join(out)


def zigzag(x1, x2, y, step=6, amp=4):
    pts, x, up = [], x1, True
    while x <= x2 + .01:
        pts.append(f"{f(x)},{f(y - (amp if up else 0))}")
        x += step / 2
        up = not up
    return "L".join(pts)


# --------------------------------------------------------- page chrome ---
PAGE_W, PAGE_H = 1600, 1131


def page(num, title, subtitle, body, note):
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {PAGE_W} {PAGE_H}" width="{PAGE_W}" height="{PAGE_H}">'
           + defs()
           + f'<rect width="{PAGE_W}" height="{PAGE_H}" fill="#fff"/>'
           + text(110, 112, title, 30, "#333", 700, "start", 'letter-spacing="1"')
           + text(110, 156, subtitle, 17, "#444", 500, "start")
           + text(1490, 92, num, 20, "#555", 400, "end")
           + body
           + text(110, 1072, note, 11, "#888", 400, "start")
           + "</svg>")
    return f"""<!doctype html>
<html lang="zh-Hant"><head><meta charset="utf-8">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+TC:wght@400;500;700;900&display=block" rel="stylesheet">
<style>html,body{{margin:0;background:#fff}}svg{{display:block;font-family:'Noto Sans TC','WenQuanYi Zen Hei',sans-serif}}</style>
</head><body>{svg}</body></html>"""


# ============================================================ PAGE 1 ======
# White line-art products with the logo applied directly (oblique view).
DX, DY = 0.55, -0.4


def box_faces(x, y, w, h, d, front="#fff", side="#e7e7e7", top="#f5f5f5"):
    ox, oy = d * DX, d * DY
    return (poly([(x, y), (x + ox, y + oy), (x + w + ox, y + oy), (x + w, y)], top)
            + poly([(x + w, y), (x + w + ox, y + oy), (x + w + ox, y + h + oy), (x + w, y + h)], side)
            + poly([(x, y), (x + w, y), (x + w, y + h), (x, y + h)], front))


def page1():
    b = []
    # stand-up kraft pouch
    b.append(ground(445, 748, 150))
    b.append(poly([(360, 252), (318, 300), (312, 722), (348, 744)], "#e7e7e7"))
    b.append(path(f"M{zigzag(360, 560, 252)}L572,744L348,744Z"))
    b.append(path("M362,272H560", "none", "#9a9a9a", 1.4))
    b.append(path("M350,716Q460,700 571,716", "none", "#9a9a9a", 1.4))
    b.append(logo(462, 492, 150, C["olive"]))

    # shopping bag with flat handles
    x, y, w, h, d = 790, 404, 280, 318, 110
    ox, oy = d * DX, d * DY

    def handle(x1, x2, yb, yt, t=13):
        return poly([(x1, yb), (x1, yt), (x2, yt), (x2, yb), (x2 - t, yb), (x2 - t, yt + t), (x1 + t, yt + t), (x1 + t, yb)])
    b.append(ground(960, 724, 200))
    b.append(handle(890 + ox * .9, 1010 + ox * .9, y + oy + 20, 286))
    b.append(poly([(x + w, y), (x + w + ox, y + oy), (x + w + ox, y + h + oy), (x + w, y + h)], "#e9e9e9"))
    b.append(path(f"M{f(x+w+ox/2)},{f(y+oy/2)}V{f(y+h+oy/2-22)}L{f(x+w)},{f(y+h)}"
                  f"M{f(x+w+ox/2)},{f(y+h+oy/2-22)}L{f(x+w+ox)},{f(y+h+oy)}", "none", "#9a9a9a", 1.4))
    b.append(poly([(x, y), (x + ox, y + oy), (x + w + ox, y + oy), (x + w, y)], "#d9d9d9"))
    b.append(handle(855, 975, y + 30, 312))
    b.append(poly([(x, y), (x + w, y), (x + w, y + h), (x, y + h)]))
    b.append(path(f"M{x},{y+16}H{x+w}", "none", "#9a9a9a", 1.4))
    b.append(logo(930, 568, 176, C["maroon"]))

    # gable-top milk carton
    x, y, w, h, d = 1205, 452, 150, 254, 90
    ox, oy = d * DX, d * DY
    rx, ry = ox / 2, oy / 2 - 56
    b.append(ground(1300, 708, 120))
    b.append(poly([(x + w, y), (x + w + ox, y + oy), (x + w + ox, y + h + oy), (x + w, y + h)], "#e7e7e7"))
    b.append(poly([(x + w, y), (x + w + ox, y + oy), (x + w + rx, y + ry)], "#dedede"))
    b.append(poly([(x + rx, y + ry - 15), (x + w + rx, y + ry - 15), (x + w + rx, y + ry), (x + rx, y + ry)]))
    b.append(poly([(x, y), (x + w, y), (x + w + rx, y + ry), (x + rx, y + ry)], "#f5f5f5"))
    b.append(poly([(x, y), (x + w, y), (x + w, y + h), (x, y + h)]))
    b.append(logo(1280, 584, 118, C["olive"]))

    # coffee cup
    cx = 235
    b.append(ground(cx, 932, 82))
    b.append(path(f"M{cx-62},776L{cx-46},922Q{cx},934 {cx+46},922L{cx+62},776Z", "url(#cyl)"))
    b.append(path(f"M{cx-60},760L{cx-50},738H{cx+50}L{cx+60},760Z", "#f5f5f5"))
    b.append(f'<ellipse cx="{cx}" cy="738" rx="50" ry="6" fill="#fff" stroke="{INK}" stroke-width="2"/>')
    b.append(f'<ellipse cx="{cx+22}" cy="737" rx="9" ry="2.5" fill="#cfcfcf"/>')
    b.append(f'<rect x="{cx-68}" y="758" width="136" height="20" rx="6" fill="#fff" stroke="{INK}" stroke-width="2"/>')
    b.append(logo(cx, 850, 98, C["maroon"]))

    # cube box
    b.append(ground(600, 962, 150))
    b.append(box_faces(478, 772, 190, 190, 130))
    b.append(path("M478,786H668", "none", "#a5a5a5", 1.3))
    b.append(logo(573, 874, 132, C["olive"]))

    # pastry gift box (糕餅禮盒): logo on the lid
    bx, by, bw, bh, bd = 755, 892, 308, 60, 190
    b.append(ground(960, 956, 210))
    b.append(box_faces(bx, by, bw, bh, bd, "#fff", "#e3e3e3", "#eee"))
    lx, ly, lw, lh, ld = 749, 852, 320, 46, 190
    b.append(box_faces(lx, ly, lw, lh, ld))
    b.append(banner(lx + lw / 2, ly + lh / 2, (lh - 2) * LOCKUP_AR, "maroon"))

    # teacup + tea bag with logo tag
    cx, rim = 1290, 852
    b.append(ground(cx, 968, 125, 15))
    b.append(f'<ellipse cx="{cx}" cy="952" rx="112" ry="22" fill="#fff" stroke="{INK}" stroke-width="2"/>')
    b.append(f'<ellipse cx="{cx}" cy="948" rx="58" ry="10" fill="#f0f0f0" stroke="#a5a5a5" stroke-width="1.3"/>')
    b.append(path(f"M{cx+70},868C{cx+128},856 {cx+130},932 {cx+52},924L{cx+56},910C{cx+108},916 {cx+104},874 {cx+72},882Z"))
    b.append(f'<ellipse cx="{cx}" cy="{rim}" rx="76" ry="15" fill="#fff" stroke="{INK}" stroke-width="2"/>')
    b.append(f'<ellipse cx="{cx}" cy="{rim+2}" rx="68" ry="11" fill="#e4e0d8"/>')
    b.append(path(f"M{cx-14},{rim+4}L{cx+2},{rim-22}L{cx+20},{rim+3}Z", "#f7f3ea", INK, 1.5))
    b.append(path(f"M{cx-76},{rim}C{cx-76},{rim+58} {cx-48},{rim+90} {cx},{rim+92}C{cx+48},{rim+90} {cx+76},{rim+58} {cx+76},{rim}"
                  f"A76,15 0 0 1 {cx-76},{rim}Z", "url(#cyl)"))
    b.append(path(f"M{cx+2},{rim-22}Q{cx-60},{rim-26} {cx-74},{rim-2}Q{cx-84},{rim+16} {cx-92},{rim+34}", "none", INK, 1.4))
    b.append(f'<g transform="translate({cx-94},{rim+66}) rotate(7)">'
             f'<rect x="-23" y="-30" width="46" height="58" rx="3" fill="#fff" stroke="{INK}" stroke-width="1.8"/>'
             f'<circle cy="-22" r="2.6" fill="none" stroke="{INK}" stroke-width="1.2"/>'
             + logo(0, 5, 38, C["olive"]) + "</g>")

    # tea sachet envelope
    sx, sy = 1480, 850
    b.append(ground(sx + 4, 942, 68))
    g = [f'<rect x="-60" y="-84" width="120" height="168" rx="3" fill="#fff" stroke="{INK}" stroke-width="2"/>',
         f'<rect x="-60" y="-84" width="120" height="16" fill="#f1f1f1" stroke="{INK}" stroke-width="2"/>',
         f'<rect x="-60" y="70" width="120" height="14" fill="#f1f1f1" stroke="{INK}" stroke-width="2"/>',
         logo(0, 2, 96, C["maroon"])]
    b.append(f'<g transform="translate({sx},{sy}) rotate(-5)">' + "".join(g) + "</g>")

    return page("01", "土城區公所標誌加入包裝設計", "應用參考", "".join(b),
                "示意圖．標誌依原稿色彩直接置入：橄欖綠 #80843C、磚紅 #9A3439。")


# ============================================================ PAGE 2 ======
# Flat packaging in the original colours with the logo / original banners.
def page2():
    b = []
    # T-shirt bag – maroon + cream logo
    d = "M140,590V250Q140,238 152,238H196V296Q250,336 304,296V238H348Q360,238 360,250V590Z"
    b.append(flat(d, [R(130, 230, 240, 370, C["maroon"]), logo(250, 450, 168, C["cream"])]))

    # bottle – olive + sage logo
    d = ("M452,292H488V322H452Z M454,322H486V398C486,430 526,448 526,486V950Q526,975 500,975H440"
         "Q414,975 414,950V486C414,448 454,430 454,398Z")
    b.append(flat(d, [R(400, 280, 140, 710, C["olive"]), R(400, 280, 140, 44, "#5c5f2a"),
                      logo(470, 690, 88, C["sage"]), R(400, 280, 140, 710, "url(#gloss)")]))

    # paper bag with handles – cream bag, maroon banner pasted
    b.append(path("M676,252C676,150 812,150 812,252", "none", "#888", 2))
    b.append(path("M688,252C688,166 800,166 800,252", "none", "#888", 2))
    b.append(flat("M598,252H894V622H598Z", [R(598, 252, 296, 370, C["cream"]), banner(746, 437, 262, "maroon")],
                  outline="#e3dbc6"))

    # die-cut bag – sage bag, olive banner pasted
    b.append(flat("M968,252H1238V600H968Z", [R(968, 252, 270, 348, C["sage"]), banner(1103, 452, 240, "olive")]))
    b.append('<rect x="1068" y="276" width="70" height="24" rx="12" fill="#fff"/>')

    # canvas tote – olive + sage logo
    b.append(path("M1328,330C1326,214 1390,214 1392,330", "none", "#d9cfb7", 11))
    b.append(path("M1412,330C1410,214 1474,214 1476,330", "none", "#d9cfb7", 11))
    b.append(flat("M1302,326H1502V578H1302Z", [R(1302, 326, 200, 252, C["olive"]), logo(1402, 452, 150, C["sage"])]))

    # can – sage + olive logo
    d = ("M158,664Q162,648 178,648H282Q298,648 302,664L320,694V944L304,972Q298,986 282,986H178"
         "Q162,986 156,972L140,944V694Z")
    b.append(flat(d, [R(130, 640, 200, 360, C["sage"]), R(130, 640, 200, 46, "#d8d8d8"), R(130, 958, 200, 40, "#d8d8d8"),
                      logo(230, 818, 150, C["olive"]), R(130, 640, 200, 360, "url(#gloss)")]))

    # coffee cup – maroon + cream logo
    cx = 680
    b.append(flat(f"M{cx-70},726H{cx+70}L{cx+54},986H{cx-54}Z",
                  [R(cx - 80, 720, 160, 270, C["maroon"]), logo(cx, 850, 104, C["cream"]),
                   R(cx - 80, 720, 160, 270, "url(#gloss)")]))
    b.append(f'<path d="M{cx-66},710H{cx+66}L{cx+74},728H{cx-74}Z" fill="#fff" stroke="#999" stroke-width="1.6" stroke-linejoin="round"/>')

    # round lid / coaster – cream + maroon logo
    cx, cy = 960, 856
    d = f"M{cx-122},{cy}A122,122 0 1 0 {cx+122},{cy}A122,122 0 1 0 {cx-122},{cy}Z"
    b.append(flat(d, [R(cx - 130, cy - 130, 260, 260, C["cream"]), logo(cx, cy, 168, C["maroon"])], outline="#e3dbc6"))

    # snack pouch – olive pouch, cream banner pasted
    x1, x2 = 1190, 1420
    zt = zigzag(x1, x2, 652, 8, 6)
    zb = zigzag(x1, x2, 990, 8, -6)
    d = f"M{zt}L{x2},990" + "L" + "L".join(reversed(zb.split("L"))) + "Z"
    b.append(flat(d, [R(x1 - 10, 640, x2 - x1 + 20, 360, C["olive"]),
                      R(x1 - 10, 640, x2 - x1 + 20, 30, "#000", 0, 'opacity=".12"'),
                      R(x1 - 10, 962, x2 - x1 + 20, 40, "#000", 0, 'opacity=".12"'),
                      banner((x1 + x2) / 2, 816, 206, "cream"),
                      R(x1 - 10, 640, x2 - x1 + 20, 360, "url(#gloss)")]))

    return page("02", "土城區公所標誌加入包裝設計", "應用參考．以原稿標準色作為包材底色，直接置入標誌或橫式標準組合。",
                "".join(b), "示意圖．標準組合依原稿四種配色：青瓷綠／橄欖綠、橄欖綠／青瓷綠、米白／磚紅、磚紅／米白。")


# ============================================================ PAGE 3 ======
# Tea bags & pastry gift boxes.
def oblique_box(x, y, w, h, d, front_layers, top_layers, side_layers, shadow=True):
    ox, oy = d * DX, d * DY
    out = []
    if shadow:
        out.append(f'<path d="M{f(x+14)},{f(y+h+14)}H{f(x+w+14)}l{f(ox)},{f(oy)}V{f(y+oy+14)}'
                   f'H{f(x+w+ox)}V{f(y+h+oy)}Z" fill="{SHADOW}"/>')
    top = f"M{f(x)},{f(y)}L{f(x+ox)},{f(y+oy)}H{f(x+w+ox)}L{f(x+w)},{f(y)}Z"
    side = f"M{f(x+w)},{f(y)}L{f(x+w+ox)},{f(y+oy)}V{f(y+h+oy)}L{f(x+w)},{f(y+h)}Z"
    out.append(flat(top, top_layers, shadow=False))
    out.append(flat(side, side_layers, shadow=False))
    out.append(flat(f"M{f(x)},{f(y)}H{f(x+w)}V{f(y+h)}H{f(x)}Z", front_layers, shadow=False))
    return "".join(out)


def page3():
    b = []
    b.append(text(130, 222, "茶包系列", 18, C["olive"], 700, "start"))
    b.append(text(212, 223, "TEA BAG", 12, "#999", 500, "start", 'letter-spacing="2"'))
    b.append('<path d="M130,240H790" stroke="#ddd" stroke-width="1.2"/>')
    b.append(text(850, 222, "糕餅禮盒系列", 18, C["maroon"], 700, "start"))
    b.append(text(968, 223, "PASTRY GIFT BOX", 12, "#999", 500, "start", 'letter-spacing="2"'))
    b.append('<path d="M850,240H1500" stroke="#ddd" stroke-width="1.2"/>')

    # ---- tea box: olive, sage logo
    x, y, w, h, d = 134, 320, 250, 210, 100
    big = R(0, 0, 2000, 2000, C["olive"])
    b.append(oblique_box(x, y, w, h, d,
                         [big, logo(x + w / 2, y + h / 2, 146, C["sage"])],
                         [big, R(0, 0, 2000, 2000, "#000", 0, 'opacity=".10"')],
                         [big, R(0, 0, 2000, 2000, "#000", 0, 'opacity=".22"')]))

    # ---- three tea sachets
    def sachet(bg, ink):
        layers = [R(-62, -86, 124, 172, bg), logo(0, 0, 100, ink),
                  R(-62, -86, 124, 18, "#000", 0, 'opacity=".12"'),
                  R(-62, 68, 124, 18, "#000", 0, 'opacity=".12"')]
        return flat("M-56,-80H56V80H-56Z", layers, outline="#e3dbc6" if bg == C["cream"] else None)
    for bg, ink, rot, tx in ((C["maroon"], C["cream"], -9, 525), (C["cream"], C["maroon"], 0, 638), (C["sage"], C["olive"], 9, 751)):
        b.append(f'<g transform="translate({tx},{410 + abs(rot)*1.5}) rotate({rot})">' + sachet(bg, ink) + "</g>")

    # ---- teacup with tea bag & logo-shaped tag
    cx, top = 290, 730
    cup = (f"M{cx-108},{top}H{cx+108}V{top+120}C{cx+108},{top+168} {cx+72},{top+196} {cx+28},{top+196}"
           f"H{cx-28}C{cx-72},{top+196} {cx-108},{top+168} {cx-108},{top+120}Z")
    hd = f"M{cx+104},{top+30}C{cx+168},{top+26} {cx+168},{top+120} {cx+92},{top+130}"
    b.append(path(hd, "none", SHADOW, 15, 'transform="translate(13,13)"'))
    b.append(path(hd, "none", "#c9c9c9", 15))
    b.append(path(hd, "none", "#fff", 11))
    b.append(flat(cup, [R(cx - 120, top - 10, 240, 220, "#fff"), R(cx - 120, top + 26, 240, 200, "#eadfc6"),
                        R(cx - 120, top + 26, 240, 5, "#d9c9a2"), R(cx - 120, top - 10, 240, 220, "url(#gloss)")],
                  outline="#c9c9c9"))
    b.append(path(f"M{cx+30},{top+70}L{cx+56},{top+24}L{cx+82},{top+70}Z", "#f6f0e2", "#c9b98f", 1.5))
    b.append(path(f"M{cx+56},{top+24}C{cx+40},{top-40} {cx-90},{top-50} {cx-112},{top+2}"
                  f"C{cx-122},{top+24} {cx-128},{top+44} {cx-130},{top+62}", "none", "#8c8c8c", 1.5))
    b.append(sil(cx - 123, top + 116, 92, SHADOW))
    b.append(sil(cx - 131, top + 108, 92, "#fff"))
    b.append(logo(cx - 131, top + 108, 92, C["maroon"]))

    # ---- pyramid tea bags with logo die-cut tags
    def pyramid(px, py, s, tag_bg, tag_ink, tx, ty):
        tri = f"M{px},{py-48*s}L{px+50*s},{py+38*s}H{px-50*s}Z"
        return "".join([
            path(tri, SHADOW, None, 0, 'transform="translate(10,10)"'),
            path(tri, "#f7f2e8", "#b9ad94", 1.4),
            path(f"M{px},{py-48*s}L{px+8*s},{py+38*s}", "none", "#d8ccb2", 1.2),
            path(f"M{px},{py-48*s}C{px+10},{py-110*s} {tx-40},{ty-90} {tx},{ty-34}", "none", "#8c8c8c", 1.4),
            sil(tx + 8, ty + 8, 72, SHADOW),
            sil(tx, ty, 72, tag_bg),
            logo(tx, ty, 72, tag_ink)])
    b.append(pyramid(545, 880, 1.05, C["cream"], C["olive"], 620, 730))
    b.append(pyramid(690, 895, .9, C["cream"], C["maroon"], 752, 760))

    # ---- closed pastry gift box: maroon, cream banner on the lid band, logo on lid top
    x, y, w, h, d = 868, 372, 330, 58, 160
    ox, oy = d * DX, d * DY
    b.append(f'<path d="M{x+14},{y+h+72}H{x+w+14}l{f(ox)},{f(oy)}V{f(y+oy+14)}H{f(x+w+ox)}V{f(y+h+oy)}Z" fill="{SHADOW}"/>')
    b.append(R(x + 6, y + h, w - 12, 58, "#7d2a2e"))
    b.append(poly([(x + w - 6, y + h), (x + w - 6 + ox, y + h + oy), (x + w - 6 + ox, y + h + oy + 58), (x + w - 6, y + h + 58)],
                  "#5e1f22", None))
    big = R(0, 0, 2000, 2000, C["maroon"])
    b.append(oblique_box(x, y, w, h, d,
                         [big, banner(x + w / 2, y + h / 2, (h - 10) * LOCKUP_AR, "cream")],
                         [big, R(0, 0, 2000, 2000, "#fff", 0, 'opacity=".06"')],
                         [big, R(0, 0, 2000, 2000, "#000", 0, 'opacity=".25"')], shadow=False))

    # ---- gift carrier bag: olive, sage logo
    b.append(path("M1372,310C1372,232 1448,232 1448,310", "none", "#8a8a8a", 2))
    b.append(path("M1384,310C1384,244 1436,244 1436,310", "none", "#8a8a8a", 2))
    b.append(flat("M1316,306H1504V530H1316Z", [R(1316, 306, 188, 224, C["olive"]), logo(1410, 418, 140, C["sage"])]))

    # ---- open gift box (top view) with 4 x 2 wrapped pastries
    x, y, w, h = 862, 600, 356, 272
    b.append(flat(f"M{x},{y}H{x+w}V{y+h}H{x}Z", [R(x, y, w, h, C["maroon"])]))
    b.append(R(x + 16, y + 16, w - 32, h - 32, "#f3e9d2"))
    b.append(R(x + 16, y + 16, w - 32, h - 32, "none", 0, 'stroke="#6f2529" stroke-width="2"'))
    gap = 8
    cw, ch = (w - 32 - 5 * gap) / 4, (h - 32 - 3 * gap) / 2
    order = ["sage", "cream", "olive", "maroon", "maroon", "olive", "cream", "sage"]
    for k, var in enumerate(order):
        r_, c_ = divmod(k, 4)
        px = x + 16 + gap + c_ * (cw + gap)
        py = y + 16 + gap + r_ * (ch + gap)
        b.append(R(px, py, cw, ch, C["cream"], 3, 'stroke="#e2d3b1" stroke-width="1.5"'))
        pw, ph = cw - 14, ch - 14
        pcx, pcy = px + cw / 2, py + ch / 2
        d_ = f"M{f(pcx-pw/2)},{f(pcy-ph/2)}H{f(pcx+pw/2)}V{f(pcy+ph/2)}H{f(pcx-pw/2)}Z"
        b.append(flat(d_, [R(pcx - pw / 2, pcy - ph / 2, pw, ph, C[var]),
                           logo(pcx, pcy, 50, BANNER_INK[var]),
                           R(pcx - pw / 2, pcy - ph / 2, pw, 11, "#000", 0, 'opacity=".12"'),
                           R(pcx - pw / 2, pcy + ph / 2 - 11, pw, 11, "#000", 0, 'opacity=".12"')],
                      shadow=False, outline="#e3d6b8" if var == "cream" else None))

    # ---- single wrapped pastries (pillow packs) with the original banners
    def pillow(x, y, w, h, var):
        zl = "M" + "L".join(f"{f(x + (6 if i % 2 else 0))},{f(y + i*h/14)}" for i in range(15))
        zr = "L".join(f"{f(x + w - (6 if i % 2 else 0))},{f(y + h - i*h/14)}" for i in range(15))
        return flat(zl + "L" + zr + "Z",
                    [R(x, y, w, h, C[var]), banner(x + w / 2, y + h / 2, w - 52, var),
                     R(x, y, 22, h, "#000", 0, 'opacity=".10"'), R(x + w - 22, y, 22, h, "#000", 0, 'opacity=".10"'),
                     R(x, y, w, h, "url(#gloss)")],
                    outline="#e3dbc6" if var == "cream" else None)
    b.append(pillow(1270, 600, 236, 100, "sage"))
    b.append(pillow(1270, 736, 236, 100, "cream"))
    b.append(pillow(1270, 872, 236, 100, "maroon"))

    return page("03", "茶包與糕餅禮盒包裝設計", "應用參考．茶盒、茶包、立體茶包吊牌、糕餅禮盒、單顆包裝與禮盒提袋。",
                "".join(b), "示意圖．標誌與標準組合依原稿直接置入；茶包吊牌可依標誌外形軋型。")


if __name__ == "__main__":
    os.makedirs(HTML_DIR, exist_ok=True)
    for name, fn in (("page-01-logo", page1), ("page-02-packaging", page2), ("page-03-tea-pastry", page3)):
        _clip[0] = 0
        html = fn()
        open(os.path.join(HTML_DIR, name + ".html"), "w", encoding="utf-8").write(html)
        print("wrote", name, len(html) // 1024, "KB")
