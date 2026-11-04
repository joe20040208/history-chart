"""Tucheng District Office wayfinding icons, drawn in the stroke language
of the 新北市土城區公所 wordmark.

Stroke rules taken from the wordmark
  * hairline monoline strokes (t) against bold vertical bars (T ~ 2.75 t)
    - bold bars usually sit on the right, as in 新 / 北 / 市 / 所
  * frames with one bold side, as in the small boxes inside 區
  * a pair of diagonals, one thin and one bold, that do not touch, as in 公
  * crossbars overshoot the verticals, as in 土 / 城
  * right-angle turns and hooks (市, 北); no curves; dots are small squares

All icons sit on a 96 x 96 grid (live area 8-88) and are pure strokes
(butt caps, miter joins) plus filled squares, so they stay editable in
Illustrator. Use Object > Expand to outline the strokes if needed.

    python3 icons.py          # writes svg/, sheet.html
    node render_icons.js      # renders sheet PNG + PDF/AI
"""
import math
import os
import re
import subprocess
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
t, T = 4.0, 11.0               # hairline / bold bar
INK = "#9A3439"                # 磚紅 (original wordmark colour)
TILE = "#FCF5E3"               # 米白


def f(v):
    return f"{v:.2f}".rstrip("0").rstrip(".")


# ------------------------------------------------------------ primitives ---
# ('l', [(x,y),...], width, closed)  stroke
# ('b', [(x,y),...])                 filled polygon (blocks / heads / dots)
def L(*pts, w=t, closed=False):
    return ("l", list(pts), w, closed)


def h(x1, x2, y, w=t):
    return L((x1, y), (x2, y), w=w)


def v(x, y1, y2, w=t):
    return L((x, y1), (x, y2), w=w)


def V(x, y1, y2):
    return v(x, y1, y2, T)


def B(x, y, w, hh):
    return ("b", [(x, y), (x + w, y), (x + w, y + hh), (x, y + hh)])


def octagon(cx, cy, r, start=None, end=None):
    """Flat-topped octagon points (circumradius r)."""
    pts = [(cx + r * math.cos(math.radians(22.5 + 45 * k)), cy + r * math.sin(math.radians(22.5 + 45 * k)))
           for k in range(8)]
    return pts


def transform(prims, s=1.0, rot=0.0, tx=0.0, ty=0.0, ox=0.0, oy=0.0, bold_scale=None):
    """Scale/rotate primitives about (ox, oy) then move to (tx, ty).
    Hairlines keep their width; bold bars shrink a little with the figure."""
    c, sn = math.cos(math.radians(rot)), math.sin(math.radians(rot))
    bs = bold_scale if bold_scale is not None else max(s, .72)

    def p(x, y):
        x, y = (x - ox) * s, (y - oy) * s
        return (x * c - y * sn + tx, x * sn + y * c + ty)
    out = []
    for pr in prims:
        if pr[0] == "l":
            w = pr[2] if pr[2] <= t else max(t + 1, pr[2] * bs)
            out.append(("l", [p(*q) for q in pr[1]], w, pr[3]))
        else:
            out.append(("b", [p(*q) for q in pr[1]]))
    return out


# --------------------------------------------------------------- figures ---
def person(cx=48, top=8, legs=88, arms=60):
    """Standing figure: square head, overshooting shoulder hairline,
    hairline arms, two bold legs."""
    return [B(cx - 6.5, top, 13, 13),
            h(cx - 24, cx + 24, top + 23),
            v(cx - 20, top + 23, arms), v(cx + 20, top + 23, arms),
            V(cx - 8, top + 23, legs), V(cx + 8, top + 23, legs)]


def woman(cx=48, top=8):
    sh = top + 23
    return [B(cx - 6.5, top, 13, 13),
            h(cx - 24, cx + 24, sh),
            v(cx - 21, sh, 56), v(cx + 21, sh, 56),
            L((cx - 5, sh + 2), (cx - 17, 68)),            # thin diagonal (公)
            L((cx + 6, sh + 2), (cx + 18, 66), w=T),       # bold diagonal (公)
            h(cx - 26, cx + 26, 68),                        # hem, overshooting
            v(cx - 7, 68, 88), v(cx + 7, 68, 88)]


def isa():
    """International Symbol of Access, squared off."""
    wheel = octagon(36, 64, 24)
    # open wheel: from the top edge (left of the torso) round to the right side
    pts = [(29, wheel[5][1]), wheel[5], wheel[4], wheel[3], wheel[2], wheel[1], wheel[0], (wheel[0][0], 65)]
    return [B(30, 8, 12, 12),
            V(38, 25, 60),
            h(43.5, 62, 34),
            L((43.5, 58), (66, 58), (72, 84), (84, 84)),
            L(*pts)]


def chevron(cx, cy, dx, dy, thick_right=True):
    """Two diagonals meeting near (cx, cy) without touching (公)."""
    g = 2.5
    left = L((cx - dx, cy + dy), (cx - g, cy + g * dy / dx))
    right = L((cx + g, cy + g * dy / dx), (cx + dx, cy + dy), w=T if thick_right else t)
    return [left, right]


# ----------------------------------------------------------------- icons ---
ICONS = []


def icon(slug, zh, en):
    def deco(fn):
        ICONS.append((slug, zh, en, fn))
        return fn
    return deco


@icon("restroom-men", "男洗手間", "Men's Restroom")
def _():
    return person()


@icon("restroom-women", "女洗手間", "Women's Restroom")
def _():
    return woman()


@icon("restroom-accessible", "無障礙洗手間", "Accessible Restroom")
def _():
    fig = transform(isa(), s=.8, tx=-6, ty=18, bold_scale=.85)
    # "WC" lettered in the wordmark's strokes: hairline W ending in a bold
    # diagonal (公), C as a 匚 frame with a bold upright and overshooting bars
    wc = [L((44, 8), (50, 36), (57, 14), (62, 30)), L((64.5, 30), (70.5, 8), w=T),
          V(80.5, 8, 36), h(75, 92, 10), h(75, 92, 34)]
    return fig + wc


@icon("accessible", "無障礙設施", "Accessibility")
def _():
    return isa()


@icon("ramp", "無障礙坡道", "Accessible Ramp")
def _():
    ang = math.degrees(math.atan2(-28, 82))
    fig = transform(isa(), s=.6, rot=ang, ox=36, oy=88, tx=40, ty=64.5)
    return [L((6, 84), (90, 56), w=T), h(4, 92, 88), v(88, 60, 88)] + fig


@icon("elevator-accessible", "無障礙電梯", "Accessible Elevator")
def _():
    frame = [h(8, 88, 10), h(8, 88, 86), v(12, 10, 86), V(82, 10, 86)]
    fig = transform(isa(), s=.56, tx=20, ty=34, bold_scale=.75)
    arrows = [L((56, 30), (64, 22), (72, 30)), L((56, 42), (64, 50), (72, 42))]
    return frame + fig + arrows


@icon("nursing-room", "哺集乳室", "Nursing Room")
def _():
    return [B(42, 8, 12, 10),
            h(28, 68, 22), h(32, 64, 28),
            v(36, 28, 88), V(59.5, 28, 88), h(34, 65, 86),
            h(36, 47, 42), h(36, 47, 54), h(36, 47, 66)]


@icon("drinking-water", "飲水機／補水站", "Drinking Water")
def _():
    return [h(16, 40, 10), v(28, 10, 18),
            V(28, 18, 36),
            L((33.5, 28), (56, 28), (56, 36)),
            v(56, 42, 50), v(56, 55, 59),
            L((38, 64), (42, 88)), L((74, 64), (70, 88), w=T), h(40, 72, 86),
            h(40, 70, 74)]


@icon("first-aid", "急救箱", "First Aid")
def _():
    return [L((34, 22), (34, 12), (62, 12), (62, 22)),
            h(8, 88, 24), h(8, 88, 86), v(12, 24, 86), V(82, 24, 86),
            V(44, 38, 74), L((28, 56), (60, 56), w=T)]


@icon("aed", "AED", "AED")
def _():
    heart = [(48, 86), (12, 50), (12, 28), (24, 16), (36, 16), (48, 28), (60, 16), (72, 16), (84, 28), (84, 50)]
    return [L(*heart, closed=True), L((84, 30), (84, 50), (51, 83), w=T),
            L((50, 32), (38, 52), (54, 52), (44, 70), w=6)]


@icon("emergency-exit", "緊急逃生口", "Emergency Exit")
def _():
    door = [h(52, 92, 10), V(60, 10, 88), v(88, 10, 88), h(56, 92, 88)]
    runner = [B(27, 10, 12, 12),
              L((35, 28), (24, 56), w=T),
              L((36, 34), (46, 42), (52, 36)),
              L((31, 32), (20, 36), (14, 46)),
              L((25, 54), (38, 64), (38, 86)),
              L((23, 56), (15, 72), (6, 72))]
    return door + runner


@icon("stairs", "樓梯／逃生梯", "Stairs")
def _():
    out = [h(6, 30, 86)]
    x, y = 28, 86
    for i in range(4):
        out.append(V(x - 3.5 + 5.5, y - 18, y))       # bold riser
        out.append(h(x - 4, x + 20, y - 18))          # hairline tread
        x, y = x + 18, y - 18
    out.append(L((10, 64), (68, 14)))                 # handrail
    return out


@icon("elevator", "電梯", "Elevator")
def _():
    frame = [h(8, 88, 10), h(8, 88, 86), v(12, 10, 86), V(82, 10, 86)]
    return frame + [L((21, 32), (32, 21), (43, 32)), V(32, 34, 78),
                    V(58, 18, 62), L((47, 64), (58, 75), (69, 64))]


@icon("no-smoking", "禁止吸菸", "No Smoking")
def _():
    o = octagon(48, 48, 41)
    return [L(*o, closed=True),
            h(18, 64, 50), h(18, 64, 62), v(20, 50, 62), V(70, 50, 62),
            L((70, 44), (66, 38), (70, 32), (66, 26)),
            L((22, 22), (74, 74), w=T)]


@icon("parking", "停車場", "Parking")
def _():
    return [V(35, 10, 88), h(24, 70, 12),
            L((67, 12), (67, 52), (40.5, 52)),
            h(24, 46, 86)]


@icon("wifi", "WiFi", "Wi-Fi")
def _():
    # arcs as the top quarter of an octagon round the dot; the inner one bold
    cx, cy = 48, 80
    out = [B(42, 74, 12, 12)]
    for r, w in ((22, T), (41, t), (60, t)):
        o = octagon(cx, cy, r)
        a, b = o[5], o[6]                       # flat top edge
        k = .55                                 # how far down the slanted edges
        pl = (a[0] + (o[4][0] - a[0]) * k, a[1] + (o[4][1] - a[1]) * k)
        pr = (b[0] + (o[7][0] - b[0]) * k, b[1] + (o[7][1] - b[1]) * k)
        out.append(L(pl, a, b, pr, w=w))
    return out


@icon("charging", "充電站", "Charging")
def _():
    return [h(8, 80, 28), h(8, 80, 70), v(12, 28, 70), V(74, 28, 70), B(80, 40, 8, 18),
            L((48, 34), (34, 50), (52, 50), (38, 66), w=7)]


@icon("trash", "垃圾桶", "Trash")
def _():
    return [L((38, 20), (38, 10), (58, 10), (58, 20)),
            h(10, 86, 22),
            v(22, 30, 88), V(70, 30, 88), h(20, 75.5, 86),
            v(37, 38, 78), v(52, 38, 78)]


@icon("recycling", "資源回收", "Recycling")
def _():
    A, Bp, Cp = (48, 10), (88, 80), (8, 80)

    def edge(p, q, w, gap=12):
        d = math.dist(p, q)
        ux, uy = (q[0] - p[0]) / d, (q[1] - p[1]) / d
        s = (p[0] + ux * gap, p[1] + uy * gap)
        e = (q[0] - ux * gap, q[1] - uy * gap)
        # arrow head: two short hairlines behind the end point
        nx, ny = -uy, ux
        head = L((e[0] - ux * 9 + nx * 8, e[1] - uy * 9 + ny * 8), e, (e[0] - ux * 9 - nx * 8, e[1] - uy * 9 - ny * 8))
        return [L(s, e, w=w), head]
    return edge(A, Bp, T) + edge(Bp, Cp, t) + edge(Cp, A, t)


@icon("printing", "影印／列印", "Printing")
def _():
    return [L((28, 32), (28, 10), (68, 10), (68, 32)),
            h(8, 88, 32), v(12, 32, 68), V(82, 32, 68), h(8, 88, 68),
            h(22, 74, 56),
            L((28, 56), (28, 88), (68, 88), (68, 56)),
            h(36, 60, 72), h(36, 52, 80)]


@icon("office-hours", "服務時間", "Office Hours")
def _():
    o = octagon(48, 48, 41)
    return [L(*o, closed=True), V(48, 20, 52), h(42.5, 72, 50),
            v(48, 76, 84), h(12, 20, 48), h(76, 84, 48)]


@icon("service-counter", "服務台", "Service Counter")
def _():
    return [B(41.5, 8, 13, 13), h(26, 70, 31), v(30, 31, 46), v(66, 31, 46),
            V(41, 31, 50), V(55, 31, 50),
            h(6, 90, 54), v(14, 54, 88), V(80, 54, 88)]


@icon("information", "詢問處", "Information")
def _():
    return [L((26, 22), (26, 10), (68, 10), (68, 48), (54, 48)),
            V(48, 48, 70), h(36, 54, 48),
            B(42, 76, 12, 12)]


@icon("queue-ticket", "抽號碼牌", "Queue Ticket")
def _():
    return [h(6, 90, 10), v(10, 10, 26), V(84.5, 10, 26), h(6, 90, 26),
            v(18, 26, 88), V(72, 26, 88), h(16, 77.5, 86),
            # "01" in the same stroke language
            h(26, 49, 42), h(26, 49, 74), v(28, 42, 74), V(43.5, 42, 74),
            h(50, 58, 44), V(58, 42, 74)]


@icon("waiting-area", "等候區", "Waiting Area")
def _():
    return [V(30, 10, 88), h(20, 72, 58), v(68, 58, 88),
            L((35.5, 40), (62, 40), (62, 58)),
            h(20, 40, 86)]


@icon("id-registration", "戶政／證件", "ID & Registration")
def _():
    return [h(6, 90, 20), h(6, 90, 76), v(10, 20, 76), V(84, 20, 76),
            B(20, 32, 10, 10), h(16, 38, 50), V(24, 50, 66),
            h(46, 72, 34), h(46, 72, 46), h(46, 64, 58)]


@icon("forms", "申請書表", "Forms")
def _():
    return [L((56, 10), (18, 10), (18, 88), (72, 88)),
            V(72, 26, 88), L((56, 10), (76, 30)),
            v(56, 10, 28), h(56, 70, 28),
            h(28, 62, 42), h(28, 62, 54), h(28, 62, 66), h(28, 48, 78)]


@icon("suggestion-box", "意見箱", "Suggestion Box")
def _():
    return [h(6, 90, 22), h(6, 90, 78), v(10, 22, 78), V(84, 22, 78),
            L((12, 26), (45, 52)), L((51, 52), (78.5, 30), w=T)]


# ------------------------------------------------------------------ output ---
def prims_to_svg(prims, color=INK):
    out = []
    for pr in prims:
        if pr[0] == "l":
            pts, w, closed = pr[1], pr[2], pr[3]
            d = "M" + "L".join(f"{f(x)},{f(y)}" for x, y in pts) + ("Z" if closed else "")
            out.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{f(w)}" '
                       f'stroke-linecap="butt" stroke-linejoin="miter" stroke-miterlimit="10"/>')
        else:
            pts = pr[1]
            out.append(f'<polygon points="{" ".join(f"{f(x)},{f(y)}" for x, y in pts)}" fill="{color}"/>')
    return "".join(out)


# ---- label text converted to outlines (no font needed in Illustrator) ----
_fonts = {}


def _font(weight, chars):
    from fontTools.ttLib import TTFont
    key = weight
    if key not in _fonts:
        cache = os.path.join(HERE, f".font-{weight}.ttf")
        if not os.path.exists(cache):
            q = urllib.parse.quote("".join(sorted(set(chars))))
            css = urllib.request.urlopen(
                f"https://fonts.googleapis.com/css2?family=Noto+Sans+TC:wght@{weight}&text={q}").read().decode()
            url = re.search(r"url\((https://[^)]+)\)", css).group(1)
            open(cache, "wb").write(urllib.request.urlopen(url).read())
        _fonts[key] = TTFont(cache)
    return _fonts[key]


def text_path(s, x, y, size, weight, anchor="middle", color="#333", charset=""):
    """Return <path> with text s outlined; y is the baseline."""
    from fontTools.pens.svgPathPen import SVGPathPen
    from fontTools.pens.transformPen import TransformPen
    font = _font(weight, charset)
    gs = font.getGlyphSet()
    cmap = font.getBestCmap()
    upm = font["head"].unitsPerEm
    sc = size / upm
    hmtx = font["hmtx"]
    names = [cmap[ord(ch)] for ch in s]
    width = sum(hmtx[n][0] for n in names) * sc
    x0 = x - (width / 2 if anchor == "middle" else 0)
    pen = SVGPathPen(gs)
    cx = x0
    for n in names:
        tp = TransformPen(pen, (sc, 0, 0, -sc, cx, y))
        gs[n].draw(tp)
        cx += hmtx[n][0] * sc
    return f'<path d="{pen.getCommands()}" fill="{color}"/>'


SHEET_W, SHEET_H = 1600, 1200
COLS, TILE_S, GAP_X = 7, 150, 46
ROW_H, TOP = 222, 214


def sheet_svg():
    title = "土城區公所 標準字風格圖示"
    sub = "依標準字筆劃特徵設計：細線與粗直筆對比、單邊加粗的框、一細一粗的斜筆、橫筆出頭、方塊點、無曲線．共 28 款"
    note = "96 × 96 格線．細筆 4．粗筆 11．線段皆為筆畫（可在 Illustrator 以「物件 > 擴充」轉外框）．標準色 #9A3439"
    labels = "".join(zh + en for _, zh, en, _ in ICONS)
    charset = title + sub + note + labels + "0123456789"
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {SHEET_W} {SHEET_H}" width="{SHEET_W}" height="{SHEET_H}">',
           f'<rect width="{SHEET_W}" height="{SHEET_H}" fill="#fff"/>',
           text_path(title, 110, 118, 30, 700, "start", "#333", charset),
           text_path(sub, 110, 162, 16, 500, "start", "#555", charset),
           text_path(note, 110, SHEET_H - 50, 12, 400, "start", "#888", charset)]
    x0 = (SHEET_W - (COLS * TILE_S + (COLS - 1) * GAP_X)) / 2
    for i, (slug, zh, en, fn) in enumerate(ICONS):
        r, c = divmod(i, COLS)
        x = x0 + c * (TILE_S + GAP_X)
        y = TOP + r * ROW_H
        s = 1.1
        off = (TILE_S - 96 * s) / 2
        out.append(f'<g id="tile-{slug}"><rect x="{f(x)}" y="{f(y)}" width="{TILE_S}" height="{TILE_S}" rx="10" fill="{TILE}"/></g>')
        out.append(f'<g id="{slug}" transform="translate({f(x + off)},{f(y + off)}) scale({s})">{prims_to_svg(fn())}</g>')
        out.append(f'<g id="label-{slug}">'
                   + text_path(f"{i+1:02d}  {zh}", x + TILE_S / 2, y + TILE_S + 28, 15, 700, "middle", "#333", charset)
                   + text_path(en, x + TILE_S / 2, y + TILE_S + 47, 11, 400, "middle", "#888", charset)
                   + "</g>")
    out.append("</svg>")
    return "".join(out)


if __name__ == "__main__":
    svg_dir = os.path.join(HERE, "svg")
    os.makedirs(svg_dir, exist_ok=True)
    for old in os.listdir(svg_dir):
        os.remove(os.path.join(svg_dir, old))
    for i, (slug, zh, en, fn) in enumerate(ICONS):
        body = prims_to_svg(fn())
        open(os.path.join(svg_dir, f"{i+1:02d}-{slug}.svg"), "w", encoding="utf-8").write(
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 96 96" width="96" height="96">'
            f"<title>{zh} {en}</title>{body}</svg>\n")
    sheet = sheet_svg()
    open(os.path.join(HERE, "tucheng-icons-sheet.svg"), "w", encoding="utf-8").write(sheet)
    open(os.path.join(HERE, "sheet.html"), "w", encoding="utf-8").write(
        "<!doctype html><html><head><meta charset='utf-8'><style>@page{size:1600px 1200px;margin:0}"
        "html,body{margin:0}svg{display:block}</style></head><body>" + sheet + "</body></html>")
    print(len(ICONS), "icons")
