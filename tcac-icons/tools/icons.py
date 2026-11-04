"""Pictogram definitions for the Taoyuan Children's Art Center icon set.

Every icon lives in a 120 x 120 box. Pictogram strokes use round caps/joins
like the wordmark. Colours are injected per colourway:
  fg   - pictogram colour
  ring - frame colour (info / rule / prohibit / warn differ in the colour set)
Masks (not white fills) are used for gaps so every file stays transparent.
"""

SW = 7  # base stroke width, matches the weight of the wordmark at icon size

BLUE = "#7C9DC6"   # sampled from the logo
INK = "#231815"    # sampled from the English line of the logo
CORAL = "#E9736B"  # prohibition
AMBER = "#F2B23E"  # caution

COLORWAYS = {
    "color": {"fg": BLUE, "info": BLUE, "rule": BLUE, "prohibit": CORAL, "warn": AMBER, "accent": CORAL},
    "blue":  {"fg": BLUE, "info": BLUE, "rule": BLUE, "prohibit": BLUE, "warn": BLUE, "accent": BLUE},
    "white": {"fg": "#FFFFFF", "info": "#FFFFFF", "rule": "#FFFFFF", "prohibit": "#FFFFFF", "warn": "#FFFFFF", "accent": "#FFFFFF"},
}


def eyes(cx, cy, gap=6.5, r=2.6, fill="{fg}"):
    return (f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{fill}" stroke="none"/>'
            f'<circle cx="{cx + gap}" cy="{cy}" r="{r}" fill="{fill}" stroke="none"/>')


def heart(cx, cy, s=1.0):
    # heart drawn as a stroked outline, s scales it
    return (f'<path transform="translate({cx} {cy}) scale({s})" '
            'd="M0 9 C-4 5 -11 1 -11 -5 C-11 -10 -7 -13 -3.5 -13 C-1.5 -13 0 -11.5 0 -9.5 '
            'C0 -11.5 1.5 -13 3.5 -13 C7 -13 11 -10 11 -5 C11 1 4 5 0 9 Z"/>')


def rounded_poly(pts, r):
    """Closed polygon path with rounded corners (quadratic corner curves)."""
    import math
    n = len(pts)
    d = []
    for i in range(n):
        p0, p1, p2 = pts[i - 1], pts[i], pts[(i + 1) % n]
        def toward(a, b, dist):
            dx, dy = b[0] - a[0], b[1] - a[1]
            L = math.hypot(dx, dy)
            return (a[0] + dx / L * dist, a[1] + dy / L * dist)
        a = toward(p1, p0, r)
        b = toward(p1, p2, r)
        d.append(("M" if i == 0 else "L") + f"{a[0]:.2f} {a[1]:.2f} Q{p1[0]} {p1[1]} {b[0]:.2f} {b[1]:.2f}")
    return " ".join(d) + " Z"


# ---------------------------------------------------------------- frames
FRAMES = {
    "info": lambda: f'<rect x="7" y="7" width="106" height="106" rx="27" stroke="{{info}}"/>',
    "rule": lambda: f'<circle cx="60" cy="60" r="53" stroke="{{rule}}"/>',
    "prohibit": lambda: f'<circle cx="60" cy="60" r="53" stroke="{{prohibit}}"/>',
    "warn": lambda: f'<path d="{rounded_poly([(60, 9), (114, 104), (6, 104)], 13)}" stroke="{{warn}}"/>',
}
# slash for prohibition, on the 45 degree diagonal
SLASH = (22.5, 22.5, 97.5, 97.5)


# ---------------------------------------------------------------- pictograms
def umbrella_one():
    # one furled umbrella, tip down, handle (J hook) up, in local coords
    return ('<path d="M0 -15 V-35 A6.5 6.5 0 0 0 -13 -35 V-32"/>'
            '<path d="M0 -17 C5 -8 6.5 4 6 13 M0 -17 C-5 -8 -6.5 4 -6 13"/>')


ICONS = {}


def icon(key, zh, en, frame):
    def deco(fn):
        ICONS[key] = {"zh": zh, "en": en, "frame": frame, "draw": fn}
        return fn
    return deco


@icon("umbrella-stand", "雨傘架", "Umbrella Stand", "info")
def _():
    return (
        '<path d="M30 72 H90"/>'
        '<path d="M36 72 L40.5 95 Q41.5 101 47.5 101 H72.5 Q78.5 101 79.5 95 L84 72"/>'
        f'<g transform="translate(45 59) rotate(-7)">{umbrella_one()}</g>'
        f'<g transform="translate(75 59) scale(-1 1) rotate(-7)">{umbrella_one()}</g>'
    )


@icon("stroller-parking", "嬰兒車放置處", "Stroller Parking", "info")
def _():
    return (
        '<path d="M28 55 A25 25 0 0 1 53 30 V55 Z"/>'
        '<path d="M28 63 H88 A30 20 0 0 1 28 63 Z"/>'
        '<path d="M88 63 L95 45 C96.5 41 99.5 39.5 103 40.5"/>'
        '<path d="M50 81.5 L45.5 88 M68 81.5 L73.5 88"/>'
        '<circle cx="43" cy="95" r="7"/>'
        '<circle cx="76" cy="95" r="7"/>'
    )


@icon("volunteer-seat", "志工專用座位", "Volunteer Seating", "info")
def _():
    return (
        '<path d="M30 44 V100 M30 80 H70 M66 80 V100"/>'
        '<circle cx="47" cy="29" r="10"/>' + eyes(48.5, 30.5, gap=5.5, r=2.1) +
        '<path d="M46 46 L44 68 H70 L74 96" stroke-width="10"/>'
        '<path d="M48 52 L62 44 L66 32" stroke-width="7"/>'
        '<g fill="{accent}" stroke="none">' + heart(80, 28, 1.05) + '</g>'
    )


@icon("nursing-room", "哺乳室", "Nursing Room", "info")
def _():
    # parent (left) hugging a baby (right) - heads use the mascot's dot eyes
    baby_cut = '<circle cx="72" cy="59" r="13.5"/><rect x="60" y="68" width="24" height="28" rx="11" stroke-width="9"/>'
    arm = '<path d="M44 84 C52 96 74 98 94 86"/>'
    arm_cut = '<path d="M44 84 C52 96 74 98 94 86" stroke-width="16"/>'
    return [
        ('<circle cx="50" cy="29" r="12"/>' + eyes(52.4, 32.5, gap=5.6, r=2.2) +
         '<path d="M20 104 V90 C20 67 34 53 50 53 C58 53 64 56 68 60"/>', baby_cut + arm_cut),
        ('<rect x="60" y="68" width="24" height="28" rx="11"/>', '<circle cx="72" cy="59" r="13.5"/>' + arm_cut),
        ('<circle cx="72" cy="59" r="9.5"/>' + eyes(68, 61, gap=4.8, r=1.9), None),
        (arm, None),
    ]


@icon("pantry", "茶水間", "Pantry", "info")
def _():
    return (
        '<path d="M28 56 H80 V66 C80 82 70 91 54 91 C38 91 28 82 28 66 Z"/>'
        '<path d="M80 61 H84 A9.5 9.5 0 0 1 84 80 H78"/>'
        '<path d="M22 101 H90"/>'
        '<path d="M44 45 C38 39 48 34 44 27 C42 23 45 19 49 20.5"/>'
        '<path d="M62 45 C56 39 66 34 62 27 C60 23 63 19 67 20.5"/>'
    )


@icon("snake-warning", "有蛇出沒", "Caution: Snakes", "warn")
def _():
    return [
        ('<path d="M27 93 C31.5 93 33 89 35 86 C37.5 82.5 40 80.5 43 80.5 C50.5 80.5 51 94 58.5 94 C67 94 70 86 69 78 C68.5 72.5 65 68.5 64 63" stroke-width="8.5"/>', None),
        ('<ellipse cx="66" cy="57" rx="9.5" ry="7.5" transform="rotate(-18 66 57)" fill="{fg}" stroke="none"/>',
         '<circle cx="68.4" cy="54" r="2.3"/>'),
        ('<path d="M74.8 55.2 L79 53.8 M79 53.8 L81.6 50.6 M79 53.8 L82.6 56" stroke-width="2.8" stroke="{accent}"/>', None),
    ]


@icon("no-crossing", "禁止跨越", "Do Not Cross", "prohibit")
def _():
    return (
        # stanchions + rope
        '<path d="M32 70 V97 M88 70 V97 M26 98 H38 M82 98 H94"/>'
        '<circle cx="32" cy="66" r="3.5" fill="{fg}"/><circle cx="88" cy="66" r="3.5" fill="{fg}"/>'
        '<path d="M32 72 Q60 90 88 72"/>'
        # arrow going over the rope
        '<path d="M30 56 C34 26 82 24 88 52"/>'
        '<path d="M79.5 49 L88.5 54.5 L93.5 45"/>'
    )


@icon("pets-carried", "寵物不落地", "Pets Must Be Carried", "rule")
def _():
    head = '<circle cx="60" cy="55" r="13"/>'
    ears = ('<path d="M50 46 C40 43 35 53 38 62 C40 67 46 66 47 60 Z" fill="{fg}"/>'
            '<path d="M70 46 C80 43 85 53 82 62 C80 67 74 66 73 60 Z" fill="{fg}"/>')
    cut = ('<circle cx="60" cy="55" r="17"/>'
           '<path d="M50 46 C40 43 35 53 38 62 C40 67 46 66 47 60 Z" stroke-width="10"/>'
           '<path d="M70 46 C80 43 85 53 82 62 C80 67 74 66 73 60 Z" stroke-width="10"/>')
    return [
        ('<path d="M31 72 C29 12 91 12 89 72"/>', cut),
        ('<path d="M28 73 H92 L88 90 Q86.5 96 80 96 H40 Q33.5 96 32 90 Z"/>', None),
        (head + ears + eyes(56.4, 54, gap=7.2, r=2.2) +
         '<ellipse cx="60" cy="60.5" rx="3" ry="2.3" fill="{fg}" stroke="none"/>'
         '<circle cx="60" cy="86.5" r="3.4" fill="{fg}" stroke="none"/>'
         '<circle cx="54.5" cy="81.5" r="1.8" fill="{fg}" stroke="none"/>'
         '<circle cx="60" cy="79.5" r="1.8" fill="{fg}" stroke="none"/>'
         '<circle cx="65.5" cy="81.5" r="1.8" fill="{fg}" stroke="none"/>', None),
    ]


@icon("no-food-drink", "禁止飲食", "No Food or Drink", "prohibit")
def _():
    return (
        # drink cup with straw (lower left)
        '<path d="M24 60 H48 M27 60 L30.5 92 Q31 96 35 96 H38 Q42 96 42.5 92 L46 60"/>'
        '<path d="M33 60 L30 46 H24"/>'
        # burger (upper right)
        '<path d="M65 41 C65 30 95 30 95 41 Z"/>'
        '<path d="M64 48.5 H96"/>'
        '<path d="M65 56 H95 V57 Q95 62 90 62 H70 Q65 62 65 57 Z"/>'
    )
