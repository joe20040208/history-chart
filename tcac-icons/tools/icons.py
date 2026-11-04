"""Pictogram definitions for the Taoyuan Children's Art Center icon set.

Every icon lives in a 120 x 120 box. Pictogram strokes use round caps/joins
like the wordmark. Colours are injected per colourway:
  fg     - pictogram colour
  slash  - prohibition slash
  accent - small highlights (heart, snake tongue)
The set is frameless and text-free; prohibitions are shown by a slash only.
Masks (not white fills) are used for gaps so every file stays transparent.
"""

SW = 7  # design stroke width on the 120 grid (all widths below are written against it)
STROKE_SCALE = 0.8  # final output thins every stroke: 7 -> 5.6

BLUE = "#7C9DC6"   # sampled from the logo
INK = "#231815"    # sampled from the English line of the logo
CORAL = "#E9736B"  # prohibition

COLORWAYS = {
    "color": {"fg": BLUE, "slash": CORAL, "accent": CORAL},
    "blue":  {"fg": BLUE, "slash": BLUE, "accent": BLUE},
    "white": {"fg": "#FFFFFF", "slash": "#FFFFFF", "accent": "#FFFFFF"},
}


def eyes(cx, cy, gap=6.5, r=2.6, fill="{fg}"):
    return (f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{fill}" stroke="none"/>'
            f'<circle cx="{cx + gap}" cy="{cy}" r="{r}" fill="{fill}" stroke="none"/>')


def heart(cx, cy, s=1.0):
    # heart drawn as a stroked outline, s scales it
    return (f'<path transform="translate({cx} {cy}) scale({s})" '
            'd="M0 9 C-4 5 -11 1 -11 -5 C-11 -10 -7 -13 -3.5 -13 C-1.5 -13 0 -11.5 0 -9.5 '
            'C0 -11.5 1.5 -13 3.5 -13 C7 -13 11 -10 11 -5 C11 1 4 5 0 9 Z"/>')


# prohibition slash on the 45 degree diagonal (no ring: the set is frameless)
SLASH = (21, 21, 99, 99)


# ---------------------------------------------------------------- pictograms
def umbrella_one():
    # one furled umbrella, tip down, handle (J hook) up, in local coords
    return ('<path d="M0 -15 V-35 A6.5 6.5 0 0 0 -13 -35 V-32"/>'
            '<path d="M0 -17 C5 -8 6.5 4 6 13 M0 -17 C-5 -8 -6.5 4 -6 13"/>')


ICONS = {}


def icon(key, zh, en, kind="info", xf=None):
    """kind: info | rule | warn | prohibit (prohibit adds the slash).
    xf: optional SVG transform applied to the whole pictogram."""
    def deco(fn):
        ICONS[key] = {"zh": zh, "en": en, "kind": kind, "xf": xf, "draw": fn}
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
        '<circle cx="47" cy="29" r="10"/>' + eyes(47.4, 30.5, gap=5.2, r=2.1) +
        '<path d="M46 46 L44 68 H70 L74 96" stroke-width="10"/>'
        '<path d="M48 52 L62 44 L66 32" stroke-width="7"/>'
        '<g fill="{accent}" stroke="none">' + heart(82, 27, 1.1) + '</g>'
    )


@icon("nursing-room", "哺乳室", "Nursing Room", "info")
def _():
    # parent (left) hugging a baby (right) - heads use the mascot's dot eyes
    baby_cut = '<circle cx="72" cy="59" r="13.5"/><rect x="60" y="68" width="24" height="28" rx="11" stroke-width="9"/>'
    arm = '<path d="M44 84 C52 96 74 98 94 86"/>'
    arm_cut = '<path d="M44 84 C52 96 74 98 94 86" stroke-width="16"/>'
    return [
        ('<circle cx="50" cy="29" r="12"/>' + eyes(51.2, 32.5, gap=5.4, r=2.2) +
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


@icon("snake-warning", "有蛇出沒", "Caution: Snakes", "warn", xf="translate(-22 -42) scale(1.42)")
def _():
    k = 1.42
    return [
        (f'<path d="M27 93 C31.5 93 33 89 35 86 C37.5 82.5 40 80.5 43 80.5 C50.5 80.5 51 94 58.5 94 '
         f'C67 94 70 86 69 78 C68.5 72.5 65 68.5 64 63" stroke-width="{10 / k:.2f}"/>', None),
        ('<ellipse cx="66" cy="57" rx="9.5" ry="7.5" transform="rotate(-18 66 57)" fill="{fg}" stroke="none"/>',
         '<circle cx="68.4" cy="54" r="2.3"/>'),
        (f'<path d="M74.8 55.2 L79 53.8 M79 53.8 L81.6 50.6 M79 53.8 L82.6 56" stroke-width="{4 / k:.2f}" stroke="{{accent}}"/>', None),
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


@icon("baby-changing", "尿布台", "Baby Changing", "info")
def _():
    return (
        '<rect x="16" y="70" width="88" height="13" rx="6.5"/>'
        '<path d="M30 83 V102 M90 83 V102"/>'
        # baby lying on its back, one arm and one leg up
        '<circle cx="31" cy="49" r="10"/>' + eyes(28.2, 47.5, gap=6, r=2.1) +
        '<rect x="46" y="44" width="30" height="16" rx="8"/>'
        '<path d="M66 44 V60" stroke-width="5"/>'
        '<path d="M53 44 L59 32"/>'
        '<path d="M76 50 L86 38 L96 46"/>'
    )


@icon("restroom", "洗手間", "Restrooms", "info")
def _():
    return (
        '<path d="M60 18 V102" stroke-width="5"/>'
        # left figure
        '<circle cx="36" cy="27" r="9.5"/>' + eyes(33, 28.5, gap=6, r=2) +
        '<rect x="25" y="44" width="22" height="30" rx="8"/>'
        '<path d="M30.5 74 V100 M41.5 74 V100"/>'
        # right figure
        '<circle cx="84" cy="27" r="9.5"/>' + eyes(81, 28.5, gap=6, r=2) +
        '<path d="M77 44 H91 Q93.5 44 94.5 47 L101 74 H67 L73.5 47 Q74.5 44 77 44 Z"/>'
        '<path d="M78.5 74 V100 M89.5 74 V100"/>'
    )


@icon("accessible", "無障礙設施", "Accessible Facilities", "info")
def _():
    return (
        '<circle cx="50" cy="23" r="9.5"/>' + eyes(50.6, 24.5, gap=4.8, r=1.9) +
        '<path d="M48 40 L46 63" stroke-width="10"/>'
        '<path d="M48 47 L64 50"/>'
        '<path d="M46 64 H70" stroke-width="9"/>'
        '<path d="M70 64 L78 87 H89"/>'
        '<path d="M32.6 68.6 A19 19 0 1 0 63.2 74"/>'
    )


@icon("lockers", "置物櫃", "Lockers", "info")
def _():
    dots = "".join(f'<circle cx="{x}" cy="{y}" r="3.4" fill="{{fg}}" stroke="none"/>'
                   for x, y in [(50, 46), (85, 46), (50, 88), (85, 88)])
    return (
        '<rect x="22" y="16" width="76" height="88" rx="10"/>'
        '<path d="M60 16 V104 M22 60 H98"/>'
        '<path d="M32 29 H46 M70 29 H84 M32 73 H46 M70 73 H84"/>' + dots
    )


@icon("info-desk", "服務台", "Information Desk", "info")
def _():
    return (
        '<circle cx="44" cy="31" r="11"/>' + eyes(44.6, 33, gap=5.8, r=2.2) +
        '<path d="M22 66 C22 54 32 49 44 49 C56 49 66 54 66 66"/>'
        '<rect x="14" y="70" width="92" height="30" rx="8"/>'
        # speech bubble: "ask here"
        '<path d="M72 16 H96 A8 8 0 0 1 104 24 V36 A8 8 0 0 1 96 44 H80 L70 51 L72 43 A8 8 0 0 1 64 35 V24 A8 8 0 0 1 72 16 Z"/>'
        + "".join(f'<circle cx="{x}" cy="30" r="2.8" fill="{{fg}}" stroke="none"/>' for x in (75, 84, 93))
    )


@icon("elevator", "電梯", "Elevator", "info")
def _():
    return (
        '<rect x="28" y="42" width="64" height="62" rx="8"/>'
        '<path d="M60 42 V104"/>'
        '<path d="M37 31 L46 20 L55 31 Z M65 20 L74 31 L83 20 Z" fill="{fg}" stroke-width="5"/>'
    )


@icon("no-touching", "請勿觸摸", "Please Do Not Touch", "prohibit")
def _():
    return (
        '<path d="M38 66 V40 A6.5 6.5 0 0 1 51 40 V60"/>'
        '<path d="M51 40 V31 A6.5 6.5 0 0 1 64 31 V60"/>'
        '<path d="M64 36 A6.5 6.5 0 0 1 77 36 V62"/>'
        '<path d="M77 44 A6.5 6.5 0 0 1 90 44 V72 C90 90 78 102 62 102 H58 C48 102 42 97 37 90 '
        'L24 71 A6 6 0 0 1 33 63 L38 68"/>'
    )


@icon("no-running", "禁止奔跑", "No Running", "prohibit")
def _():
    return (
        '<circle cx="71" cy="22" r="9.5"/>' + eyes(71.6, 23.5, gap=4.8, r=1.9) +
        '<path d="M65 38 L57 60" stroke-width="10"/>'
        '<path d="M57 61 L73 70 L70 91" stroke-width="9"/>'
        '<path d="M57 61 L46 77 L30 78" stroke-width="9"/>'
        '<path d="M64 43 L77 51 L86 44 M64 43 L50 48 L44 40"/>'
        '<path d="M18 46 H30 M14 58 H26" stroke-width="5"/>'
    )


@icon("no-flash", "禁止使用閃光燈", "No Flash Photography", "prohibit")
def _():
    return (
        '<rect x="16" y="44" width="80" height="54" rx="12"/>'
        '<path d="M38 44 L43 34 H69 L74 44"/>'
        '<circle cx="56" cy="71" r="15"/>'
        '<path d="M97 14 L87 29 H99 L89 43" stroke-width="6"/>'
    )


@icon("no-smoking", "禁止吸菸", "No Smoking", "prohibit")
def _():
    return (
        '<rect x="16" y="68" width="82" height="15" rx="4"/>'
        '<path d="M36 68 V83"/>'
        '<path d="M88 58 C82 52 92 47 88 40 C86 36 89 32 93 33.5"/>'
        '<path d="M76 58 C70 52 80 47 76 40 C74 36 77 32 81 33.5"/>'
    )


@icon("keep-quiet", "請保持安靜", "Please Keep Quiet", "rule")
def _():
    # solid hand (index finger over the lips) in front of an outlined face
    hand = ('<rect x="54" y="50" width="12" height="40" rx="6"/>'
            '<path d="M44 92 C44 84 48 80 56 80 H66 C73 80 77 84 77 91 C77 99 72 104 64 104 H56 C49 104 44 99 44 92 Z"/>')
    gap = hand.replace("/>", ' stroke-width="9"/>')
    return [
        ('<circle cx="60" cy="44" r="31"/>' + eyes(46, 37, gap=28, r=3.2)
         + '<path d="M46 61 H74"/>', gap),
        (f'<g fill="{{fg}}" stroke="none">{hand}</g>', None),
    ]

ORDER = [
    "umbrella-stand", "stroller-parking", "volunteer-seat", "nursing-room", "baby-changing",
    "restroom", "accessible", "pantry", "lockers", "info-desk",
    "elevator", "snake-warning", "pets-carried", "keep-quiet", "no-crossing",
    "no-food-drink", "no-touching", "no-running", "no-flash", "no-smoking",
]
ICONS = {k: ICONS[k] for k in ORDER}
