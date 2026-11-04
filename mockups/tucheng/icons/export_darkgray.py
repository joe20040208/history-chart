"""Dark-grey, transparent, text-free export of the icon set.

    python3 export_darkgray.py   # writes darkgray/svg, darkgray/sheet.html
    node render_darkgray.js      # writes darkgray/png, sheet PNG, PDF/AI
"""
import os
import icons

COLOR = "#333333"   # 深灰 (K80)
OUT = os.path.join(icons.HERE, "darkgray")

# sheet: 7 x 4 grid, icons only, no background
COLS, CELL, ICON, PAD = 7, 200, 144, 40
ROWS = -(-len(icons.ICONS) // COLS)
W, H = COLS * CELL + 2 * PAD, ROWS * CELL + 2 * PAD


def main():
    svg_dir = os.path.join(OUT, "svg")
    os.makedirs(svg_dir, exist_ok=True)
    for old in os.listdir(svg_dir):
        os.remove(os.path.join(svg_dir, old))
    names = []
    cells = []
    for i, (slug, zh, en, fn) in enumerate(icons.ICONS):
        body = icons.prims_to_svg(fn(), COLOR)
        name = f"{i+1:02d}-{slug}"
        names.append(name)
        open(os.path.join(svg_dir, name + ".svg"), "w", encoding="utf-8").write(
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 96 96" width="96" height="96">{body}</svg>\n')
        r, c = divmod(i, COLS)
        s = ICON / 96
        x = PAD + c * CELL + (CELL - ICON) / 2
        y = PAD + r * CELL + (CELL - ICON) / 2
        cells.append(f'<g id="{slug}" transform="translate({icons.f(x)},{icons.f(y)}) scale({icons.f(s)})">{body}</g>')
    sheet = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}">'
             + "".join(cells) + "</svg>")
    open(os.path.join(OUT, "tucheng-icons-darkgray.svg"), "w", encoding="utf-8").write(sheet)
    open(os.path.join(OUT, "sheet.html"), "w", encoding="utf-8").write(
        f"<!doctype html><html><head><meta charset='utf-8'><style>@page{{size:{W}px {H}px;margin:0}}"
        "html,body{margin:0;background:transparent}svg{display:block}</style></head><body>" + sheet + "</body></html>")
    open(os.path.join(OUT, "names.txt"), "w").write("\n".join(names) + "\n")
    print(len(names), "icons", W, "x", H)


if __name__ == "__main__":
    main()
