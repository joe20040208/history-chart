# 新北市土城區公所 — 包裝應用模擬圖

These mock-up pages follow the style of the 臺南400 guideline pages. They use only the original artwork: the logo and the horizontal banner (logo + wordmark) in its four original colour combinations, applied as-is. No extra patterns or decoration are added.

| 頁面 | 內容 |
| --- | --- |
| `output/page-01-logo.png` | 標誌直接置入白色包材（線稿）：咖啡杯、立體夾鏈袋、紙提袋、牛奶盒、方盒、糕餅禮盒、茶杯＋茶包吊牌、茶包袋 |
| `output/page-02-packaging.png` | 標準色底色包材＋標誌／橫式組合：背心袋、瓶身、紙提袋、手提袋、帆布袋、鋁罐、紙杯、杯蓋、零食袋 |
| `output/page-03-tea-pastry.png` | 茶包系列（茶盒、茶包、立體茶包＋標誌軋型吊牌、茶杯）與糕餅禮盒系列（禮盒、禮盒提袋、開盒內襯、單顆包裝） |

標準色（取自原稿）：磚紅 `#9A3439`、橄欖綠 `#80843C`、青瓷綠 `#C4DCBF`、米白 `#FCF5E3`。

## 重新產生

```bash
python3 build.py      # writes html/*.html (inline SVG)
node render.js        # renders output/*.png at 2x (needs Playwright + Chromium)
```

`assets/` holds the logo, its outer silhouette (used for die-cut tea-bag tags) and the wordmark. They were vectorised from the supplied artwork.

## 標準字風格圖示（icons/）

The 28 wayfinding icons are drawn from the stroke features of the 新北市土城區公所 wordmark:
- **Thin vs. bold**: hairline strokes against bold vertical bars (新, 北, 市, 所).
- **Frames**: frames with one bold side (區).
- **Diagonals**: thin-and-bold diagonal pairs (公).
- **Crossbars**: horizontal strokes overshoot the verticals (土).
- **Shapes**: right-angle hooks and square dots, with no curves.

| 檔案 | 用途 |
| --- | --- |
| `icons/tucheng-icons.ai` | PDF-compatible Illustrator file. The icons are editable strokes and the labels are outlined. |
| `icons/tucheng-icons.pdf` | The same artwork as a PDF. |
| `icons/tucheng-icons-sheet.svg` / `.png` | The full icon sheet, plus a preview image. |
| `icons/svg/*.svg`, `icons/tucheng-icons-svg.zip` | Each icon on its own 96 × 96 artboard. |

Grid: 96 × 96, hairline 4, bold 11, colour `#9A3439`. Regenerate with `python3 icons.py && node render_icons.js`.

### 深灰透明版（icons/darkgray/）

This version is dark gray `#333333` on a transparent background, with no labels or tiles.

| 檔案 | 用途 |
| --- | --- |
| `darkgray/png/*.png`, `tucheng-icons-darkgray-png.zip` | One transparent 1024 × 1024 PNG per icon. |
| `darkgray/svg/*.svg`, `tucheng-icons-darkgray-svg.zip` | One 96 × 96 SVG per icon, with no background. |
| `darkgray/tucheng-icons-darkgray.ai` / `.pdf` / `.svg` / `.png` | All 28 icons on one transparent sheet. |

Regenerate with `python3 export_darkgray.py && node render_darkgray.js`. Use `SIZE=2048` to change the PNG size.
