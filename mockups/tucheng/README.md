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
