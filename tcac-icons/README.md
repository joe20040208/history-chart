# 桃園市兒童美術館 館內標示圖示

依標準字風格繪製的 20 個館內標示 icon，沒有外框、沒有文字。線條粗細一致、端點是圓的，人物和動物沿用吉祥物「圓頭＋兩點眼睛」的表情。禁止類只用一條斜線表示。

![overview](overview.png)

| # | 圖示 | English | 類型 |
|---|---|---|---|
| 01 | 雨傘架 | Umbrella Stand | 設施 |
| 02 | 嬰兒車放置處 | Stroller Parking | 設施 |
| 03 | 志工專用座位 | Volunteer Seating | 設施 |
| 04 | 哺乳室 | Nursing Room | 設施 |
| 05 | 尿布台 | Baby Changing | 設施 |
| 06 | 洗手間 | Restrooms | 設施 |
| 07 | 無障礙設施 | Accessible Facilities | 設施 |
| 08 | 茶水間 | Pantry | 設施 |
| 09 | 置物櫃 | Lockers | 設施 |
| 10 | 服務台 | Information Desk | 設施 |
| 11 | 電梯 | Elevator | 設施 |
| 12 | 有蛇出沒 | Caution: Snakes | 警告 |
| 13 | 寵物不落地 | Pets Must Be Carried | 規範 |
| 14 | 請保持安靜 | Please Keep Quiet | 規範 |
| 15 | 禁止跨越 | Do Not Cross | 禁止 |
| 16 | 禁止飲食 | No Food or Drink | 禁止 |
| 17 | 請勿觸摸 | Please Do Not Touch | 禁止 |
| 18 | 禁止奔跑 | No Running | 禁止 |
| 19 | 禁止使用閃光燈 | No Flash Photography | 禁止 |
| 20 | 禁止吸菸 | No Smoking | 禁止 |

## 檔案

- `svg/color/`：彩色版，藍色主體，禁止斜線與小點綴用珊瑚紅
- `svg/blue/`：單色藍版
- `svg/white/`：反白版，透明底白線，放在藍色或深色底上用
- `png/`：以上三種的 PNG，512×512，透明底

SVG 都是透明底，可以直接匯入 Illustrator / Figma 編修。線條交疊處的留白是用 mask 挖空，不是白色色塊。

## 規格

| 項目 | 數值 |
|---|---|
| 畫布 | 100×100（viewBox `10 10 100 100`） |
| 主線粗 | 5.6 |
| 標準字藍（取自 logo） | `#7C9DC6` |
| 禁止斜線 | `#E9736B` |

## 重新產生

圖形定義在 `tools/icons.py`。線條粗細可以用 `STROKE_SCALE` 整組一起調整。

```bash
python3 tools/build.py     # 產生 svg/ 與 overview.html
node tools/render.js       # 產生 png/ 與 overview.png（需要 playwright）
```
