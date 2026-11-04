// Render PNGs from the built SVGs with Playwright/Chromium.
//   icons -> png/<colourway>/<name>.png   (512 x 512, transparent)
//   overview.html -> overview.png
const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const ROOT = path.resolve(__dirname, '..');

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  const shoot = async (svgFile, outFile, w, h, scale) => {
    const svg = fs.readFileSync(svgFile, 'utf8');
    await page.setViewportSize({ width: w, height: h });
    await page.setContent(`<html><body style="margin:0;background:transparent">${svg.replace(/width="\d+" height="\d+"/, `width="${w}" height="${h}"`)}</body></html>`);
    fs.mkdirSync(path.dirname(outFile), { recursive: true });
    await page.screenshot({ path: outFile, omitBackground: true, clip: { x: 0, y: 0, width: w, height: h } });
  };
  for (const cw of fs.readdirSync(path.join(ROOT, 'svg'))) {
    for (const f of fs.readdirSync(path.join(ROOT, 'svg', cw))) {
      await shoot(path.join(ROOT, 'svg', cw, f), path.join(ROOT, 'png', cw, f.replace('.svg', '.png')), 512, 512);
    }
  }
  const ov = await browser.newPage({ viewport: { width: 1200, height: 800 }, deviceScaleFactor: 2 });
  await ov.goto('file://' + path.join(ROOT, 'overview.html'));
  await ov.evaluate(() => document.fonts.ready);
  await ov.screenshot({ path: path.join(ROOT, 'overview.png'), fullPage: true });
  await browser.close();
})();
