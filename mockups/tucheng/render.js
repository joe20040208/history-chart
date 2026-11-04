// Render html/*.html to output/*.png with the pre-installed Playwright Chromium.
const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

(async () => {
  const dir = path.join(__dirname, 'html');
  const out = path.join(__dirname, 'output');
  fs.mkdirSync(out, { recursive: true });
  const scale = +(process.env.SCALE || 2);
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 1131 }, deviceScaleFactor: scale });
  for (const f of fs.readdirSync(dir).filter((n) => n.endsWith('.html')).sort()) {
    await page.goto('file://' + path.join(dir, f), { waitUntil: 'networkidle' });
    await page.evaluate(() => document.fonts.ready);
    await page.waitForTimeout(200);
    const png = path.join(out, f.replace(/\.html$/, '.png'));
    await page.screenshot({ path: png });
    console.log('rendered', png);
  }
  await browser.close();
})();
