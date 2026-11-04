// Render sheet.html to a PNG preview and a vector PDF (also saved as a PDF-compatible .ai).
const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

(async () => {
  const scale = +(process.env.SCALE || 2);
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 1200 }, deviceScaleFactor: scale });
  await page.goto('file://' + path.join(__dirname, 'sheet.html'));
  await page.screenshot({ path: path.join(__dirname, 'tucheng-icons-sheet.png') });
  const pdf = path.join(__dirname, 'tucheng-icons.pdf');
  await page.pdf({ path: pdf, width: '1600px', height: '1200px', printBackground: true, pageRanges: '1' });
  fs.copyFileSync(pdf, path.join(__dirname, 'tucheng-icons.ai'));
  await browser.close();
  console.log('done');
})();
