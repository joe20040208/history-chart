// Transparent PNGs (one per icon + sheet) and a vector PDF/AI without background.
const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

(async () => {
  const dir = path.join(__dirname, 'darkgray');
  const size = +(process.env.SIZE || 1024);
  const pngDir = path.join(dir, 'png');
  fs.mkdirSync(pngDir, { recursive: true });
  for (const f of fs.readdirSync(pngDir)) fs.unlinkSync(path.join(pngDir, f));
  const browser = await chromium.launch();

  const page = await browser.newPage({ viewport: { width: size, height: size } });
  for (const name of fs.readFileSync(path.join(dir, 'names.txt'), 'utf8').trim().split('\n')) {
    const svg = fs.readFileSync(path.join(dir, 'svg', name + '.svg'), 'utf8')
      .replace('width="96" height="96"', `width="${size}" height="${size}"`);
    await page.setContent(`<html><body style="margin:0;background:transparent">${svg}</body></html>`);
    await page.screenshot({ path: path.join(pngDir, name + '.png'), omitBackground: true,
                            clip: { x: 0, y: 0, width: size, height: size } });
  }
  await page.close();

  const html = 'file://' + path.join(dir, 'sheet.html');
  const m = fs.readFileSync(path.join(dir, 'sheet.html'), 'utf8').match(/width="(\d+)" height="(\d+)"/);
  const [w, h] = [+m[1], +m[2]];
  const sheet = await browser.newPage({ viewport: { width: w, height: h }, deviceScaleFactor: 2 });
  await sheet.goto(html);
  await sheet.screenshot({ path: path.join(dir, 'tucheng-icons-darkgray.png'), omitBackground: true });
  const pdf = path.join(dir, 'tucheng-icons-darkgray.pdf');
  await sheet.pdf({ path: pdf, width: w + 'px', height: h + 'px', printBackground: false, pageRanges: '1' });
  fs.copyFileSync(pdf, path.join(dir, 'tucheng-icons-darkgray.ai'));
  await browser.close();
  console.log('done');
})();
