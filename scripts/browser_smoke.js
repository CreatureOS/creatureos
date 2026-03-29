#!/usr/bin/env node
const { chromium } = require('playwright');

async function main() {
  const url = process.argv[2] || 'http://127.0.0.1:404/';
  const expectedText = process.argv[3] || 'Choose an Ecosystem';
  const browser = await chromium.launch({ headless: true, chromiumSandbox: false });
  try {
    const page = await browser.newPage();
    await page.goto(url, { waitUntil: 'networkidle' });
    const bodyText = await page.locator('body').innerText();
    if (!bodyText.includes(expectedText)) {
      throw new Error(`Expected page to include "${expectedText}"`);
    }
    console.log(JSON.stringify({ url, expectedText }, null, 2));
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exit(1);
});
