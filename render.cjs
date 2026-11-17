// Render locally authored BPMN and Mermaid sources, with no network requests.
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { createRequire } = require('node:module');

const root = __dirname;
const toolRoot = process.env.DIAGRAM_TOOLS || root;
const runtimeRoot = process.env.DIAGRAM_RUNTIME_NODE_MODULES;
const resolveTool = createRequire(path.join(toolRoot, 'package.json'));
const { chromium } = runtimeRoot ? require(path.join(runtimeRoot, 'playwright')) : resolveTool('playwright');

async function main() {
  const browser = await chromium.launch({
    executablePath: process.env.DIAGRAM_BROWSER || undefined,
    headless: true,
    args: ['--disable-gpu'],
  });
  const page = await browser.newPage({ viewport: { width: 1800, height: 1200 }, deviceScaleFactor: 1 });
  await page.route('**/*', route => route.abort());
  const report = { bpmn: [], mermaid: [] };
  const preview = path.join(os.tmpdir(), 'geosearch-diagram-previews');
  fs.mkdirSync(preview, { recursive: true });
  try {
    await page.setContent('<html><body style="margin:0;background:white"><div id="canvas" style="width:3400px;height:1600px"></div></body></html>');
    await page.addScriptTag({ path: path.join(toolRoot, 'node_modules/bpmn-js/dist/bpmn-viewer.development.js') });
    const xml = fs.readFileSync(path.join(root, 'search-process.bpmn'), 'utf8');
    for (const [diagramId, basename, title] of [
      ['Process_Search_Diagram', 'bpmn-search', 'BPMN — показ блока'],
      ['Process_Geo_Diagram', 'bpmn-geosearch', 'BPMN — геопоиск'],
    ]) {
      const result = await page.evaluate(async ({ xml, diagramId }) => {
        if (window.viewer) window.viewer.destroy();
        window.viewer = new BpmnJS({ container: '#canvas', textRenderer: {
          defaultStyle: { fontFamily: 'Arial, sans-serif', fontSize: 16 },
          externalStyle: { fontFamily: 'Arial, sans-serif', fontSize: 14 },
        } });
        const imported = await window.viewer.importXML(xml, diagramId);
        const { svg } = await window.viewer.saveSVG();
        return { svg, warnings: imported.warnings.map(w => w.message) };
      }, { xml, diagramId });
      const box = /viewBox="([^"]+)"/.exec(result.svg)[1].split(' ').map(Number);
      const [x, y, width, height] = box;
      let svg = result.svg.replace(/viewBox="[^"]+"/, `viewBox="${x - 25} ${y - 65} ${width + 50} ${height + 100}"`);
      svg = svg.replace(/width="[^"]+" height="[^"]+"/, `width="${width + 50}" height="${height + 100}"`);
      svg = svg.replace('</svg>', `<text x="${x}" y="${y - 25}" font-family="Arial,sans-serif" font-size="26" font-weight="bold" fill="#163b5c">${title}</text></svg>`);
      fs.writeFileSync(path.join(root, `${basename}.svg`), svg);
      report.bpmn.push({ diagramId, warnings: result.warnings });
    }
    await page.setContent('<html><body style="margin:0;background:white"><main id="diagram"></main></body></html>');
    const esbuild = resolveTool('esbuild');
    const bundle = await esbuild.build({
      stdin: { contents: 'import mermaid from "mermaid"; import elk from "@mermaid-js/layout-elk"; mermaid.registerLayoutLoaders(elk); window.mermaid=mermaid;', resolveDir: toolRoot },
      bundle: true, format: 'iife', write: false,
    });
    await page.addScriptTag({ content: bundle.outputFiles[0].text });
    await page.evaluate(() => mermaid.initialize({
      startOnLoad: false, securityLevel: 'strict', theme: 'base',
      themeVariables: { fontFamily: 'Arial, sans-serif', fontSize: '17px', lineColor: '#516b82', primaryTextColor: '#163b5c' },
      flowchart: { htmlLabels: false, curve: 'basis', nodeSpacing: 65, rankSpacing: 90, padding: 18, wrappingWidth: 320 },
    }));
    for (const basename of ['c4-context', 'c4-container']) {
      const source = fs.readFileSync(path.join(root, `${basename}.mmd`), 'utf8');
      const svg = await page.evaluate(async ({ source, id }) => {
        await mermaid.parse(source);
        const rendered = (await mermaid.render(id, source)).svg;
        const host = document.createElement('div');
        host.innerHTML = rendered;
        return new XMLSerializer().serializeToString(host.querySelector('svg'));
      }, { source, id: basename.replaceAll('-', '_') });
      fs.writeFileSync(path.join(root, `${basename}.svg`), svg);
      report.mermaid.push({ source: `${basename}.mmd`, parsed: true });
    }
    for (const basename of ['bpmn-search', 'bpmn-geosearch', 'c4-context', 'c4-container']) {
      const svg = fs.readFileSync(path.join(root, `${basename}.svg`), 'utf8');
      await page.setContent(`<html><body style="margin:0;background:white">${svg}</body></html>`);
      await page.locator('svg').evaluate(el => {
        const box = el.viewBox.baseVal;
        const width = Math.min(box.width, 2200);
        el.style.width = `${width}px`;
        el.style.height = `${width * box.height / box.width}px`;
        el.style.maxWidth = 'none';
      });
      await page.locator('svg').screenshot({ path: path.join(preview, `${basename}.png`) });
    }
    fs.writeFileSync(path.join(root, 'validation.json'), JSON.stringify(report, null, 2) + '\n');
    console.log(JSON.stringify({ ...report, preview }, null, 2));
  } finally {
    await browser.close();
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
