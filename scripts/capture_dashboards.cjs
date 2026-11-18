// Optional visual acceptance. Requires Playwright and an installed browser.
const path = require('node:path');
const fs = require('node:fs');
const { chromium } = require(process.env.PLAYWRIGHT_ROOT || 'playwright');
const root = path.resolve(__dirname, '..');
const env = Object.fromEntries((fs.existsSync(path.join(root, '.env')) ? fs.readFileSync(path.join(root, '.env'), 'utf8') : '')
  .split(/\r?\n/).filter(line => line.includes('=') && !line.startsWith('#')).map(line => [line.slice(0,line.indexOf('=')),line.slice(line.indexOf('=')+1)]));
const base = 'http://localhost:' + (process.env.GRAFANA_PORT || env.GRAFANA_PORT || '3000');
async function main() {
  const browser = await chromium.launch({headless:true, executablePath:process.env.DIAGRAM_BROWSER || undefined});
  try {
    const page = await browser.newPage({viewport:{width:1680,height:1200},deviceScaleFactor:1});
    await page.goto(base + '/login');
    await page.getByPlaceholder('email or username').fill(env.GRAFANA_ADMIN_USER || 'admin');
    await page.getByPlaceholder('password', {exact:true}).fill(env.GRAFANA_ADMIN_PASSWORD || 'admin');
    await page.getByRole('button',{name:'Log in',exact:true}).click();
    const skip = page.getByRole('button',{name:'Skip',exact:true});
    try { await skip.waitFor({timeout:10000}); await skip.click(); } catch {}
    fs.mkdirSync(path.join(root, 'reports/screenshots'),{recursive:true});
    const requested = process.argv.slice(2).filter(value => !value.startsWith('--'));
    for (const name of ['scoring','storage','system','business'].filter(name => !requested.length || requested.includes(name))) {
      await page.goto(base + '/d/fraud-' + name + '?from=now-15m&to=now&refresh=5s&kiosk');
      await page.waitForSelector('[data-testid*="panel"]',{timeout:30000});
      await page.waitForTimeout(10000);
      const text = await page.locator('body').innerText();
      if (/Query error|db query error|Panel plugin not found|Failed to load/i.test(text)) throw new Error(name + ': ' + text);
      await page.screenshot({path:path.join(root,'reports/screenshots',name+'.png'),fullPage:true});
      console.log(name + ': captured');
    }
    if (process.argv.includes('--filters')) {
      const now = Date.now();
      const response = await page.request.post(base + '/api/ds/query', {data: {
        from:String(now-900000),to:String(now),queries:[{refId:'A',datasource:{uid:'postgres',type:'postgres'},
          rawSql:'SELECT DISTINCT merch FROM monitoring_scores WHERE has_context ORDER BY 1 LIMIT 2',
          format:'table',rawQuery:true}]
      }});
      const merchants = (await response.json()).results.A.frames[0].data.values[0];
      if (merchants.length !== 2) throw new Error('Need two merchants for visual filter acceptance');
      const cases = [
        ['single DC', ['DC'], ['All'], sql => sql.includes("'DC'")],
        ['multiple states', ['DC','NY'], ['All'], sql => sql.includes("'DC'") && sql.includes("'NY'")],
        ['multiple merchants', ['All'], merchants, sql => merchants.every(m => sql.includes("'"+m.replaceAll("'","''")+"'"))],
        ['quoted empty selection', ['All'], ["O'Reilly"], sql => sql.includes("'O''Reilly'")],
        ['all values', ['All'], ['All'], sql => sql.includes("'__all_values__'")]
      ];
      const evidence = [];
      for (const [name,states,selectedMerchants,matches] of cases) {
        const sent = [], replies = [];
        const onRequest = request => {
          if (request.url().includes('/api/ds/query') && request.method()==='POST')
            sent.push(...(request.postDataJSON()?.queries || []).map(q => q.rawSql || ''));
        };
        const onResponse = response => {
          if (response.url().includes('/api/ds/query')) replies.push(response.json());
        };
        page.on('request',onRequest); page.on('response',onResponse);
        const params = new URLSearchParams({from:'now-15m',to:'now',kiosk:''});
        states.forEach(s => params.append('var-state',s));
        selectedMerchants.forEach(m => params.append('var-merchant',m));
        await page.goto(base+'/d/fraud-business?'+params);
        await page.waitForSelector('[data-testid*="panel"]',{timeout:30000});
        await page.waitForTimeout(8000);
        page.off('request',onRequest); page.off('response',onResponse);
        const errors = (await Promise.all(replies)).flatMap(r => Object.values(r.results || {})).filter(r => r.error);
        if (errors.length || !sent.some(matches)) throw new Error(name+': '+JSON.stringify({errors,sent}));
        evidence.push({filter:name,status:'passed'});
        console.log(name+': passed');
      }
      fs.writeFileSync(path.join(root,'reports/visual_validation.json'),JSON.stringify(evidence,null,2));
    }
  } finally {
    await browser.close();
  }
}
main().catch(error=>{console.error(error);process.exit(1)});
