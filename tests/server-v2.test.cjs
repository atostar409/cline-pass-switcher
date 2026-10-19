const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { Readable } = require('node:stream');

const root = path.resolve(__dirname, '..');
const source = fs.readFileSync(path.join(root, 'server-v2.js'), 'utf8');
const page = fs.readFileSync(path.join(root, 'public/index-v2.html'), 'utf8');
const model = 'cline-pass/deepseek-v4-flash';

test('AstrBot auto route chooses the fast/deep GLM paths deterministically', () => {
  const app = backend();
  const fast = 'cline-pass/deepseek-v4.1-flash';
  const deep = 'cline-pass/glm-5.3';
  assert.equal(app.run(`resolveModelId('cline-pass/astrbot-auto', {messages:[{role:'user',content:'老婆在干嘛呀'}]})`), fast);
  assert.equal(app.run(`resolveModelId('cline-pass/astrbot-auto', {messages:[{role:'user',content:'/深思 这个方案怎么设计'}]})`), deep);
  assert.equal(app.run(`resolveModelId('cline-pass/astrbot-auto', {messages:[{role:'user',content:'请详细分析这个架构报错并给出修复方案'}]})`), deep);
  assert.equal(app.run(`resolveModelId('cline-pass/astrbot-auto', {messages:[{role:'user',content:'${'x'.repeat(220)}'}]})`), deep);
  assert.equal(app.run(`resolveModelId('cline-pass/astrbot-auto', {messages:[{role:'user',content:'代码 ' + String.fromCharCode(96) + String.fromCharCode(96) + String.fromCharCode(96)}]})`), deep);
  assert.equal(app.run(`resolveModelId('cline-pass/astrbot-auto', {messages:[{role:'user',content:'请问 A？请问 B？'}]})`), deep);
  assert.equal(app.run(`resolveModelId('cline-pass/astrbot-auto', {messages:[{role:'user',content:[{type:'text',text:'${'x'.repeat(260)}'},{type:'image_url',image_url:{url:'https://example.invalid/a.png'}}]}]})`), fast);
  assert.equal(app.run(`resolveModelId('cline-pass/glm-5.3', {messages:[{role:'user',content:'短句'}]})`), deep);
});

test('AstrBot auto route sends the resolved model upstream', async () => {
  const app = backend();
  app.run(`globalThis.__seen = null; runChatChain = async (req, body, modelId, cfg) => {
    globalThis.__seen = { modelId, bodyModel: body.model, bodyMessages: body.messages, temperature: body.temperature, top_p: body.top_p, cfg };
    return { status: 200, out: { choices: [{ message: { content: 'ok' } }], usage: { prompt_tokens: 7, completion_tokens: 3 } }, routing: {}, trace: [], acc: { name: 'test' }, t0: Date.now() };
  };`);
  const response = await app.post('/v1/chat/completions', {
    model: 'cline-pass/astrbot-auto',
    messages: [{ role: 'user', content: '/深思 这个方案怎么设计' }],
  });
  assert.equal(response.status, 200);
  const seen = JSON.parse(app.run('JSON.stringify(globalThis.__seen)'));
  assert.equal(seen.modelId, 'cline-pass/glm-5.3');
  assert.equal(seen.bodyModel, 'cline-pass/glm-5.3');
  assert.equal(seen.bodyMessages.length, 1);
  assert.equal(seen.bodyMessages[0].content, '这个方案怎么设计');
  let usage = JSON.parse(app.files.get('metadata.json')).usage[0];
  assert.equal(usage.model, 'cline-pass/astrbot-auto');
  assert.equal(usage.resolvedModel, 'cline-pass/glm-5.3');

  await app.post('/v1/chat/completions', {
    model: 'cline-pass/astrbot-auto',
    messages: [{ role: 'user', content: '老婆在干嘛呀' }],
  });
  const fastSeen = JSON.parse(app.run('JSON.stringify(globalThis.__seen)'));
  assert.equal(fastSeen.modelId, 'cline-pass/deepseek-v4.1-flash');
  assert.equal(fastSeen.bodyModel, 'cline-pass/deepseek-v4.1-flash');
  assert.equal(fastSeen.temperature, 0.72);
  assert.equal(fastSeen.top_p, 0.92);
  assert.ok(fastSeen.bodyMessages.some((message) => message.role === 'system' && message.content.includes('现场细节')));
  usage = JSON.parse(app.files.get('metadata.json')).usage[0];
  assert.equal(usage.model, 'cline-pass/astrbot-auto');
  assert.equal(usage.resolvedModel, 'cline-pass/deepseek-v4.1-flash');
});

test('AstrBot auto alias is discoverable from /v1/models with an existing config', async () => {
  const app = backend({
    'config.json': JSON.stringify({ knownModels: ['cline-pass/glm-5.3'], accounts: [] }),
  });
  const response = await app.get('/v1/models');
  assert.equal(response.status, 200);
  assert.ok(response.body.data.some((entry) => entry.id === 'cline-pass/astrbot-auto'));
  const persisted = JSON.parse(app.files.get('config.json'));
  assert.ok(persisted.knownModels.includes('cline-pass/astrbot-auto'));
});

function backend(saved = {}) {
  const files = new Map(Object.entries(saved));
  const responseListeners = new Map();
  let handler;
  const context = vm.createContext({
    fs: { readFileSync(file) { if (!files.has(path.basename(file))) throw new Error('absent'); return files.get(path.basename(file)); },
      writeFileSync(file, content) { files.set(path.basename(file), content); } },
    path, process: { env: {} }, console: { log() {}, warn() {} },
    http: { createServer(fn) { handler = fn; return { on() {}, listen() {} }; } },
    setTimeout, clearTimeout, AbortController, URL, Buffer, TextDecoder, performance,
    // 沙箱替身：流式中继把上游 body 交给 Readable.fromWeb；测试里传进来的已经是可迭代流，原样返回即可
    Readable: { fromWeb: (web) => web },
    fetch() { throw new Error('Unexpected network request'); },
  });
  vm.runInContext(source.replace(/^import .*;\r?\n/gm, '')
    .replace('const __dirname = path.dirname(fileURLToPath(import.meta.url));', "const __dirname = '.';"), context);
  const mockResponse = () => {
    const res = {
      status: undefined, body: undefined, writes: [], ended: false,
      headersSent: false, writableEnded: false, destroyed: false,
      on(name, listener) { responseListeners.set(name, listener); }, off(name) { responseListeners.delete(name); },
      writeHead(code) { res.status = code; res.headersSent = true; },
      write(chunk) { res.writes.push(String(chunk)); return true; },
      end(text) {
        res.ended = true; res.writableEnded = true;
        if (text === undefined) return;
        try { res.body = JSON.parse(text); } catch { res.body = String(text); }
      },
    };
    return res;
  };
  return {
    files, context,
    disconnect: () => responseListeners.get('close')?.(),
    run: (code) => vm.runInContext(code, context),
    async post(url, body, headers = {}) {
      const req = Readable.from([Buffer.from(JSON.stringify(body))]);
      Object.assign(req, { url, method: 'POST', headers });
      const res = mockResponse();
      await handler(req, res);
      return { status: res.status, body: res.body, writes: res.writes, ended: res.ended };
    },
    async get(url, headers = {}) {
      const req = Readable.from([]);
      Object.assign(req, { url, method: 'GET', headers });
      const res = mockResponse();
      await handler(req, res);
      return { status: res.status, body: res.body, writes: res.writes, ended: res.ended };
    },
  };
}

function frontend() {
  const result = { innerHTML: '', style: {} };
  const context = vm.createContext({ localStorage: { getItem() { return ''; } },
    document: { querySelector() { return result; }, addEventListener() {} },
    window: { innerWidth: 1200 },
    requestAnimationFrame() {}, cancelAnimationFrame() {},
    performance, setInterval() {}, clearInterval() {} });
  const script = page.match(/<script>([\s\S]*?)<\/script>/)[1];
  vm.runInContext(script.slice(0, script.indexOf("$('#proxyBase').onclick")), context);
  return { result, run: (code) => vm.runInContext(code, context) };
}

test('watch: validation, deduplication, per-model persistence and authentication', async () => {
  const app = backend();
  for (const upstream of ['', '<img src=x>', 'constructor', '__proto__', 'a'.repeat(129), 1, null]) {
    assert.equal((await app.post('/api/watch-upstream', { model, upstream })).status, 400);
  }
  assert.equal((await app.post('/api/watch-upstream', { model: '__proto__', upstream: 'custom' })).status, 400);
  assert.equal((await app.post('/api/watch-upstream', { model: 'missing/model', upstream: 'custom' })).status, 400);
  for (let i = 0; i < 2; i++) assert.equal((await app.post('/api/watch-upstream', { model, upstream: 'custom-host' })).status, 200);
  const restarted = backend(Object.fromEntries(app.files));
  assert.equal(restarted.run(`META.models['${model}'].watchedUpstreams.join()`), 'custom-host');
  assert.equal(restarted.run(`META.models['${model}'].upstreams.length`), 17);
  assert.equal(restarted.run(`META.models['cline-pass/kimi-k3']`), undefined);
  const locked = backend({ 'config.json': JSON.stringify({ proxyKey: 'test-key' }) });
  assert.equal((await locked.post('/api/watch-upstream', { model, upstream: 'custom' })).status, 401);
});

test('metrics: source units, missing values, zero prices and endpoint grouping', () => {
  const app = backend();
  const or = JSON.parse(app.run(`JSON.stringify(endpointMetrics([
    { tag: 'vendor/a', pricing: { prompt: '0.000001', completion: '0.000002' }, latency_last_30m: { p50: 0.5 }, throughput_last_30m: { p50: 30 } },
    { tag: 'vendor/b', pricing: { prompt: '0.000002', completion: '0.0000005' }, latency_last_30m: { p50: 0.7 }, throughput_last_30m: { p50: 40 } },
    { tag: 'unknown', pricing: { prompt: null, completion: '' }, latency_last_30m: null },
    { tag: 'free', pricing: { prompt: '0', completion: '0' } }
  ], 'OpenRouter'))`));
  assert.equal(or[0].endpoints, 2);
  assert.deepEqual([or[0].input, or[0].output, or[0].cost, or[0].ttft, or[0].tps], [2, 0.5, 2.5, 500, 40]);
  assert.equal(or[1].cost, null);
  assert.equal(or[1].ttft, null);
  assert.equal(or[2].cost, 0);
  assert.equal(app.run(`metricNumber('  ')`), null);
  assert.equal(app.run(`endpointMetrics([{provider_name:'vendor',latency_last_1h:{p50:1200}}], 'Vercel')[0].ttft`), 1200);
});

test('delete: cleans model settings, survives restart and discovery, supports manual restore', async () => {
  const app = backend();
  assert.equal(app.run(`META.models['${model}'].upstreams.length`), 16);
  assert.equal(app.run(`META.models['deepseek/deepseek-v4-flash'].upstreams.length`), 29);
  app.run(`config.perModel['${model}'] = {upstreams:['deepinfra','deepseek'],upstream:'deepinfra',exclude:['deepinfra'],sort:'cost'};
    Object.assign(META.models['${model}'], {watchedUpstreams:['deepinfra'],availableProviders:['deepinfra'],upstreamDetail:{deepinfra:{cost:1}},upstreamStatus:{deepinfra:{status:'ok'}},lastProvider:'deepinfra'});`);
  for (const upstream of [null, '', '<bad>', 'constructor']) {
    assert.equal((await app.post('/api/delete-upstream', { model, upstream })).status, 400);
  }
  const response = await app.post('/api/delete-upstream', { model, upstream: 'deepinfra' });
  assert.equal(response.status, 200);
  assert.ok(!response.body.meta.upstreams.includes('deepinfra'));
  assert.deepEqual(response.body.config.upstreams, ['deepseek']);
  assert.deepEqual(response.body.config.exclude, []);
  assert.deepEqual(response.body.meta.watchedUpstreams, []);
  assert.deepEqual(response.body.meta.upstreamDetail, {});
  assert.equal(response.body.meta.lastProvider, undefined);
  const restarted = backend(Object.fromEntries(app.files));
  assert.equal(restarted.run(`META.models['${model}'].upstreams.includes('deepinfra')`), false);
  restarted.run(`learnAvailableProviders('${model}','Available providers are: deepinfra, another.');`);
  assert.equal(restarted.run(`META.models['${model}'].upstreams.includes('deepinfra')`), false);
  assert.equal(restarted.run(`META.models['${model}'].upstreams.includes('another')`), true);
  await restarted.post('/api/watch-upstream', { model, upstream: 'deepinfra' });
  assert.equal(restarted.run(`META.models['${model}'].upstreams.includes('deepinfra')`), true);
  const locked = backend({'config.json':JSON.stringify({proxyKey:'key'})});
  assert.equal((await locked.post('/api/delete-upstream', { model, upstream:'deepinfra' })).status, 401);
});

test('probe: correct gateway source, retained watch list and clear unavailable metrics', async () => {
  for (const pipeline of ['planner', 'direct']) {
    const app = backend();
    await app.post('/api/watch-upstream', { model, upstream: 'custom-host' });
    await app.post('/api/delete-upstream', { model, upstream: 'deepseek' });
    const responseBody = { choices: [{ message: { content: 'OK' } }], provider: 'deepseek', model: 'deepseek/deepseek-v4-flash' };
    if (pipeline === 'planner') responseBody.choices[0].message.provider_metadata = { gateway: { routing: { finalProvider: 'deepseek', canonicalSlug: 'deepseek/deepseek-v4-flash' } } };
    app.run(`META.orModelList = ['deepseek/deepseek-v4-flash']; META.orModelsFetchedAt = Date.now();
      fetchJSON = async (url) => {
        if (url.endsWith('/endpoints')) {
          if (!url.startsWith('${pipeline === 'planner' ? 'https://ai-gateway.vercel.sh' : 'https://openrouter.ai'}')) throw new Error('Wrong source');
          return { json: { data: { endpoints: ['deepseek','deepinfra'].map((slug) => ({ provider_name: slug, tag: slug, pricing: { prompt: '0', completion: '0' } })) } } };
        }
        return {json:${JSON.stringify(responseBody)}};
      };
      harvestAvailableProviders = async () => ['deepseek'];`);
    const response = await app.post('/api/probe', { model });
    assert.equal(response.body.ok, true);
    assert.ok(response.body.upstreams.includes('custom-host'));
    assert.ok(!response.body.upstreams.includes('deepseek'));
    assert.equal(response.body.upstreamDetail.deepseek, undefined);
    assert.equal(response.body.upstreamDetail.deepinfra.source, pipeline === 'planner' ? 'Vercel' : 'OpenRouter');
    app.run(`vercelEndpoints = orEndpoints = async () => { throw new Error('offline'); };`);
    assert.deepEqual((await app.post('/api/probe', { model })).body.upstreamDetail, {});
  }
});

test('test endpoint: strict custom target, complete error providers and bad input', async () => {
  const app = backend();
  app.run(`config.perModel['${model}'] = {pinMode:'preferred',exclude:['custom-host']};
    runChatChain = async (req, body, model, cfg) => {
      if (cfg.pinMode !== 'strict' || cfg.exclude.length || cfg.upstreams[0] !== 'custom-host') throw new Error('Incorrect custom test settings');
      return { status: 502, out: { error: { message: 'Available providers are: deepinfra, relace, nextbit.' } }, trace: [{upstream:'custom-host',status:502,ms:1}] };
    };`);
  const response = await app.post('/api/test', { model, upstreams: ['custom-host'], exclude: [], pinMode: 'strict' });
  assert.equal(response.body.ok, false);
  assert.deepEqual(response.body.errorProviders, ['deepinfra', 'relace', 'nextbit']);
  assert.equal((await app.post('/api/test', { model, upstreams: '<script>' })).status, 400);
  assert.equal(app.run(`errorProviders({message:'prefix '+JSON.stringify({error:{metadata:{available_providers:['relace','<bad>']}}})}).join()`), 'relace');
});

test('config: saving all watched providers does not silently truncate', async () => {
  const app = backend();
  await app.post('/api/watch-upstream', { model, upstream: 'custom-host' });
  const upstreams = JSON.parse(app.run(`JSON.stringify(META.models['${model}'].upstreams)`));
  await app.post('/api/config', { perModel: { [model]: { upstreams } } });
  assert.equal(app.run(`config.perModel['${model}'].upstreams.length`), 17);
});

test('UI: strategy sorting, missing metrics last, labels and silent missing values', () => {
  const ui = frontend();
  ui.run(`var meta = { upstreams:['missing','fast','cheap'], upstreamDetail: { fast:{cost:5,ttft:10,tps:100}, cheap:{cost:0,ttft:30,tps:20} } };`);
  assert.equal(ui.run(`sortedUpstreams(meta,'cost').join()`), 'cheap,fast,missing');
  assert.equal(ui.run(`sortedUpstreams(meta,'ttft').join()`), 'fast,cheap,missing');
  assert.equal(ui.run(`sortedUpstreams(meta,'tps').join()`), 'fast,cheap,missing');
  assert.equal(ui.run(`sortedUpstreams(meta,null).join()`), 'missing,fast,cheap');
  assert.equal(ui.run(`metricText(null,'ttft')`), '');
  assert.equal(ui.run(`metricText(null,'cost')`), '');
  assert.equal(ui.run(`metricText(null,'tps')`), '');
  assert.equal(ui.run(`metricsNote(null,'ttft')`), '');
  assert.equal(ui.run(`metricText({},null)`), '');
  assert.match(ui.run(`metricText({ttft:25},'ttft')`), /25 ms/);
  const mixed = ui.run(`upstreamPanel('${model}',meta,{sort:'ttft'})`);
  assert.match(mixed, /data-u="fast"[\s\S]*?aria-label="列表排名 1"/);
  assert.match(mixed, /data-u="missing"[\s\S]*?aria-label="列表排名 3"/);
  const empty = ui.run(`upstreamPanel('${model}',{upstreams:['first','second']},{sort:'cost'})`);
  assert.match(empty, /data-u="first"[\s\S]*?aria-label="列表排名 1"/);
  assert.match(empty, /data-u="second"[\s\S]*?aria-label="列表排名 2"/);
  assert.ok(!empty.includes('class="upmetric"'));
});

test('UI: clickable providers bind original model and escape hostile error text', () => {
  const ui = frontend();
  ui.run(`showTest({ok:false,error:'<img src=x onerror=alert(1)> deepinfra; not-deepinfra',errorProviders:['deepinfra','<bad>'],trace:[{upstream:'custom-host',status:502,ms:1}]},'${model}',['custom-host']);`);
  assert.ok(!ui.result.innerHTML.includes('<img'));
  assert.match(ui.result.innerHTML, /&lt;img/);
  assert.match(ui.result.innerHTML, /data-watch-model="cline-pass\/deepseek-v4-flash"/);
  assert.match(ui.result.innerHTML, /data-watch-upstream="deepinfra"/);
  assert.match(ui.result.innerHTML, /not-deepinfra/);
  assert.ok(!ui.result.innerHTML.includes('data-watch-upstream="<bad>"'));
  assert.match(ui.run(`highlightProviders('custom-new.','${model}',['custom-new'])`), /<\/button>\.$/);
  assert.equal(ui.run(`highlightProviders('custom-new.extra','${model}',['custom-new'])`), 'custom-new.extra');
});

test('UI: collapsed summary follows strategy and observed probe provider', () => {
  const ui = frontend();
  for (const [sort, label] of [['cost','最低成本'], ['ttft','最快首字'], ['tps','最高吞吐'], ['', '网关智能选择']]) {
    const html = ui.run(`summaryChips({sort:'${sort}'},{probedAt:1,lastProvider:'deepinfra'})`);
    assert.ok(html.includes('自动 · ' + label));
    assert.ok(html.includes('探测命中：deepinfra'));
  }
  assert.ok(!ui.run(`summaryChips({sort:'cost'},null)`).includes('探测命中'));
  const manual = ui.run(`summaryChips({upstreams:['deepseek'],exclude:['azure']},{probedAt:1,lastProvider:'deepseek'})`);
  assert.ok(manual.includes('1&nbsp;deepseek'));
  assert.ok(manual.includes('✕ 1'));
  assert.ok(!manual.includes('自动'));
  assert.ok(manual.includes('探测命中：deepseek'));
  assert.ok(ui.run(`summaryChips({sort:'cost',exclude:['azure']},null)`).includes('自动 · 最低成本'));
  assert.ok(ui.run(`summaryChips({},{probedAt:1,lastProvider:'<img>'})`).includes('&lt;img&gt;'));
});

test('UI: delete updates the matching model and hides missing metric markup', async () => {
  const ui = frontend();
  const panel = ui.run(`upstreamPanel('${model}', {upstreams:['deepinfra']}, {sort:'ttft'})`);
  assert.match(panel, /data-delete-upstream="deepinfra"/);
  assert.ok(!panel.includes('class="upmetric"'));
  assert.ok(!panel.includes('class="metrics-note"'));
  ui.run(`DATA = {subscription:[{id:'${model}',meta:{upstreams:['deepinfra']},config:{upstreams:['deepinfra']}}]};
    var rendered = false, message = '', button = {disabled:false};
    render = () => {rendered=true;}; status = (text) => {message=text;};
    api = async (url, body) => {
      if(url!=='/api/delete-upstream' || body.model!=='${model}' || body.upstream!=='deepinfra') throw new Error('Incorrect delete target');
      return {ok:true,meta:{upstreams:[]},config:{upstreams:[]}};
    };`);
  await ui.run(`deleteUpstream('${model}','deepinfra',button)`);
  assert.equal(ui.run('DATA.subscription[0].meta.upstreams.length'), 0);
  assert.equal(ui.run('DATA.subscription[0].config.upstreams.length'), 0);
  assert.equal(ui.run('rendered'), true);
  assert.equal(ui.run('button.disabled'), false);
  assert.equal(ui.result.open, true);
});

function streamFixture(app) {
  app.run(`var clock = 0, cancelled = false, released = false;
    performance = {now:()=>clock};
    function fixtureResponse(parts) {
      let index = 0;
      return {ok:true,headers:{get:()=> 'text/event-stream; charset=utf-8'},body:{getReader:()=>({
        async read() { if(index===parts.length) return {done:true}; const part=parts[index++]; clock=part.at; return {done:false,value:Buffer.isBuffer(part.text)?part.text:Buffer.from(part.text)}; },
        async cancel() {cancelled=true;}, releaseLock() {released=true;}
      })}};
    }
    function frame(data) {return 'data: '+JSON.stringify(data)+'\\r\\n\\r\\n';}
    var parts = [
      {at:100,text:': heartbeat\\r\\n\\r\\n'},
      {at:400,text:frame({choices:[{delta:{reasoning:'推理'}}]})},
      {at:1400,text:frame({choices:[{delta:{content:'雨滴',reasoning_details:{unexpected:true}},finish_reason:'stop'}]})},
      {at:1600,text:frame({choices:[],usage:{completion_tokens:100,completion_tokens_details:{reasoning_tokens:20}},provider:'deepinfra',model:'deepseek/deepseek-v4-flash'})+'data: [DONE]\\r\\n\\r\\n'}
    ];`);
}

test('speed: monotonic timings, reasoning, usage and fragmented UTF-8 SSE', async () => {
  const app = backend(); streamFixture(app);
  app.run(`var bytes=Buffer.from(parts[2].text), splitAt=bytes.indexOf(Buffer.from('雨'))+1;
    parts.splice(2,1,{at:1300,text:bytes.subarray(0,splitAt)},{at:1400,text:bytes.subarray(splitAt)});`);
  const result = await app.run('measureStream(fixtureResponse(parts),0)');
  assert.equal(result.firstPacketMs, 100);
  assert.equal(result.generationSpeed, 100);
  assert.equal(result.perceivedSpeed, 62.5);
  assert.equal(result.preview, '雨滴');
  assert.equal(result.finalProvider, 'deepinfra');
  assert.equal(app.run('cancelled && released'), true);
});

test('speed: missing or invalid token counts are hidden, single chunk has no generation rate', async () => {
  for (const count of [null, -1, 'invalid', 0, 1.5]) {
    const app = backend(); streamFixture(app);
    app.run(`parts[3].text=frame({usage:{completion_tokens:${JSON.stringify(count)}}})+'data: [DONE]\\n\\n';`);
    const result = await app.run('measureStream(fixtureResponse(parts),0)');
    assert.equal(result.generationSpeed, null);
    assert.equal(result.perceivedSpeed, null);
    assert.equal(result.firstPacketMs, 100);
  }
  const app = backend(); streamFixture(app);
  app.run('parts.splice(1,1)');
  assert.equal((await app.run('measureStream(fixtureResponse(parts),0)')).generationSpeed, null);
});

test('speed: rejects incomplete streams, stream errors, malformed JSON and empty output', async () => {
  for (const change of [
    'parts.pop()',
    `parts=[{at:100,text:'data: {bad}\\n\\n'}]`,
    `parts=[{at:100,text:frame({error:{message:'rate limited'}})}]`,
    `parts=[{at:100,text:'data: [DONE]\\n\\n'}]`,
  ]) {
    const app = backend(); streamFixture(app); app.run(change);
    await assert.rejects(app.run('measureStream(fixtureResponse(parts),0)'));
    assert.equal(app.run('cancelled && released'), true);
  }
});

test('speed API: strict supplier, correct public source, prices and uptime windows', async () => {
  for (const pipeline of ['planner', 'direct']) {
    const app = backend(); streamFixture(app);
    app.run(`config.apiKey='test-only'; META.models['${model}'].pipeline='${pipeline}'; var calls=[];
      fetch = async (url,opts) => {
        calls.push(url);
        if(url.endsWith('/chat/completions')) {
          const body=JSON.parse(opts.body);
          if(body.max_tokens!==1024 || !body.stream_options.include_usage || !opts.signal) throw new Error('Invalid speed request');
          const only=${pipeline === 'planner' ? 'body.providerOptions.gateway.only' : 'body.provider.only'};
          if(only.join()!=='deepinfra') throw new Error('Supplier not pinned');
          ${pipeline === 'planner' ? `parts[3].text=frame({usage:{completion_tokens:100},provider_metadata:{gateway:{routing:{finalProvider:'deepinfra',canonicalSlug:'deepseek/deepseek-v4-flash'}}}})+'data: [DONE]\\n\\n';` : ''}
          return fixtureResponse(parts);
        }
        if(!url.startsWith('${pipeline === 'planner' ? 'https://ai-gateway.vercel.sh' : 'https://openrouter.ai'}')) throw new Error('Wrong metrics source');
        return {ok:true,json:async()=>({data:{endpoints:[{provider_name:'deepinfra',tag:'deepinfra',pricing:{prompt:'0.000001',completion:'0.000002',input_cache_read:'0'},uptime_last_5m:91,uptime_last_15m:92,uptime_last_1d:99}]}})};
      };`);
    const response = await app.post('/api/speed-test', { model, upstream:'deepinfra' });
    assert.equal(response.status, 200);
    assert.equal(response.body.ok, true);
    assert.equal(response.body.matched, true);
    const offer = response.body.detail.offers[0];
    assert.deepEqual([offer.input,offer.output,offer.cacheRead,offer.uptimeDay], [1,2,0,99]);
    assert.equal(offer.shortWindow, pipeline === 'planner' ? '15m' : '5m');
    assert.equal(offer.uptimeShort, pipeline === 'planner' ? 92 : 91);
    assert.equal(response.body.totalMs, 1600);
    assert.equal(app.run('calls.length'), 2);
  }
});

test('speed: unknown suppliers, wrong actual supplier and missing attribution are rejected', async () => {
  const app = backend();
  assert.equal((await app.post('/api/speed-test',{model,upstream:'missing'})).status,400);
  assert.equal((await app.post('/api/speed-test',{model:'__proto__',upstream:'deepinfra'})).status,400);
  const locked = backend({'config.json':JSON.stringify({proxyKey:'key'})});
  assert.equal((await locked.post('/api/speed-test',{model,upstream:'deepinfra'})).status,401);
  for (const actual of [null, 'other']) {
    const fixture = backend(); streamFixture(fixture);
    fixture.run(`config.apiKey='test-only'; parts[3].text=frame({usage:{completion_tokens:100},provider:${JSON.stringify(actual)}})+'data: [DONE]\\n\\n';
      fetch=async()=>fixtureResponse(parts);`);
    const response = await fixture.post('/api/speed-test',{model,upstream:'deepinfra'});
    assert.equal(response.body.ok, false);
    assert.equal(response.body.generationSpeed, undefined);
  }
});

test('speed UI: source-specific availability, zero cache price and silent missing data', () => {
  for (const source of ['Vercel','OpenRouter']) {
    const ui=frontend();
    ui.run(`SPEED_RESULTS=[{ok:true,model:'${model}',upstream:'deepinfra',actual:'deepinfra',matched:true,totalMs:1600,firstPacketMs:100,generationSpeed:100,perceivedSpeed:62.5,completionTokens:100,testedAt:1,preview:'<img>',detail:{source:'${source}',fetchedAt:1,offers:[{endpoint:'deepinfra',input:1,output:2,cacheRead:0,uptimeShort:99,uptimeDay:100}]}}]; renderSpeedTable()`);
    assert.ok(ui.result.innerHTML.includes(source==='Vercel'?'可用(15m)':'可用(5m)'));
    assert.ok(ui.result.innerHTML.includes('可用(1d)'));
    assert.ok(ui.result.innerHTML.includes('缓存读'));
    assert.ok(ui.result.innerHTML.includes('$0'));
    assert.ok(ui.result.innerHTML.includes('&lt;img&gt;'));
    ui.run(`SPEED_RESULTS=[{ok:true,model:'${model}',upstream:'deepinfra',actual:'deepinfra',matched:true,totalMs:1600,firstPacketMs:100,generationSpeed:null,perceivedSpeed:null,testedAt:1,detail:null}]; renderSpeedTable()`);
    assert.ok(!ui.result.innerHTML.includes('生成速度'));
    assert.ok(!ui.result.innerHTML.includes('可用('));
  }
});

test('speed API: client disconnect aborts the upstream request', async () => {
  const app = backend();
  app.context.disconnectResponse = app.disconnect;
  app.run(`config.apiKey='test-only'; var wasAborted=false;
    fetch=async(url,opts)=>{disconnectResponse(); wasAborted=opts.signal.aborted; throw new Error('Disconnected');};`);
  const response = await app.post('/api/speed-test',{model,upstream:'deepinfra'});
  assert.equal(response.status,502);
  assert.equal(app.run('wasAborted'),true);
  assert.match(response.body.error,/连接已断开/);
});

test('speed API: bounded timeout aborts the request and reports failure', async () => {
  const app = backend();
  app.run(`config.apiKey='test-only'; var capturedMs=0, forceTimeout, wasAborted=false;
    setTimeout=(fn,ms)=>{capturedMs=ms;forceTimeout=fn;return 1;}; clearTimeout=()=>{};
    fetch=async(url,opts)=>{forceTimeout();wasAborted=opts.signal.aborted;throw new Error('Timeout');};`);
  const response = await app.post('/api/speed-test',{model,upstream:'deepinfra'});
  assert.equal(response.status,502);
  assert.equal(app.run('capturedMs'),120000);
  assert.equal(app.run('wasAborted'),true);
  assert.match(response.body.error,/超时/);
});

test('batch speed UI: numeric sorting, missing last, separate windows and stable ties', () => {
  const ui = frontend();
  ui.run(`SPEED_RESULTS = [
    {ok:true,upstream:'slow',generationSpeed:10,firstPacketMs:20,detail:{source:'OpenRouter',offers:[{input:0,uptimeShort:95,uptimeDay:99}]}},
    {ok:false,upstream:'failed',state:'失败',error:'<bad>'},
    {ok:true,upstream:'fast',generationSpeed:100,firstPacketMs:200,detail:{source:'Vercel',offers:[{input:3,uptimeShort:99,uptimeDay:100}]}},
    {ok:true,upstream:'tie',generationSpeed:100,firstPacketMs:30}
  ];`);
  for (const [key, order] of [['generationSpeed','fast,tie,slow,failed'],['firstPacketMs','slow,tie,fast,failed'],['input','fast,slow,failed,tie'],['uptime5m','slow,failed,fast,tie'],['uptime15m','fast,slow,failed,tie'],['uptimeDay','fast,slow,failed,tie']]) {
    ui.run(`sortSpeedTable('${key}')`);
    assert.equal(ui.run('speedTableRows().map(r=>r.upstream).join()'),order);
  }
  assert.ok(ui.result.innerHTML.includes('可用(5m)'));
  assert.ok(ui.result.innerHTML.includes('可用(15m)'));
  assert.ok(!ui.result.innerHTML.includes('&lt;bad&gt;'));
  assert.ok(ui.result.innerHTML.includes('失败'));
  assert.ok(ui.result.innerHTML.includes('class="speed-table-wrap"'));
  assert.ok(!ui.result.innerHTML.includes('<th>来源 / 详情</th>'));
});

test('batch speed UI: default all, controls and sequential progressive results', async () => {
  const ui = frontend();
  ui.run(`var inputs=[], nodes={};
    document.querySelector = (id) => nodes[id] ||= {value:'${model}',dataset:{},style:{},querySelectorAll:(selector)=>selector==='input:checked'?inputs.filter(x=>x.checked):inputs};
    DATA={subscription:[{id:'${model}',config:{},meta:{upstreams:['a','b','c']}}]};
    onSpeedModelChange();`);
  assert.equal(ui.run(`$('#speedUpstreams').innerHTML.match(/ checked/g).length`),3);
  ui.run(`inputs=['a','b','c'].map(value=>({value,checked:true})); selectSpeedUpstreams(false);`);
  assert.equal(ui.run(`$('#speedBtn').disabled`),true);
  ui.run(`selectSpeedUpstreams(true); var calls=[], snapshots=[], active=0, maxActive=0;
    renderSpeedTable=()=>snapshots.push(SPEED_RESULTS.map(r=>r.state).join());
    api=async(url,body)=>{
      active++;maxActive=Math.max(active,maxActive);calls.push(body.upstream);
      await Promise.resolve();active--;
      if(body.upstream==='b') throw new Error('simulated failure');
      return {ok:true,generationSpeed:body.upstream==='c'?100:10};
    };`);
  await ui.run('runSpeedTest()');
  assert.equal(ui.run('calls.join()'),'a,b,c');
  assert.equal(ui.run('maxActive'),1);
  assert.equal(ui.run('snapshots.includes("完成,测速中,等待中")'),true);
  assert.equal(ui.run('SPEED_RESULTS.map(r=>r.state).join()'),'完成,失败,完成');
  assert.equal(ui.run('SPEED_SORT'),'generationSpeed');
  assert.equal(ui.run('speedTableRows().map(r=>r.upstream).join()'),'c,a,b');
  assert.equal(ui.run('SPEED_BUSY'),false);
  assert.equal(ui.run('inputs.every(x=>!x.disabled)'),true);
});

test('usage ledger: usageTokens extraction, record wiring, cap, /api/usage auth and persistence', async () => {
  const seed = { models: {}, history: [], usage: [{ ts: 1000, model, account: 'a@x', provider: 'relace', prompt: 10, completion: 5, cached: 4, total: 15 }] };
  const app = backend({ 'config.json': JSON.stringify({ proxyKey: 'k', port: 3123 }), 'metadata.json': JSON.stringify(seed) });
  assert.equal((await app.get('/api/usage')).status, 401);
  const ok = await app.get('/api/usage', { 'x-admin-key': 'k' });
  assert.equal(ok.status, 200);
  assert.equal(ok.body.usage.length, 1);
  assert.deepEqual({ ...app.run(`usageTokens({prompt_tokens:10, completion_tokens:'5', prompt_tokens_details:{cached_tokens:4}})`) }, { prompt: 10, completion: 5, cached: 4, total: 15 });
  assert.deepEqual({ ...app.run('usageTokens({prompt_tokens:3})') }, { prompt: 3, completion: 0, cached: 0, total: 3 });
  for (const bad of ['null', 'undefined', '{}', '{prompt_tokens:-1}', '{completion_tokens:"x"}', '"s"', '3']) {
    assert.equal(app.run(`usageTokens(${bad})`), null, bad);
  }
  app.run(`record('m1', { provider: 'p', ms: 7, stream: true, account: 'a@x', usage: { prompt_tokens: 3, completion_tokens: 2 } })`);
  app.run(`record('m2', { provider: 'p' })`);
  assert.equal(app.run('META.usage.length'), 2);
  const restarted = backend(Object.fromEntries(app.files));
  const r2 = await restarted.get('/api/usage', { 'x-admin-key': 'k' });
  assert.equal(r2.body.usage[0].model, 'm1');
  assert.equal(r2.body.usage[0].total, 5);
  assert.equal(r2.body.usage[0].stream, true);
  assert.equal(r2.body.usage[0].account, 'a@x');
  restarted.run(`META.usage = Array.from({ length: 5000 }, (_, i) => ({ ts: i, model: 'm', prompt: 1, completion: 0, cached: 0, total: 1 })); record('mx', { usage: { prompt_tokens: 1, completion_tokens: 0 } })`);
  assert.equal(restarted.run('META.usage.length'), 5000);
});

test('usage UI: overview cards, sky, ribbon, recent details and lab sections are wired into the page', () => {
  // 总览：数字条 / 席位卡阵 / 星图 / 趋势 / 缎带 / 明细
  for (const id of ['statStrip', 'deck', 'sky', 'skyAxis', 'trendSvg', 'trendTip', 'ribbon', 'rbBody', 'recentBody', 'recentFilter', 'detailNote', 'planMeta']) {
    assert.ok(page.includes(`id="${id}"`), id);
  }
  // 机房：账号管理（含订阅日）、订阅模型、测试台、测速、目录
  for (const id of ['accBody', 'subBody', 'testResult', 'speedResult', 'catBody', 'accRenew', 'exposeCatalog']) {
    assert.ok(page.includes(id), id);
  }
  // 请求历史板块已删除：账本合并进最近明细（100 条）
  assert.ok(!page.includes('请求历史'));
  assert.ok(!page.includes('loadHistory'));
  assert.ok(!page.includes('histBody'));
  assert.ok(!page.includes('甜甜圈'), '甜甜圈已退役');
  assert.match(page, /loadUsage\(\);\s*\n\s*loadPlanUsage\(\);/);
  assert.ok(page.includes('/api/plan-usage'), '官方用量接口在岗');
  assert.ok(page.includes('停止V2.bat'), '旧服务 404 时席位卡会说人话');
  assert.ok(page.includes("slice(0, RECENT_LIMIT)"), '明细按所选条数截断');
  assert.ok(page.includes('data-n="100"'), '明细可切到 100 条');
  assert.ok(page.includes('id="numToggle"'), '数字写法开关在岗');
  assert.ok(!page.includes('mascot'), '看板娘已移除');
  assert.ok(!page.includes('assets/mascot'), '看板娘素材引用已移除');
  assert.ok(page.includes("getItem('cps_num')"), '数字写法记忆在岗');
  assert.ok(page.includes("NUM_MODE === 'short'"), '顶部按钮切换简写/完整写法');
});

// —— 官方账单端点：归一解析（结构取自 2026-09-27 实测响应） ——
test('plan-usage: parseOfficialPlan normalizes plan, period end, caps and three windows', () => {
  const app = backend();
  const planJson = {
    data: {
      currentPeriodEnd: '2026-10-10T00:42:46Z',
      plan: {
        displayName: 'Cline Pass (Monthly)', interval: 'Monthly', isActive: true,
        entitlements: { cline_pass: { enabled: true, inferenceCapThreshold: { last5HoursUsageCostUSDPerUser: 1e9, last7daysUsageCostUSDPerUser: 2.5e9, last30daysUsageCostUSDPerUser: 5e9 } } },
      },
    },
  };
  const limitsJson = { data: { limits: [
    { type: 'five_hour', percentUsed: 0 },
    { type: 'weekly', percentUsed: 38, resetsAt: '2026-10-01T04:29:04.754763563Z' },
    { type: 'monthly', percentUsed: 96, resetsAt: '2026-10-10T01:10:34.756851473Z' },
  ] } };
  const out = app.run(`parseOfficialPlan(${JSON.stringify(planJson)}, ${JSON.stringify(limitsJson)})`);
  assert.equal(out.planName, 'Cline Pass (Monthly)');
  assert.equal(out.interval, 'Monthly');
  assert.equal(out.isActive, true);
  assert.equal(out.currentPeriodEnd, '2026-10-10T00:42:46Z');
  assert.equal(out.caps.fiveHour.percentUsed, 0);
  assert.equal(out.caps.fiveHour.limitUsd, 10);
  assert.equal(out.caps.weekly.percentUsed, 38);
  assert.equal(out.caps.weekly.limitUsd, 25);
  assert.equal(out.caps.weekly.resetsAt, '2026-10-01T04:29:04.754763563Z');
  assert.equal(out.caps.monthly.percentUsed, 96);
  assert.equal(out.caps.monthly.limitUsd, 50);
  const broken = app.run('parseOfficialPlan(null, null)');
  assert.equal(broken.planName, null);
  assert.equal(broken.currentPeriodEnd, null);
  assert.equal(broken.caps.monthly.percentUsed, 0);
  assert.equal(broken.caps.monthly.limitUsd, undefined);
});

// —— 本地降级：5 小时滚动 / 日历周（周一起）/ 本月 三个窗口聚合 ——
test('plan-usage: localWindowTotals aggregates rolling and calendar windows per account', () => {
  const app = backend();
  const now = new Date('2026-09-27T12:00:00').getTime(); // 周日
  const H = 3600_000;
  const rows = [
    { ts: now - H, account: 'a@x', total: 100 },        // 1 小时前 → 三窗全进
    { ts: now - 3 * H, account: 'a@x', total: 200 },    // 3 小时前 → 5h + 周 + 月
    { ts: now - 6 * H, account: 'a@x', total: 400 },    // 6 小时前 → 出 5h 窗，进 周/月
    { ts: now - 8 * 24 * H, account: 'a@x', total: 800 },  // 8 天前 → 只进月
    { ts: now - 40 * 24 * H, account: 'a@x', total: 1600 }, // 上上月 → 全部出窗
    { ts: now - H, account: 'b@x', total: 9999 },       // 别的账号，不计入
  ];
  const t = app.run(`localWindowTotals(${JSON.stringify(rows)}, 'a@x', ${now})`);
  assert.equal(t.fiveHour, 300);
  assert.equal(t.requests.fiveHour, 2);
  // 2026-09-27 是周日，周一起算 → 9-21 以后：100+200+400 都在
  assert.equal(t.weekly, 700);
  assert.equal(t.requests.weekly, 3);
  assert.equal(t.monthly, 1500);
  assert.equal(t.requests.monthly, 4);
  const empty = app.run(`localWindowTotals([], 'a@x', ${now})`);
  assert.equal(empty.fiveHour, 0);
});

// —— 持久账本：累计/按天不随 5000 行滚动窗口淘汰（2026-10-12 修「累计几个星期一直是十几亿」） ——
test('usage ledger: totals and daily buckets survive row-window eviction and restart', async () => {
  const app = backend();
  const rec = (account) => `record('m1', ${JSON.stringify({ requestedModel: 'cline-pass/demo', account, usage: { prompt_tokens: 100, completion_tokens: 50, prompt_tokens_details: { cached_tokens: 40 } } })})`;
  app.run(`${rec('a@x')}; ${rec('b@x')};`);
  const meta = JSON.parse(app.files.get('metadata.json'));
  assert.equal(meta.usageTotals.requests, 2);
  assert.equal(meta.usageTotals.total, 300);
  assert.equal(meta.usageTotals.prompt, 200);
  assert.equal(meta.usageTotals.cached, 80);
  assert.equal(meta.usageTotals.byModel['cline-pass/demo'].total, 300);
  assert.equal(meta.usageTotals.byAcc['a@x'].total, 150);
  assert.ok(Number.isFinite(meta.usageTotals.since), '账本可信起点被记录');
  const day = app.run('localDayKey(Date.now())');
  assert.equal(meta.usageDaily[day].total, 300);
  assert.equal(meta.usageDaily[day].requests, 2);
  assert.equal(meta.usageDaily[day].byModel['cline-pass/demo'].total, 300);
  assert.equal(meta.usageDaily[day].byAcc['b@x'].requests, 1);
  // 行缓冲清空（模拟 5000 条滚动淘汰）后重启：账本总额与按天桶都还在
  app.run('META.usage.length = 0; saveMeta();');
  const restarted = backend(Object.fromEntries(app.files));
  const res = await restarted.get('/api/usage');
  assert.equal(res.status, 200);
  assert.equal(res.body.usage.length, 0);
  assert.equal(res.body.totals.total, 300);
  assert.equal(res.body.totals.requests, 2);
  assert.equal(res.body.daily[day].total, 300);
  // 再记一笔：进当日桶也进总额（不因行缓冲空而丢）
  restarted.run(`${rec('a@x')};`);
  const meta2 = JSON.parse(restarted.files.get('metadata.json'));
  assert.equal(meta2.usageTotals.total, 450);
  assert.equal(meta2.usageTotals.byAcc['a@x'].requests, 2);
  // 按天桶封顶 DAY_KEEP：超出的最老日期被删，最新日期保留
  restarted.run(`META.usageDaily = {};
  for (let i = 0; i < 409; i++) META.usageDaily[localDayKey(new Date(2026, 0, 1 + i).getTime())] = { requests: 1, prompt: 0, completion: 0, cached: 0, total: 1, byModel: {}, byAcc: {} };
  ledgerAdd({ ts: Date.now(), model: 'm1', account: 'a@x', total: 5 }); saveMeta();`);
  const dailyAfter = JSON.parse(restarted.files.get('metadata.json')).usageDaily;
  assert.equal(Object.keys(dailyAfter).length, 400, '按天桶保留上限生效');
  assert.ok(!dailyAfter['2026-01-01'], '最老日期已滚出');
  assert.ok(dailyAfter[day].total >= 5, '当日桶继续累加');
});

test('usage ledger UI: stat strip / star map / trend / ribbon prefer the persistent ledger', () => {
  assert.ok(page.includes('LEDGER = res.totals ? { totals: res.totals, daily: res.daily || {} } : null;'), '前端接住账本');
  assert.match(page, /\$\{fmtDT\(lt\.since\)\}/, '累计卡提示可信起点');
  assert.ok(page.includes('if (hasLedger) { for (const [k, d] of Object.entries(LEDGER.daily)) dayMap.set(k, d); }'), '星图读按天账本');
  assert.ok(page.includes("for (const [m, v] of Object.entries(d.byModel)) dayModel.set(d.key + '\\u0001' + m, v.total || 0);"), '趋势读按天分模型');
  assert.ok(page.includes('LEDGER.totals.byModel'), '缎带读全量模型汇总');
  assert.ok(page.includes('明细保留最近'), '明细条如实写明只是窗口片段');
});

test('plan-usage: estimatePeriodEnd prefers renewStart, falls back to addedAt plus 30 days', () => {
  const app = backend();
  const out = app.run(`estimatePeriodEnd({ renewStart: '2026-09-20T00:00:00Z', addedAt: '2026-09-01T00:00:00Z' })`);
  assert.equal(out, '2026-10-20T00:00:00.000Z');
  const byAdded = app.run(`estimatePeriodEnd({ addedAt: '2026-09-01T00:00:00Z' })`);
  assert.equal(byAdded, '2026-10-01T00:00:00.000Z');
  assert.equal(app.run('estimatePeriodEnd({})'), null);
});

// —— 账号 schema 迁移：启动补 addedAt 并写回；POST 保留 addedAt/renewStart ——
test('accounts: migration backfills addedAt and POST keeps it together with renewStart', async () => {
  const app = backend({ 'config.json': JSON.stringify({ port: 3123, accounts: [{ name: 'a@x', key: 'sk_a', enabled: true }] }) });
  assert.match(String(app.run('config.accounts[0].addedAt')), /^\d{4}-\d{2}-\d{2}T/);
  const persisted = JSON.parse(app.files.get('config.json'));
  assert.ok(persisted.accounts[0].addedAt, '迁移写回 config.json');
  const post = await app.post('/api/accounts', { accounts: [{ name: 'a@x', key: 'sk_a' }, { name: 'b@x', key: 'sk_b', renewStart: '2026-09-15' }], mode: 'roundrobin', active: 1 });
  assert.equal(post.status, 200);
  assert.match(String(app.run("config.accounts.find(a=>a.name==='a@x').addedAt")), /^\d{4}-\d{2}-\d{2}T/);
  assert.equal(app.run("config.accounts.find(a=>a.name==='b@x').renewStart"), '2026-09-15');
  assert.equal(app.run("config.accounts.find(a=>a.name==='b@x').addedAt") !== null, true);
  assert.equal(JSON.parse(app.files.get('config.json')).accountMode, 'roundrobin');
});

// —— /api/plan-usage：鉴权 + 官方不可达时按账号降级本地估算 ——
test('plan-usage API: auth required, official-first, graceful local fallback per account', async () => {
  const seedConfig = {
    port: 3123, proxyKey: 'k',
    accounts: [
      { name: 'a@x', key: 'sk_a', enabled: true, addedAt: '2026-09-01T00:00:00Z' },
      { name: 'off@x', key: 'sk_off', enabled: true, addedAt: '2026-09-02T00:00:00Z' },
    ],
  };
  const seedMeta = { models: {}, history: [], usage: [{ ts: Date.now() - 3600_000, model, account: 'a@x', total: 123, prompt: 100, completion: 23, cached: 0 }] };
  const app = backend({ 'config.json': JSON.stringify(seedConfig), 'metadata.json': JSON.stringify(seedMeta) });
  assert.equal((await app.get('/api/plan-usage')).status, 401);
  // 沙箱 fetch 默认 throw → 全部账号走 local 降级
  const ok = await app.get('/api/plan-usage', { 'x-admin-key': 'k' });
  assert.equal(ok.status, 200);
  assert.equal(ok.body.accounts.length, 2);
  for (const acc of ok.body.accounts) {
    assert.equal(acc.source, 'local');
    assert.equal(acc.estimated, true);
    assert.ok(acc.error, '降级原因可见');
    assert.match(acc.currentPeriodEnd, /^\d{4}-\d{2}-\d{2}T/);
    assert.ok(acc.caps.monthly.localTokens >= 0);
  }
  const a = ok.body.accounts[0];
  assert.equal(a.caps.fiveHour.localTokens, 123, '本地 5h 窗口计入最近一笔');
  // 强刷参数走 ?refresh=1
  const fresh = await app.get('/api/plan-usage?refresh=1', { 'x-admin-key': 'k' });
  assert.equal(fresh.status, 200);
});

// —— 账号熔断：额度满/密钥坏自动禁用，轮询跳过，绝不拖累其他号 ——
test('accounts circuit breaker: classify, disable, persist, rotation skip, auto-trim no false kill', async () => {
  const app = backend({ 'config.json': JSON.stringify({ port: 3123, accountMode: 'roundrobin', accounts: [
    { name: 'a@x', key: 'sk_a', enabled: true },
    { name: 'b@x', key: 'sk_b', enabled: true },
  ] }) });
  // 错误分类：429/额度词 → quota；401/403 → auth；普通上游错/网络错 → 不禁
  assert.equal(app.run(`classifyAccountError(429, 'rate limit exceeded')`), 'quota');
  assert.equal(app.run(`classifyAccountError(500, 'monthly usage quota exceeded')`), 'quota');
  assert.equal(app.run(`classifyAccountError(401, 'Unauthorized')`), 'auth');
  assert.equal(app.run(`classifyAccountError(403, 'Forbidden')`), 'auth');
  assert.equal(app.run(`classifyAccountError(502, 'upstream fetch failed')`), null);
  assert.equal(app.run(`classifyAccountError(500, 'empty response content')`), null);
  // quota 失败 → 自动禁用 + 原因持久化到 config.json
  assert.equal(app.run(`noteAccountFailure('a@x', 'quota', 'monthly cap reached')`), true);
  assert.equal(app.run('config.accounts[0].enabled'), false);
  assert.match(String(app.run('config.accounts[0].disabledReason')), /额度满/);
  assert.ok(JSON.parse(app.files.get('config.json')).accounts[0].enabled === false, '禁用持久化');
  // 已禁用的账号不重复记
  assert.equal(app.run(`noteAccountFailure('a@x', 'auth', 'x')`), false);
  // 轮询只在启用账号间进行
  assert.equal(app.run('pickAccount().name'), 'b@x');
  assert.equal(app.run('pickAccount().name'), 'b@x');
  // 全部禁光后的兜底
  assert.equal(app.run(`noteAccountFailure('b@x', 'auth', null)`), true);
  assert.equal(app.run('pickAccount().name'), '默认');
  // auto-trim：沙箱 fetch 不可用 = 网络失败 → 只报告，绝不误禁
  const trim = await app.post('/api/accounts/auto-trim', {});
  assert.equal(trim.status, 200);
  assert.equal(trim.body.results.length, 2);
  assert.equal(trim.body.results.every((r) => r.disabled === false), true, '网络失败不误禁');
  assert.ok(trim.body.results.every((r) => typeof r.enabled === 'boolean'), '报告带启用状态');
});

// —— 单账号刷新：点谁的「刷」只拉谁的官方账单，其余账号沿用缓存 ——
test('plan-usage: per-account refresh hits official endpoints only for the named account', async () => {
  const seedConfig = {
    port: 3123, proxyKey: 'k',
    accounts: [
      { name: 'a@x', key: 'sk_a', enabled: true, addedAt: '2026-09-01T00:00:00Z' },
      { name: 'b@x', key: 'sk_b', enabled: true, addedAt: '2026-09-02T00:00:00Z' },
    ],
  };
  const app = backend({ 'config.json': JSON.stringify(seedConfig), 'metadata.json': JSON.stringify({ models: {}, history: [], usage: [] }) });
  // 计数型官方端点（vm realm 里的数组不能和宿主 deepEqual，一律 join 成字符串再比）
  const hits = { plan: 0, limits: 0 };
  app.context.fetch = (url) => {
    if (String(url).includes('/plan/usage-limits')) hits.limits += 1; else hits.plan += 1;
    const body = String(url).includes('/plan/usage-limits')
      ? { data: { limits: [{ type: 'five_hour', percentUsed: 11, resetsAt: '2026-09-30T00:00:00Z' }, { type: 'weekly', percentUsed: 22, resetsAt: '2026-09-30T00:00:00Z' }, { type: 'monthly', percentUsed: 33, resetsAt: '2026-10-01T00:00:00Z' }] } }
      : { data: { plan: { displayName: 'Cline Pass Pro', interval: 'month', isActive: true, entitlements: { cline_pass: { inferenceCapThreshold: {} } } }, currentPeriodEnd: '2026-10-10T00:00:00Z' } };
    return Promise.resolve({ status: 200, text: () => Promise.resolve(JSON.stringify(body)) });
  };
  // 全量刷：2 账号 × 2 端点 = 4 次调用，两个账号都是官方数据
  const all = await app.run(`getPlanUsage(true).then(d => ({ n: d.accounts.length, sources: d.accounts.map(a => a.source).join('|') }))`);
  assert.equal(hits.plan, 2); assert.equal(hits.limits, 2);
  assert.equal(all.sources, 'official|official');
  // 缓存生效：TTL 内再取不调官方
  await app.run(`getPlanUsage(false)`);
  assert.equal(hits.plan, 2, 'TTL 内不重打官方');
  // 单账号强刷 a@x：只多 2 次调用（a 的 plan+limits），b 沿用缓存
  const single = await app.run(`getPlanUsage(true, 'a@x').then(d => ({ names: d.accounts.map(a => a.name).join('|'), sources: d.accounts.map(a => a.source).join('|') }))`);
  assert.equal(hits.plan, 3, '单刷只拉指定账号的 plan');
  assert.equal(hits.limits, 3, '单刷只拉指定账号的 limits');
  assert.equal(single.names, 'a@x|b@x', '输出保持 config 顺序');
  assert.equal(single.sources, 'official|official');
  // 单刷不存在的账号：返回缓存原样，不动官方
  const ghost = await app.run(`getPlanUsage(true, 'ghost@x').then(d => d.accounts.length)`);
  assert.equal(hits.plan, 3, '幽灵账号不触发官方调用');
  assert.equal(ghost, 2);
  // 路由层接 ?refresh=1&name=
  const viaRoute = await app.get('/api/plan-usage?refresh=1&name=b%40x', { 'x-admin-key': 'k' });
  assert.equal(viaRoute.status, 200);
  assert.equal(hits.plan, 4, '路由参数生效：b@x 被单刷');
  assert.equal(viaRoute.body.accounts.find((a) => a.name === 'b@x').planName, 'Cline Pass Pro');
});

// —— 前端：展开状态跟账号走，轮询重画不丢；明细空数据给提示；单账号刷新按钮接线 ——
test('plan-usage UI: expand state survives re-render, empty detail speaks, per-account refresh wired', () => {
  // 展开状态由 OPEN_ACCS 持有，卡片渲染读取它（重画后原地保留）
  assert.match(page, /const OPEN_ACCS = new Set\(\), REFRESHING = new Set\(\)/);
  assert.match(page, /\$\{OPEN_ACCS\.has\(p\.name\) \? ' open' : ''\}/);
  assert.match(page, /data-name="\$\{escapeHtml\(p\.name\)\}"/);
  // 点明细区不再收起（留给用户选字复制），点卡头以外不 toggle
  assert.match(page, /if \(e\.target\.closest\('\.acc-detail'\)\) return;/);
  // 「刷」按钮走单账号刷新：data-refresh-name → refreshAcc，不再全局 refreshPlan
  assert.match(page, /data-refresh-name="\$\{escapeHtml\(p\.name\)\}"/);
  assert.match(page, /async function refreshAcc\(name\)/);
  assert.match(page, /\/api\/plan-usage\?refresh=1&name=' \+ encodeURIComponent\(name\)/);
  assert.match(page, /if \(refreshBtn\) \{ refreshAcc\(refreshBtn\.dataset\.refreshName\); return; \}/);
  // 空态三连：账本未就绪 / 无本地请求 / 无模型明细 —— 展开绝不白板
  assert.match(page, /本地账本装载中/);
  assert.match(page, /本地账本里还没有这个账号的请求/);
  assert.match(page, /这个账号在本地账本里还没有模型明细/);
  assert.doesNotMatch(page, /if \(!rows\.length\) return '';/, '明细空行不再返回空串');
  // 进行中反馈 + 尊重系统减少动态偏好
  assert.match(page, /acc-refresh\.busy/);
  assert.match(page, /@media \(prefers-reduced-motion: reduce\) \{ \.acc-refresh\.busy/);
  // 沙箱实跑：AGG 未就绪时 accountCard 的明细分支与 accModelTable 空态
  const f = frontend();
  f.run(`AGG = null; PLAN = { accounts: [{ name: 'a@x', source: 'official', caps: {} }] };`);
  f.run(`globalThis.__detail = (function () { const p = PLAN.accounts[0]; const mine = AGG ? AGG.byAcc.find((a) => a.key === p.name) : null; return !AGG ? 'loading' : (mine ? accModelTable(p.name) : 'none'); })();`);
  assert.equal(f.run('__detail'), 'loading');
  f.run(`AGG = { byAcc: [], byAccModel: [] };`);
  f.run(`globalThis.__detail2 = accModelTable('ghost@x');`);
  assert.match(String(f.run('__detail2')), /还没有模型明细/);
  // 展开/收起状态机：toggle 后 OPEN_ACCS 与卡片 class 同步
  f.run(`OPEN_ACCS.add('a@x')`);
  assert.equal(f.run(`OPEN_ACCS.has('a@x')`), true);
  f.run(`OPEN_ACCS.delete('a@x')`);
  assert.equal(f.run(`OPEN_ACCS.has('a@x')`), false);
});

// —— 额度调度（smart）：本月用得最少的优先，并列轮转，窗口顶格跳过，无数据安全兜底 ——
test('smart mode: least monthly usage first, tie rotation, window guard, safe fallbacks', async () => {
  const app = backend({ 'config.json': JSON.stringify({ port: 3123, accountMode: 'smart', accounts: [
    { name: 'a@x', key: 'sk_a', enabled: true }, { name: 'b@x', key: 'sk_b', enabled: true }, { name: 'c@x', key: 'sk_c', enabled: true },
  ] }) });
  assert.equal(app.run('config.accountMode'), 'smart', '启动归一化保留 smart');
  const accs = JSON.stringify([{ name: 'a@x' }, { name: 'b@x' }, { name: 'c@x' }]);
  const plan = (m, h = [0, 0, 0], w = [0, 0, 0]) => JSON.stringify(['a@x', 'b@x', 'c@x'].map((n, i) => ({
    name: n, source: 'official', caps: {
      monthly: { percentUsed: m[i] }, fiveHour: { percentUsed: h[i] }, weekly: { percentUsed: w[i] },
    },
  })));
  const pick = (rows, counter) => app.run(`((x) => (x ? x.name : null))(pickSmartAccount(${accs}, ${rows}, ${counter}))`);
  // 本月 7/0/7 → 用用得最少的 b
  assert.equal(pick(plan([7, 0, 7]), 0), 'b@x');
  // 并列轮转：0/0/7 → a、b 交替，不钉死一个号
  assert.equal(pick(plan([0, 0, 7]), 0), 'a@x');
  assert.equal(pick(plan([0, 0, 7]), 1), 'b@x');
  assert.equal(pick(plan([0, 0, 7]), 2), 'a@x');
  // 5h / 周窗口 >=90 先让位：a 本月最低但窗口顶格 → 轮到 b
  assert.equal(pick(plan([0, 10, 20], [95, 0, 0]), 0), 'b@x');
  assert.equal(pick(plan([5, 5, 5], [0, 0, 0], [95, 95, 0]), 0), 'c@x');
  // 本月打满的跳过；全都打满 → null（调用方退回轮询，交给熔断）
  assert.equal(pick(plan([100, 3, 4]), 0), 'b@x');
  assert.equal(pick(plan([100, 100, 100]), 0), null);
  // 窗口全顶格时退一步：只要本月没打满，照样选（不把请求吊死）
  assert.equal(pick(plan([0, 0, 0], [95, 95, 95]), 0), 'a@x');
  // 官方数据缺（降级 local）的账号不进调度；全缺 → null
  const local = JSON.stringify(['a@x', 'b@x', 'c@x'].map((n) => ({ name: n, source: 'local' })));
  assert.equal(pick(local, 0), null);
  // 集成：缓存里有官方账本 → pickAccount 走额度调度
  app.run(`planCache = { at: Date.now(), data: { at: Date.now(), ttlSeconds: 60, accounts: ${plan([7, 0, 7])} }, promise: null }`);
  assert.equal(app.run('pickAccount().name'), 'b@x');
  app.run(`planCache.data.accounts = ${plan([0, 0, 7])}; SMART_COUNTER = 0;`);
  assert.equal(app.run('pickAccount().name'), 'a@x');
  assert.equal(app.run('pickAccount().name'), 'b@x');
  // 没有官方数据 → 本次退回轮询（预热在后台跑，绝不阻塞请求）
  app.run('planCache = { at: 0, data: null, promise: null }');
  const fallback = app.run('pickAccount().name');
  assert.ok(['a@x', 'b@x', 'c@x'].includes(fallback), '无数据时退回轮询');
  // POST 校验：smart 可持久化；未知模式落回 single
  const post = await app.post('/api/accounts', { accounts: [{ name: 'a@x', key: 'sk_a' }], mode: 'smart', active: 0 });
  assert.equal(post.body.mode, 'smart');
  assert.equal(JSON.parse(app.files.get('config.json')).accountMode, 'smart');
  const bogus = await app.post('/api/accounts', { accounts: [{ name: 'a@x', key: 'sk_a' }], mode: 'bogus' });
  assert.equal(bogus.body.mode, 'single');
});

// —— 额度调度端到端：官方账本后台预热，选号落在用得最少的号上 ——
test('smart mode: warms plan cache from official endpoints, then picks the least-used account', async () => {
  const app = backend({ 'config.json': JSON.stringify({ port: 3123, accountMode: 'smart', accounts: [
    { name: 'a@x', key: 'sk_a', enabled: true }, { name: 'b@x', key: 'sk_b', enabled: true },
  ] }) });
  let calls = 0;
  const planBody = { data: { plan: { displayName: 'Cline Pass (Monthly)', interval: 'Monthly', isActive: true,
    entitlements: { cline_pass: { inferenceCapThreshold: { last5HoursUsageCostUSDPerUser: 1000000000, last7daysUsageCostUSDPerUser: 2500000000, last30daysUsageCostUSDPerUser: 5000000000 } } } },
    currentPeriodEnd: '2026-10-30T00:00:00Z' } };
  app.context.fetch = async (url, opts = {}) => {
    calls += 1;
    const isA = String(opts.headers?.Authorization || '').includes('sk_a');
    const limits = isA
      ? [{ type: 'five_hour', percentUsed: 10, resetsAt: null }, { type: 'weekly', percentUsed: 20, resetsAt: null }, { type: 'monthly', percentUsed: 40, resetsAt: null }]
      : [{ type: 'five_hour', percentUsed: 0, resetsAt: null }, { type: 'weekly', percentUsed: 0, resetsAt: null }, { type: 'monthly', percentUsed: 5, resetsAt: null }];
    const body = String(url).endsWith('/usage-limits') ? { data: { limits } } : planBody;
    return { status: 200, text: async () => JSON.stringify(body) };
  };
  // 首个请求：缓存还没就绪 → 先退回轮询，同时后台预热
  app.run('planCache = { at: 0, data: null, promise: null }');
  const first = app.run('pickAccount().name');
  assert.ok(['a@x', 'b@x'].includes(first), '缓存未就绪时请求照常发出');
  await new Promise((r) => setTimeout(r, 30)); // 等后台预热落地（2 账号 × plan+limits 两端点）
  assert.equal(calls, 4, '每个账号只拉 plan + usage-limits 两次');
  assert.equal(app.run('planCache.data.accounts.length'), 2);
  assert.equal(app.run(`planCache.data.accounts.find((a) => a.name === 'b@x').source`), 'official');
  assert.equal(app.run(`planCache.data.accounts.find((a) => a.name === 'b@x').caps.monthly.percentUsed`), 5);
  // 预热后：b 本月 5% 比 a 40% 少 → 选号落 b
  assert.equal(app.run('pickAccount().name'), 'b@x');
  assert.equal(app.run('pickAccount().name'), 'b@x');
});

// —— 额度调度 UI：模式选项、保存提示文案接线 ——
test('smart mode UI: mode option, save message and stale-service guard wired in the lab page', () => {
  assert.match(page, /<option value="smart"[^>]*>账号池·额度调度/);
  assert.match(page, /smart: '额度调度'/);
  assert.match(page, /服务是旧版本不认这个模式/);
});

// —— 模型级账号钉选（accountPin）：按号隔离缓存的模型锁死一个号；满号后重锁到三窗最宽裕的号 ——
test('per-model account pin: single-account lock, widest-headroom relock, no bounce-back, save keeps it', async () => {
  const app = backend({ 'config.json': JSON.stringify({ port: 3123, accountMode: 'roundrobin', accounts: [
    { name: 'a@x', key: 'sk_a', enabled: true },
    { name: 'b@x', key: 'sk_b', enabled: true },
    { name: 'c@x', key: 'sk_c', enabled: true },
  ], perModel: { 'p/k3': { accountPin: 'b@x' } } }) });
  // 钉住的模型：连续请求都落 b，且不吃轮询计数器；未钉模型照旧轮询
  assert.equal(app.run(`pickAccount('p/k3').name`), 'b@x');
  assert.equal(app.run(`pickAccount('p/k3').name`), 'b@x');
  assert.equal(app.run(`pickAccount('p/other').name`), 'a@x');
  assert.equal(app.run(`pickAccount('p/other').name`), 'b@x');
  assert.equal(app.run('pickAccount().name'), 'c@x');
  // 三窗宽裕度纯函数：取「最先撞墙的窗口还剩多少」最大者；5h/周 ≥90% 的号有替代时先跳过
  const row = (n, h, w, m) => JSON.stringify({ name: n, source: 'official', caps: {
    fiveHour: { percentUsed: h, limitUsd: 10 }, weekly: { percentUsed: w, limitUsd: 25 }, monthly: { percentUsed: m, limitUsd: 50 } } });
  const seeds = JSON.stringify(['a@x', 'b@x', 'c@x'].map((n) => ({ name: n, key: 'sk_x', enabled: true })));
  const widest = (rows) => app.run(`((x) => (x ? x.name : null))(pickWidestAccount(${seeds}, [${rows}]))`);
  assert.equal(widest([row('a@x', 0, 0, 0), row('b@x', 50, 50, 50), row('c@x', 20, 92, 46)]), 'a@x', '全空账号宽裕度最大（c 周 92% 先跳过）');
  assert.equal(widest([row('a@x', 95, 0, 0), row('b@x', 40, 40, 40)]), 'b@x', '临近 5h 墙的先跳过');
  assert.equal(widest([row('a@x', 95, 0, 0), row('b@x', 0, 91, 0), row('c@x', 85, 95, 0)]), 'b@x', '个个临墙时退一步按宽裕度挑最大者');
  // 官方数据缺 → 按本地账本近 5h 用量最小的号挑（别把刚烧穿的号再挑出来）
  app.run(`META.usage = [{ account: 'a@x', ts: Date.now(), total: 900000 }, { account: 'b@x', ts: Date.now(), total: 950000 }]`);
  assert.equal(widest([row('a@x', null, 0, 0)]), 'c@x', '无官方数据时按本地 5h 用量挑最小');
  // 行窗口清空也挡不住"刚烧穿"的事实：周窗兜底改读持久账本（2026-10-12 账本修复带来的语义）
  app.run('META.usage = []');
  assert.equal(widest([row('a@x', null, 0, 0)]), 'c@x', '本地兜底读持久账本：刚烧过的号不会被再挑出来');
  // 连账本一起清空（回到"无任何本地数据"），后面走列表内兜底
  app.run('META.usageTotals = null; META.usageDaily = null;');
  // 首选被熔断 → 重锁（无官方数据时退到列表内可用号），并粘住不轮询
  assert.equal(app.run(`noteAccountFailure('b@x', 'quota', 'monthly cap reached')`), true);
  assert.equal(app.run(`pickAccount('p/k3').name`), 'a@x');
  app.run(`planCache = { at: Date.now(), data: { accounts: [${row('a@x', 60, 60, 60)}, ${row('c@x', 0, 0, 0)}] } }`);
  assert.equal(app.run(`pickAccount('p/k3').name`), 'a@x', '已重锁的号粘住，别的号更宽裕也不漂移');
  // 粘住的号也满了 → 重锁到三窗最宽裕的号（c 全空，宽裕度 min(10, 25, 50) 最大）
  assert.equal(app.run(`noteAccountFailure('a@x', 'quota', 'cap')`), true);
  assert.equal(app.run(`pickAccount('p/k3').name`), 'c@x');
  // 首选 b 恢复启用也不弹回（避免一次整包冷启动）；换钉子才立刻重锁
  app.run(`config.accounts[1].enabled = true`);
  assert.equal(app.run(`pickAccount('p/k3').name`), 'c@x', '首选恢复不弹回');
  app.run(`config.perModel['p/k3'].accountPin = 'b@x'`);
  assert.equal(app.run(`pickAccount('p/k3').name`), 'c@x', '同名钉子不算换');
  app.run(`config.accounts[0].enabled = true`);
  app.run(`config.perModel['p/k3'].accountPin = 'a@x'`);
  assert.equal(app.run(`pickAccount('p/k3').name`), 'a@x', '面板换钉子 → 立刻重锁到新钉子');
  assert.equal(app.run('ACCOUNT_PIN_STICKY.size'), 0, '换钉子后旧重锁记录清掉');
  // 面板状态：重锁中要报出来（pinned = 钉子，current = 当前实际锁的号）
  assert.equal(app.run(`JSON.stringify(pinStateOf('p/k3'))`), JSON.stringify({ pinned: 'a@x', current: 'a@x', relocked: false }));
  app.run(`config.accounts[0].enabled = false`);
  app.run(`planCache = { at: Date.now(), data: { accounts: [${row('b@x', 10, 10, 10)}, ${row('c@x', 0, 0, 0)}] } }`);
  assert.equal(app.run(`pickAccount('p/k3').name`), 'c@x', 'b 宽裕度 $9 < c $10 → 重锁 c');
  assert.equal(app.run(`JSON.stringify(pinStateOf('p/k3'))`), JSON.stringify({ pinned: 'a@x', current: 'c@x', relocked: true }));
  // 面板保存该模型上游配置时 accountPin 原样保留；显式 null 才解除
  await app.post('/api/config', { perModel: { 'p/k3': { upstreams: ['x'], exclude: [], pinMode: 'strict' } } });
  assert.equal(JSON.parse(app.files.get('config.json')).perModel['p/k3'].accountPin, 'a@x');
  assert.equal(app.run(`config.perModel['p/k3'].upstreams.join('|')`), 'x');
  await app.post('/api/config', { perModel: { 'p/k3': { upstreams: [], exclude: [], pinMode: 'strict', accountPin: null } } });
  assert.equal(JSON.parse(app.files.get('config.json')).perModel['p/k3'].accountPin, null);
  // 面板接线：机房页每个模型有账号钉选下拉；重锁状态有提示；旧版服务保存失败会给明确提示
  assert.match(page, /账号钉选（留空=正常轮询）/);
  assert.match(page, /class="acc-pin"/);
  assert.match(page, /已重锁 → /);
  assert.match(page, /账号钉选没有保存上：服务是旧版本/);
});

// —— 禁用号自动恢复：退避节奏、重置时间参考、痕迹不被保存动作抹掉、复检能放回轮询 ——
test('auto-recover: backoff, reset reference, disable traces survive save, recover-now re-enables', async () => {
  const app = backend({ 'config.json': JSON.stringify({ port: 3123, accountMode: 'roundrobin', accounts: [
    { name: 'a@x', key: 'sk_a', enabled: true },
    { name: 'b@x', key: 'sk_b', enabled: true },
  ] }) });

  // 退避：15 分钟起步，逐次翻倍，2 小时封顶
  assert.equal(app.run('autoRecoverDelayMs(1)'), 15 * 60_000);
  assert.equal(app.run('autoRecoverDelayMs(2)'), 30 * 60_000);
  assert.equal(app.run('autoRecoverDelayMs(3)'), 60 * 60_000);
  assert.equal(app.run('autoRecoverDelayMs(9)'), 2 * 3600_000);

  // 官方窗口重置参考：取最早的未来时间点，全过期则取最晚
  const future = { caps: {
    fiveHour: { resetsAt: '2099-01-03T00:00:00Z' },
    weekly: { resetsAt: '2099-01-01T00:00:00Z' },
    monthly: { resetsAt: '2099-01-02T00:00:00Z' },
  } };
  assert.equal(app.run(`nextResetAtOf(${JSON.stringify(future)})`), '2099-01-01T00:00:00.000Z');
  assert.equal(app.run('nextResetAtOf({ caps: {} })'), null);

  // 熔断时落下复检进度，且 quotaResetAt 会写进 config
  assert.equal(app.run(`noteAccountFailure('a@x', 'quota', 'monthly cap reached')`), true);
  assert.equal(app.run("config.accounts[0].disabledKind"), 'quota');
  assert.ok(app.run('Date.parse(config.accounts[0].recoverNextAt) > Date.now()'), '首次复检排在未来');
  app.run("config.accounts[0].quotaResetAt = '2099-01-01T00:00:00.000Z'");
  app.run('saveConfig()');

  // 整体保存账号池（机房页「保存账号配置」）不许把禁用痕迹抹掉
  const saved = await app.post('/api/accounts', { accounts: [
    { name: 'a@x', key: 'sk_a', enabled: false, renewStart: null },
    { name: 'b@x', key: 'sk_b', enabled: true, renewStart: null },
  ], mode: 'roundrobin', active: 0 });
  assert.equal(saved.status, 200);
  assert.equal(app.run("config.accounts[0].disabledKind"), 'quota', 'disabledKind 保留');
  assert.match(String(app.run('config.accounts[0].disabledReason')), /额度满/, '禁用原因保留');
  assert.equal(app.run("config.accounts[0].quotaResetAt"), '2099-01-01T00:00:00.000Z', '重置参考保留');
  assert.equal(app.run("config.accounts[1].disabledReason"), null, '启用的号不带禁用痕迹');

  // 自动恢复开关随保存走
  assert.equal(app.run('config.autoRecover'), true, '默认开');
  await app.post('/api/accounts', { accounts: [
    { name: 'a@x', key: 'sk_a', enabled: false }, { name: 'b@x', key: 'sk_b', enabled: true },
  ], mode: 'roundrobin', active: 0, autoRecover: false });
  assert.equal(app.run('config.autoRecover'), false);

  // auth 类不自动恢复：复检名单里不出现
  app.run("config.accounts[0].disabledKind = 'auth'");
  app.run("config.accounts[0].recoverNextAt = null");
  assert.equal(app.run(`(config.accounts || []).filter((a) => a.enabled === false && a.disabledKind !== 'auth').length`), 0);

  // 老记录没有 disabledKind 字段：从原因文案认回来，密钥坏的号不重试
  assert.equal(app.run(`effectiveDisabledKind({ disabledReason: '密钥无效（自动禁用，需换 key） · Unauthorized' })`), 'auth');
  assert.equal(app.run(`effectiveDisabledKind({ disabledReason: '额度满/限流（自动禁用，恢复后自动放回轮询）' })`), 'quota');
  assert.equal(app.run(`effectiveDisabledKind({ disabledKind: 'quota', disabledReason: '密钥无效' })`), 'quota', '显式字段优先');

  // 复检：沙箱 fetch 抛错 = 探不通 → 不恢复，只累计次数并推迟下次
  app.run("config.accounts[0].disabledKind = 'quota'");
  app.run("config.autoRecover = true");
  const now = await app.post('/api/accounts/recover-now', {});
  assert.equal(now.status, 200);
  assert.equal(app.run('config.accounts[0].enabled'), false, '探不通不恢复');
  assert.equal(app.run('config.accounts[0].recoverTries'), 1, '失败累计次数');
  assert.ok(app.run('Date.parse(config.accounts[0].recoverNextAt) > Date.now()'), '失败后推迟下次');
  assert.ok(now.body.accounts.some((x) => x.recoverLastAt), '复检时间回给前端');

  // 探得通 → 自己放回轮询，痕迹清空
  app.context.fetch = async () => ({ status: 200, text: async () => JSON.stringify({ choices: [] }) });
  await app.post('/api/accounts/recover-now', {});
  assert.equal(app.run('config.accounts[0].enabled'), true, '通了就自动启用');
  assert.equal(app.run('config.accounts[0].disabledKind'), null);
  assert.equal(app.run('config.accounts[0].disabledReason'), null);
  assert.ok(app.run('config.accounts[0].recoveredAt'), '记录恢复时刻');
  assert.equal(app.run('pickAccount().name'), 'a@x', '回到轮询池');
});



// ---------- 2026-10-04：流式中继（终止帧扣留 + 空流/断流自动换上游重试）----------
function sseFrames(lines) {
  return lines.join('\n\n') + '\n\n';
}

test('stream relay helpers: usable output and terminal frame classification', () => {
  const app = backend();
  assert.equal(app.run(`streamHasOutput('data: {"choices":[{"delta":{"reasoning":"x"}}]}')`), false);
  assert.equal(app.run(`streamHasOutput('data: {"choices":[{"delta":{"content":"x"}}]}')`), true);
  assert.equal(app.run(`streamHasOutput('data: {"choices":[{"delta":{"tool_calls":[{"index":0}]}}]}')`), true);
  assert.equal(app.run(`streamHasOutput('data: {"choices":[{"delta":{"content":""}}]}')`), false);
  assert.equal(app.run(`isTerminalFrame('data: {"choices":[{"finish_reason":"stop"}]}')`), true);
  assert.equal(app.run(`isTerminalFrame('data: {"choices":[{"finish_reason":null}]}')`), false);
  assert.equal(app.run(`isTerminalFrame('data: [DONE]')`), true);
  assert.equal(app.run(`isTerminalFrame('data: {"usage":{"prompt_tokens":1}}')`), true);
});

test('stream relay: an empty round is retried with a fresh upstream call, terminals held from the client', async () => {
  const app = backend();
  const emptyRound = sseFrames([
    'data: {"id":"g1","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}',
    'data: {"id":"g1","choices":[{"index":0,"delta":{"reasoning":"想了半天"},"finish_reason":null}]}',
    'data: {"id":"g1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18},"provider_metadata":{"gateway":{"routing":{"finalProvider":"deepseek","canonicalSlug":"deepseek/deepseek-v4.1-flash"}}}}',
    'data: [DONE]',
  ]);
  const goodRound = sseFrames([
    'data: {"id":"g2","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}',
    'data: {"id":"g2","choices":[{"index":0,"delta":{"content":"你好"},"finish_reason":null}]}',
    'data: {"id":"g2","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":3,"total_tokens":15}}',
    'data: [DONE]',
  ]);
  app.run(`
    globalThis.__round = 0;
    globalThis.__bodies = ${JSON.stringify([emptyRound, goodRound])};
    runChatChain = async (req, body, modelId, cfg, opts) => {
      const n = ++globalThis.__round;
      const frames = globalThis.__bodies[n - 1] || globalThis.__bodies[0];
      return {
        status: 200,
        streamUp: { status: 200, headers: { get: () => 'text/event-stream' },
          body: { async *[Symbol.asyncIterator]() { yield Buffer.from(frames); } } },
        streamHead: null, acc: { name: 'acc' + n },
        trace: [{ upstream: 'deepseek', status: 200, ms: 1, note: 'stream' }],
        t0: Date.now(), state: { clientGone: false },
      };
    };
  `);
  const res = await app.post('/v1/chat/completions', { model: 'cline-pass/deepseek-v4.1-flash', stream: true, messages: [{ role: 'user', content: 'hi' }] });
  assert.equal(res.status, 200);
  assert.equal(app.run('globalThis.__round'), 2, '空流轮之后应换上游重试一次');
  const wire = res.writes.join('');
  assert.ok(wire.includes('你好'), '第二轮的正文要送达');
  assert.ok(!wire.includes('"prompt_tokens":11'), '第一轮扣下的终止帧不许放行');
  assert.ok(wire.includes('"prompt_tokens":12'), '第二轮的终止帧照常放行');
  assert.equal((wire.match(/\[DONE\]/g) || []).length, 1, '客户端只应看到一个 [DONE]');
  assert.equal(res.ended, true);
  const usage = JSON.parse(app.files.get('metadata.json')).usage;
  assert.equal(usage.length, 2);
  assert.equal(usage[0].error, null);
  assert.equal(usage[0].prompt, 12);
  assert.equal(usage[1].error, 'stream ended without content');
  assert.equal(usage[1].prompt, 11);
});

test('stream relay: tool-call turns are output (no retry, no error row)', async () => {
  const app = backend();
  const toolRound = sseFrames([
    'data: {"id":"g1","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}',
    'data: {"id":"g1","choices":[{"index":0,"delta":{"reasoning":"查天气"},"finish_reason":null}]}',
    'data: {"id":"g1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"get_weather","arguments":"{}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":21,"completion_tokens":5,"total_tokens":26}}',
    'data: [DONE]',
  ]);
  app.run(`
    globalThis.__round = 0;
    runChatChain = async (req, body, modelId, cfg, opts) => {
      globalThis.__round++;
      return {
        status: 200,
        streamUp: { status: 200, headers: { get: () => 'text/event-stream' },
          body: { async *[Symbol.asyncIterator]() { yield Buffer.from(${JSON.stringify(toolRound)}); } } },
        streamHead: null, acc: { name: 'acc1' },
        trace: [{ upstream: 'deepseek', status: 200, ms: 1, note: 'stream' }],
        t0: Date.now(), state: { clientGone: false },
      };
    };
  `);
  const res = await app.post('/v1/chat/completions', { model: 'cline-pass/deepseek-v4.1-flash', stream: true, messages: [{ role: 'user', content: '天气' }] });
  assert.equal(res.status, 200);
  assert.equal(app.run('globalThis.__round'), 1, '工具调用流不许被判成空流');
  const wire = res.writes.join('');
  assert.ok(wire.includes('"tool_calls"'));
  assert.ok(wire.includes('[DONE]'));
  const usage = JSON.parse(app.files.get('metadata.json')).usage;
  assert.equal(usage.length, 1);
  assert.equal(usage[0].error, null);
});

test('stream relay: a stream cut after content gets a synthesized terminal frame', async () => {
  const app = backend();
  const cutRound = sseFrames([
    'data: {"id":"g1","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}',
    'data: {"id":"g1","choices":[{"index":0,"delta":{"content":"只写了一半"},"finish_reason":null}]}',
  ]);
  app.run(`
    globalThis.__round = 0;
    runChatChain = async (req, body, modelId, cfg, opts) => {
      globalThis.__round++;
      return {
        status: 200,
        streamUp: { status: 200, headers: { get: () => 'text/event-stream' },
          body: { async *[Symbol.asyncIterator]() { yield Buffer.from(${JSON.stringify(cutRound)}); } } },
        streamHead: null, acc: { name: 'acc1' },
        trace: [{ upstream: 'deepseek', status: 200, ms: 1, note: 'stream' }],
        t0: Date.now(), state: { clientGone: false },
      };
    };
  `);
  const res = await app.post('/v1/chat/completions', { model: 'cline-pass/deepseek-v4.1-flash', stream: true, messages: [{ role: 'user', content: 'hi' }] });
  assert.equal(res.status, 200);
  assert.equal(app.run('globalThis.__round'), 1, '已经有正文的流不重试');
  const wire = res.writes.join('');
  assert.ok(wire.includes('只写了一半'));
  assert.ok(wire.includes('"finish_reason":"stop"'), '补一个人工终止帧');
  assert.ok(wire.includes('[DONE]'));
  assert.equal(res.ended, true);
  // 被掐断的流没有 usage 块：照旧不产生 usage 行，但 history 里要留一笔成功记录
  const meta = JSON.parse(app.files.get('metadata.json'));
  assert.equal(meta.usage.length, 0);
  assert.equal(meta.history[0].error, null);
  assert.equal(meta.history[0].stream, true);
});

test('stream relay: exhausting all rounds fails loudly with an explicit error frame', async () => {
  const app = backend();
  const emptyRound = sseFrames([
    'data: {"id":"g1","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}',
    'data: {"id":"g1","choices":[{"index":0,"delta":{"reasoning":"空转"},"finish_reason":null}]}',
    'data: {"id":"g1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":9,"completion_tokens":2,"total_tokens":11}}',
    'data: [DONE]',
  ]);
  app.run(`
    globalThis.__round = 0;
    runChatChain = async (req, body, modelId, cfg, opts) => {
      globalThis.__round++;
      return {
        status: 200,
        streamUp: { status: 200, headers: { get: () => 'text/event-stream' },
          body: { async *[Symbol.asyncIterator]() { yield Buffer.from(${JSON.stringify(emptyRound)}); } } },
        streamHead: null, acc: { name: 'acc' + globalThis.__round },
        trace: [{ upstream: 'deepseek', status: 200, ms: 1, note: 'stream' }],
        t0: Date.now(), state: { clientGone: false },
      };
    };
  `);
  const res = await app.post('/v1/chat/completions', { model: 'cline-pass/deepseek-v4.1-flash', stream: true, messages: [{ role: 'user', content: 'hi' }] });
  assert.equal(res.status, 200);
  assert.equal(app.run('globalThis.__round'), 3, '最多三轮（1 首发 + 2 重试）');
  const wire = res.writes.join('');
  assert.ok(wire.includes('"error"'), '放弃时给客户端一个明确的错误帧');
  assert.equal((wire.match(/\[DONE\]/g) || []).length, 1);
  assert.equal(res.ended, true);
  const usage = JSON.parse(app.files.get('metadata.json')).usage;
  assert.equal(usage.length, 3);
  assert.ok(usage.every((row) => row.error === 'stream ended without content'));
});
