// Cline Pass 上游观察/切换代理
// 零依赖，Node >= 18。
//
// 网关行为（实测结论，README 有证据）：
// - 订阅模型（cline-pass/*）与非 free 目录模型：请求体里的 provider.* 会被 Cline 网关丢弃，
//   由其规划器在系统凭证上游中自行挑选，响应元数据可回读实际上游。
// - 目录模型 :free 变体：provider.only 真正透传到 OpenRouter，可精确钉住。
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { Readable, Transform } from 'node:stream';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.DATA_DIR || __dirname;
const CONFIG_PATH = path.join(DATA_DIR, 'config.json');
const META_PATH = path.join(DATA_DIR, 'metadata.json');
const PUBLIC_DIR = path.join(__dirname, 'public');

// 事件日志：失败必须留痕。流式中途断掉的请求原来完全不进账本（record 不被调），案发无迹可寻；
// 追加写 DATA_DIR/proxy-events.log（异步不阻塞、写坏了也不影响主流程），进程级异常同批入档。
const EVENTS_PATH = path.join(DATA_DIR, 'proxy-events.log');
function logEvent(line) {
  const out = `${new Date().toISOString()} ${line}\n`;
  try {
    fs.stat?.(EVENTS_PATH, (err, st) => { if (!err && st.size > 5 * 1024 * 1024) { try { fs.renameSync(EVENTS_PATH, `${EVENTS_PATH}.1`); } catch {} } });
    fs.appendFile?.(EVENTS_PATH, out, () => {});
  } catch { /* 日志失败不影响主流程 */ }
  try { console.error?.(out.trim()); } catch {}
}

// 进程级兜底：单个请求或上游异常绝不允许把整个代理带下去
// （Node 15+ 起，未处理的 Promise 拒绝默认直接以非零码退出进程 = 控制台"自己挂掉"）
process.on?.('unhandledRejection', (e) => { logEvent(`unhandledRejection ${e?.stack || e}`); });
process.on?.('uncaughtException', (e) => { logEvent(`uncaughtException ${e?.stack || e}`); });

const DEFAULT_CONFIG = {
  port: 3123,
  apiKey: '',
  proxyKey: '',
  publicBaseUrl: '',
  exposeCatalog: false,    // true 时 /v1/models 合并完整目录模型（默认仅订阅模型）
  upstreamBase: 'https://api.cline.bot/api/v1',
  accounts: [],            // { name, key, enabled } —— Cline Pass 账号池
  autoRecover: true,       // 被自动禁用的号按退避节奏复检，能用了自己放回轮询（false=只报警不动手）
  accountMode: 'single',   // single=手动指定 | roundrobin=轮询 | smart=额度调度（本月用得最少的优先）
  activeAccount: 0,        // single 模式下使用的账号下标
  knownModels: [
    'cline-pass/astrbot-auto',
    'cline-pass/glm-5.3-flash',
    'cline-pass/kimi-k3',
    'cline-pass/deepseek-v4.1-flash',
    'cline-pass/deepseek-v4-flash',
    'cline-pass/qwen3.8-max',
    'cline-pass/minimax-m3',
    'cline-pass/glm-5.3',
    'cline-pass/glm-5.2',
    'cline-pass/deepseek-v4-pro',
    'cline-pass/mimo-v2.5-pro',
    'cline-pass/mimo-v2.5',
    'cline-pass/kimi-k2.6',
    'cline-pass/qwen3.7-plus',
    'cline-pass/kimi-k2.7-code',
    'cline-pass/qwen3.7-max',
  ],
  // modelId -> { upstreams: string[]（有序优先列表，空=自动）, exclude: string[]（排除列表，优先级高于勾选）,
  //              pinMode: 'strict'|'preferred', sort: 'cost'|'ttft'|'tps'|null,
  //              accountPin: string|null（账号钉选：该模型的请求只发给这一个号。k3 走 private/k3-contributor
  //                          私有池，提示缓存按号隔离——实测跨号命中 0 笔、同号命中 ~2/3，换号=整包重烧；
  //                          空/未设 = 正常轮询） }
  // 请求按 upstreams 顺序逐个钉住尝试：第一个异常（非 200 / 网络失败 / 超时）自动顺切下一个，
  // 全部失败才把最后一个错误透传给客户端；exclude 中的上游永不被使用（自动模式下注入排除偏好）。
  // upstream 为旧版单上游兼容镜像（取列表第一个），maxRetries 已退役（旧值仅作回滚兼容保留在文件里）。
  perModel: {
    // 开箱默认：DeepSeek v4.1 Flash 只走 deepseek 直营上游。
    // 同类网关里它的首字和吞吐最稳，其它托管商质量差距大；新装用户不用再手配上游。
    'cline-pass/deepseek-v4.1-flash': { upstreams: ['deepseek'], exclude: [], pinMode: 'strict' },
    'deepseek/deepseek-v4.1-flash-1017': { upstreams: ['deepseek'], exclude: [], pinMode: 'strict' },
  },
};

function loadJson(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return fallback;
  }
}
const config = { ...DEFAULT_CONFIG, ...loadJson(CONFIG_PATH, {}) };
const META = loadJson(META_PATH, { models: {}, history: [], usage: [], catalog: null, orModelsFetchedAt: 0, orModelList: null, usageTotals: null, usageDaily: null });
META.usage ||= [];

// AstrBot 专用双档别名：短闲聊走 DeepSeek v4.1 Flash，复杂问题走 GLM 5.3。
// 路由在本地代理完成，不额外调用分类模型，也不改 AstrBot 的会话/人格上下文。
const ASTRBOT_AUTO_MODEL = 'cline-pass/astrbot-auto';
const ASTRBOT_FAST_MODEL = 'cline-pass/deepseek-v4.1-flash';
const ASTRBOT_DEEP_MODEL = 'cline-pass/glm-5.3';
const ASTRBOT_FAST_STYLE_HINT = '快答也要像一个有主意的人：先回应当前问题，再挑一个现场细节，补上自己的判断或偏爱，并落到一个具体的下一步。用短而完整的句子，删掉客服确认、泛泛甜话、重复和元话语。普通正文避开“来”字及其招呼、邀请、推进用法，也不要主动报钟点或睡眠状态；改用具体主语和打算。需要深思的内容由深档处理。';

function messageText(content) {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content
    .filter((part) => part && (part.type === 'text' || part.type === 'input_text'))
    .map((part) => typeof part.text === 'string' ? part.text : '')
    .join('\n');
}

function hasImageContent(content) {
  return Array.isArray(content) && content.some((part) => {
    const type = String(part?.type || '').toLowerCase();
    return type === 'image_url' || type === 'input_image';
  });
}

function shouldUseAstrbotDeep(body) {
  const messages = Array.isArray(body?.messages) ? body.messages : [];
  const lastUser = [...messages].reverse().find((message) => message?.role === 'user');
  if (!lastUser || hasImageContent(lastUser.content)) return false;
  const text = messageText(lastUser.content).trim();
  if (!text) return false;

  // 明确要求深思时立即切深档；其余按内容复杂度确定性判断。
  if (/^\s*[\/／](?:深思|深度|复杂|分析)/u.test(text)) return true;
  if (/(深度思考|详细分析|逐步推理|全面比较|设计方案|排查|调试|报错|堆栈|架构|性能|配置|代码|脚本|实现|修复|为什么|因果|事实核验|长期记忆|回顾)/iu.test(text) && text.length >= 18) return true;
  if (text.length >= 220 || /```/.test(text) || (text.match(/[？?]/g) || []).length >= 2) return true;
  return false;
}

function resolveModelId(modelId, body) {
  // cline-pass 别名走订阅池，改成目录模型名会切到 Cline Credits 按量计费，不能改写。
  return modelId === ASTRBOT_AUTO_MODEL
    ? (shouldUseAstrbotDeep(body) ? ASTRBOT_DEEP_MODEL : ASTRBOT_FAST_MODEL)
    : modelId;
}

function stripAstrbotRouteDirective(body, requestedModelId) {
  if (requestedModelId !== ASTRBOT_AUTO_MODEL || !Array.isArray(body?.messages)) return body;
  const messages = [...body.messages];
  const index = [...messages].map((message, i) => [message, i]).reverse()
    .find(([message]) => message?.role === 'user')?.[1];
  if (index == null) return body;
  const message = messages[index];
  const strip = (value) => value.replace(/^\s*[\/／](?:深思|深度|复杂|分析)\s*/u, '').trim();
  if (typeof message.content === 'string') {
    const content = strip(message.content);
    if (content && content !== message.content) messages[index] = { ...message, content };
  } else if (Array.isArray(message.content)) {
    const parts = [...message.content];
    const partIndex = parts.findIndex((part) => part && (part.type === 'text' || part.type === 'input_text') && typeof part.text === 'string');
    if (partIndex >= 0) {
      const text = strip(parts[partIndex].text);
      if (text && text !== parts[partIndex].text) parts[partIndex] = { ...parts[partIndex], text };
      messages[index] = { ...message, content: parts };
    }
  }
  return { ...body, messages };
}

function enrichAstrbotRouteBody(body, requestedModelId, resolvedModelId) {
  if (requestedModelId !== ASTRBOT_AUTO_MODEL || resolvedModelId !== ASTRBOT_FAST_MODEL) return body;
  const enriched = { ...body };
  // 快档保持短响应；给模型一点采样空间，让一句话里有具体判断和细节。
  if (enriched.temperature == null) enriched.temperature = 0.72;
  if (enriched.top_p == null) enriched.top_p = 0.92;
  const messages = Array.isArray(enriched.messages) ? [...enriched.messages] : [];
  const hint = { role: 'system', content: ASTRBOT_FAST_STYLE_HINT };
  const firstNonSystem = messages.findIndex((message) => message?.role !== 'system');
  if (firstNonSystem < 0) messages.push(hint);
  else messages.splice(firstNonSystem, 0, hint);
  enriched.messages = messages;
  return enriched;
}
// DeepSeek V4 Flash 的补充上游。
const DEEPSEEK_FLASH_UPSTREAMS = [
  'open-inference', 'deepinfra', 'relace', 'sail-research', 'digitalocean',
  'makora', 'wafer', 'reka', 'morph', 'baseten', 'inceptron', 'coreweave',
  'streamlake', 'together', 'baidu', 'parasail', 'mancer', 'venice',
  'fireworks', 'siliconflow', 'gmicloud', 'nextbit', 'alibaba', 'novita',
  'phala', 'atlas-cloud', 'cloudflare', 'deepseek', 'azure',
];
const DEEPSEEK_PASS_UPSTREAMS = [
  'deepinfra', 'digitalocean', 'streamlake', 'baidu', 'parasail', 'mancer',
  'venice', 'siliconflow', 'gmicloud', 'nextbit', 'alibaba', 'novita',
  'phala', 'atlas-cloud', 'deepseek', 'azure',
];
function isDeepseekFlash(modelId) {
  return modelId === 'deepseek/deepseek-v4-flash' || modelId === 'cline-pass/deepseek-v4-flash';
}
function extendDeepseekFlashUpstreams(modelId) {
  if (!isDeepseekFlash(modelId)) return;
  const meta = (META.models[modelId] ||= {});
  const defaults = modelId === 'cline-pass/deepseek-v4-flash' ? DEEPSEEK_PASS_UPSTREAMS : DEEPSEEK_FLASH_UPSTREAMS;
  meta.upstreams = [...new Set([...defaults, ...(meta.upstreams || [])])].filter((u) => !(meta.deletedUpstreams || []).includes(u));
}

function pruneDeletedUpstreams(modelId) {
  const meta = META.models[modelId];
  if (!meta?.deletedUpstreams?.length) return;
  const deleted = new Set(meta.deletedUpstreams);
  for (const field of ['upstreams', 'watchedUpstreams', 'availableProviders', 'tier0']) {
    if (Array.isArray(meta[field])) meta[field] = meta[field].filter((u) => !deleted.has(u));
  }
  for (const field of ['upstreamDetail', 'upstreamStatus']) {
    if (meta[field]) for (const u of deleted) delete meta[field][u];
  }
  for (const field of ['lastProvider', 'provider']) {
    if (deleted.has(meta[field])) delete meta[field];
  }
  const cfg = config.perModel[modelId];
  if (cfg) {
    cfg.upstreams = (cfg.upstreams || []).filter((u) => !deleted.has(u));
    cfg.exclude = (cfg.exclude || []).filter((u) => !deleted.has(u));
    cfg.upstream = cfg.upstreams[0] || null;
  }
}
for (const modelId of ['deepseek/deepseek-v4-flash', 'cline-pass/deepseek-v4-flash']) {
  extendDeepseekFlashUpstreams(modelId);
}

const saveConfig = () => fs.writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2));
const saveMeta = () => fs.writeFileSync(META_PATH, JSON.stringify(META, null, 2));

// 现有配置文件可能早于自动路由创建：启动时补入别名，保证 AstrBot 能从 /v1/models 发现它。
// 只写一次，后续启动不改动用户已有模型顺序。
if (!Array.isArray(config.knownModels)) config.knownModels = [...DEFAULT_CONFIG.knownModels];
if (!config.knownModels.includes(ASTRBOT_AUTO_MODEL)) {
  config.knownModels.unshift(ASTRBOT_AUTO_MODEL);
  saveConfig();
}

// 旧版单 apiKey 迁移为账号池
if ((!Array.isArray(config.accounts) || config.accounts.length === 0) && config.apiKey) {
  config.accounts = [{ name: '默认账号', key: config.apiKey, enabled: true }];
  config.accountMode = 'single';
  config.activeAccount = 0;
  saveConfig();
}
config.accountMode = ['roundrobin', 'smart'].includes(config.accountMode) ? config.accountMode : 'single';

// 账号 schema 迁移：补 addedAt（导入时间，用于月付到期估算）；renewStart 为手动修正的订阅日起算日。
// V1 server.js 共用 config.json 且整体替换 accounts 时会丢掉这些字段，所以每次启动幂等补齐。
(function migrateAccounts() {
  let dirty = false;
  for (const a of Array.isArray(config.accounts) ? config.accounts : []) {
    if (a && typeof a === 'object') {
      if (!a.addedAt) { a.addedAt = new Date().toISOString(); dirty = true; }
    }
  }
  if (dirty) saveConfig();
})();

// perModel 配置升级：旧版单 upstream 迁移为有序多上游列表（upstream 保留为回滚兼容镜像）
(function migratePerModel() {
  let dirty = false;
  for (const c of Object.values(config.perModel || {})) {
    if (!c || typeof c !== 'object') continue;
    if (c.upstreams === undefined) { c.upstreams = c.upstream ? [c.upstream] : []; dirty = true; }
    if (c.exclude === undefined) { c.exclude = []; dirty = true; }
    if (!Array.isArray(c.upstreams)) { c.upstreams = []; dirty = true; }
    if (!Array.isArray(c.exclude)) { c.exclude = []; dirty = true; }
  }
  if (dirty) saveConfig();
})();

for (const modelId of Object.keys(META.models)) pruneDeletedUpstreams(modelId);

// 环境变量覆盖（便于 Docker 部署）。注意：此后若通过控制台保存设置，当前生效值会写回 config.json
if (process.env.CLINE_PASS_KEY) {
  const k = process.env.CLINE_PASS_KEY.trim();
  if (k && !(config.accounts || []).some((a) => a.key === k)) {
    config.accounts = [{ name: 'env-account', key: k, enabled: true }, ...(config.accounts || [])];
  }
}
if (process.env.PROXY_KEY && process.env.PROXY_KEY.trim()) config.proxyKey = process.env.PROXY_KEY.trim();
if (process.env.PUBLIC_BASE_URL) config.publicBaseUrl = process.env.PUBLIC_BASE_URL.trim();
if (process.env.PORT) config.port = Number(process.env.PORT) || config.port;

function isConfigured() {
  return !!config.apiKey || enabledAccounts().length > 0;
}
if (!isConfigured()) {
  console.warn('[提示] 尚未配置上游 API Key：打开控制台「账号管理」添加账号并保存即可；服务已启动。');
}

// 账号选择：roundrobin 在启用的账号间轮询；smart 按官方本月额度用量调度（用得最少的优先，用量自动拉平）；
// single 使用 activeAccount 指定的账号
let RR_COUNTER = 0;
let SMART_COUNTER = 0;
function enabledAccounts() {
  return (config.accounts || []).filter((a) => a && a.key && a.enabled !== false);
}

// 额度调度的选号核心（纯函数，便于测试）：从官方账本里挑本月 percentUsed 最小的账号——
// 谁本月用得少，下一个请求就给谁，几个号自动追平；并列的按 counter 轮着来（同一百分点不钉死一个号）。
// 本月打满或 5h/周窗口 >=90% 的先跳过（顶格的号先让位）；退一步只要求本月未打满；
// 全打满或所有账号都没有官方数据时返回 null —— 调用方退回轮询，绝不把请求吊死。
function pickSmartAccount(list, planAccounts, counter) {
  const byName = new Map((Array.isArray(planAccounts) ? planAccounts : []).filter(Boolean).map((e) => [e.name, e]));
  const pct = (w) => (w && typeof w.percentUsed === 'number' ? w.percentUsed : null);
  const known = [];
  for (const acc of list) {
    const e = byName.get(acc.name);
    const caps = e && e.source === 'official' && e.caps ? e.caps : null;
    const m = caps ? pct(caps.monthly) : null;
    if (m === null) continue; // 官方数据缺（降级 local）的账号不进额度调度
    known.push({ acc, m, h: pct(caps.fiveHour) ?? 0, w: pct(caps.weekly) ?? 0 });
  }
  if (!known.length) return null;
  let pool = known.filter((k) => k.m < 100 && k.h < 90 && k.w < 90);
  if (!pool.length) pool = known.filter((k) => k.m < 100);
  if (!pool.length) return null; // 全部打满：退回轮询，交给熔断处理
  const min = Math.min(...pool.map((k) => k.m));
  const tied = pool.filter((k) => k.m === min).map((k) => k.acc);
  return tied[Math.abs(counter) % tied.length];
}

// 账号钉选的「粘性重锁」：钉住的号被熔断（额度满/密钥坏）时，同一个模型重新锁到
// 「5h/周/月三窗最宽裕」的那个号上并粘住（绝不退回轮询——k3 每换一次号 = 一次整包冷启动，
// 实测 215k 上下文一次 $1.29）；面板改钉子会立刻作废旧锁、按新钉子重锁。
const ACCOUNT_PIN_STICKY = new Map(); // modelId -> { from: 重锁时的钉子名, name: 当前锁定的号 }

// 重锁目标：当前锁的号满了之后挑下一个号用。优先级——
//   ①官方账本里 5h/周都还没到 90% 的号，按「最先撞墙的窗口还剩多少美元」最大者；
//   ②官方数据缺、但本地账本近 5h 用量最小的号（没数据就求别把刚烧穿的号再挑出来）；
//   ③官方数据全但个个临墙：退一步按宽裕度挑最大者；④调用方兜底 list[0]。
// 不许撞周和 5h 的墙：①优先于③就是这条规矩的落地。
function pickWidestAccount(list, planAccounts) {
  const byName = new Map((Array.isArray(planAccounts) ? planAccounts : []).filter(Boolean).map((e) => [e.name, e]));
  const known = [], unknown = [];
  for (const acc of list) {
    const e = byName.get(acc.name);
    const caps = e && e.source === 'official' && e.caps ? e.caps : null;
    if (!caps) { unknown.push(acc); continue; }
    const left = (w, defUsd) => {
      const p = w && typeof w.percentUsed === 'number' ? w.percentUsed : null;
      if (p === null) return null;
      const cap = typeof w.limitUsd === 'number' && w.limitUsd > 0 ? w.limitUsd : defUsd;
      return ((100 - p) / 100) * cap;
    };
    const five = left(caps.fiveHour, 10), week = left(caps.weekly, 25), month = left(caps.monthly, 50);
    if (five === null || week === null || month === null) { unknown.push(acc); continue; }
    const pct = (w) => (w && typeof w.percentUsed === 'number' ? w.percentUsed : 0);
    known.push({ acc, week, month, headroom: Math.min(five, week, month), hPct: pct(caps.fiveHour), wPct: pct(caps.weekly) });
  }
  const best = (rows) => rows.slice().sort((a, b) => (b.headroom - a.headroom) || (b.week - a.week) || (b.month - a.month))[0].acc;
  const safe = known.filter((r) => r.hPct < 90 && r.wPct < 90);
  if (safe.length) return best(safe);
  if (unknown.length) {
    const rows = unknown
      .map((a) => { const t = localWindowTotals(META.usage, a.name); return { a, h: t.fiveHour, w: t.weekly }; })
      .sort((x, y) => (x.h - y.h) || (x.w - y.w));
    return rows[0].a;
  }
  if (known.length) return best(known);
  return null;
}

function pinnedAccountFor(modelId, list) {
  const pin = modelId ? config.perModel?.[modelId]?.accountPin : null;
  if (!pin) { ACCOUNT_PIN_STICKY.delete(modelId); return null; }
  const rec = ACCOUNT_PIN_STICKY.get(modelId);
  if (rec && rec.from !== pin) ACCOUNT_PIN_STICKY.delete(modelId); // 面板换了钉子 → 作废旧锁
  const held = ACCOUNT_PIN_STICKY.get(modelId);
  const heldAcc = held ? list.find((a) => a.name === held.name) : null;
  if (heldAcc) return heldAcc;                      // 已重锁：粘住，直到它自己也满/不可用
  if (!held) {
    const pref = list.find((a) => a.name === pin);
    if (pref) return pref;                          // 首选可用 → 就用首选
  }
  // 锁定的号不可用了（满了/熔断）→ 重锁到三窗最宽裕的号：选新家时避开快撞周/5h 墙的
  maybeRefreshPlanData();
  const alt = pickWidestAccount(list, planCache.data ? planCache.data.accounts : null) || list[0] || null;
  if (!alt) return null;
  if (alt.name === pin) ACCOUNT_PIN_STICKY.delete(modelId);
  else ACCOUNT_PIN_STICKY.set(modelId, { from: pin, name: alt.name });
  logEvent(`account-pin-relock model=${modelId} pin=${pin} locked=${alt.name}`);
  return alt;
}

// 面板展示用：这个模型钉在谁身上、当前实际锁的是谁
function pinStateOf(modelId) {
  const pin = config.perModel?.[modelId]?.accountPin || null;
  if (!pin) return null;
  const held = ACCOUNT_PIN_STICKY.get(modelId);
  const relocked = !!(held && held.from === pin);
  const current = relocked && enabledAccounts().some((a) => a.name === held.name) ? held.name : pin;
  return { pinned: pin, current, relocked };
}

function pickAccount(modelId) {
  const list = enabledAccounts();
  if (!list.length) return { name: '默认', key: config.apiKey || '' };
  // 模型级账号钉选（perModel.accountPin）：该模型的请求只发给「一个」号——k3 的提示缓存按号
  // 隔离（跨号命中实测 0 笔），乱换号 = 整个前缀按 $6/M 重烧，钉住才能稳定吃缓存读价（$0.6/M）。
  // 钉住的号满额/熔断时由 pinnedAccountFor 重锁到最宽裕的号，绝不退回轮询把缓存打散。
  const pinned = pinnedAccountFor(modelId, list);
  if (pinned) return pinned;
  if (list.length > 1 && (config.accountMode === 'roundrobin' || config.accountMode === 'smart')) {
    if (config.accountMode === 'smart') {
      maybeRefreshPlanData();
      const chosen = pickSmartAccount(list, planCache.data ? planCache.data.accounts : null, SMART_COUNTER);
      if (chosen) { SMART_COUNTER = (SMART_COUNTER + 1) % 1000000000; return chosen; }
    }
    const a = list[RR_COUNTER % list.length];
    RR_COUNTER = (RR_COUNTER + 1) % 1000000000;
    return a;
  }
  const byIdx = config.accounts[config.activeAccount];
  if (byIdx && byIdx.key && byIdx.enabled !== false) return byIdx;
  return list[0];
}
const chatHeaders = (key) => ({
  'Content-Type': 'application/json',
  Authorization: `Bearer ${key}`,
});

// ---------- 账号熔断：额度满 / 密钥坏的账号自动禁用，绝不拖累其他号 ----------
// 分类：'auth'=密钥无效（永久禁用，人工换 key）；'quota'=额度满/限流（禁用，窗口重置或手动恢复）；null=普通失败不禁（网络/上游问题不是账号的错）
function classifyAccountError(status, msg) {
  const s = String(msg || '');
  if (status === 401 || status === 403 || /unauthorized|invalid\s*(api\s*)?(key|token)|api key/i.test(s)) return 'auth';
  if (status === 429 || /rate.?limit|quota|insufficient|exceed|credit|balance/i.test(s)) return 'quota';
  return null;
}
const ACCOUNT_DISABLE_REASON = {
  auth: '密钥无效（自动禁用，需换 key）',
  quota: '额度满/限流（自动禁用，恢复后自动放回轮询）',
};
function noteAccountFailure(name, kind, detail) {
  if (!kind) return false;
  const acc = (config.accounts || []).find((a) => a.name === name);
  if (!acc || acc.enabled === false) return false; // 已禁用/不存在：不重复记
  acc.enabled = false;
  acc.disabledKind = kind; // auth 不自动恢复；其余按退避节奏复检
  acc.disabledReason = ACCOUNT_DISABLE_REASON[kind] + (detail ? ' · ' + String(detail).slice(0, 80) : '');
  acc.disabledAt = new Date().toISOString();
  acc.recoverTries = 0;
  acc.recoverNextAt = new Date(Date.now() + AUTO_RECOVER_BASE_MS).toISOString();
  acc.recoveredAt = null;
  saveConfig();
  return true;
}

// ---------- 自动恢复：禁用号按退避节奏轻量复检，一旦真能用了就自己回到轮询 ----------
// 为什么必须靠探测而不是"窗口重置时间"：官方 weekly/monthly 百分比重置 ≠ 账号就能用了
// （实测有号 weekly 才 44% 照样报 insufficient_credits），所以重置时间只当参考展示，
// 放不放回轮询一律以"发一条极小请求能不能通"为准。
const AUTO_RECOVER_BASE_MS = 15 * 60_000;   // 首次复检间隔
const AUTO_RECOVER_MAX_MS = 2 * 3600_000;   // 退避上限
const AUTO_RECOVER_TICK_MS = 60_000;        // 巡查节拍

function autoRecoverDelayMs(tries) {
  const n = Math.max(0, Math.min(6, (Number(tries) || 0) - 1));
  return Math.min(AUTO_RECOVER_MAX_MS, AUTO_RECOVER_BASE_MS * 2 ** n);
}

// 老记录里没有 disabledKind 字段，从原因文案里认回来——否则密钥坏的号也会被无脑重试
function effectiveDisabledKind(acc) {
  if (acc.disabledKind) return acc.disabledKind;
  return /密钥无效/.test(String(acc.disabledReason || '')) ? 'auth' : 'quota';
}

// 一条极小请求，只问"这个号现在能不能通"，不关心返回内容
async function probeAccountUsable(acc) {
  const model = config.knownModels.includes('cline-pass/glm-5.3-flash')
    ? 'cline-pass/glm-5.3-flash'
    : (config.knownModels[0] || 'cline-pass/glm-5.3-flash');
  try {
    const { status, json } = await fetchJSON(`${config.upstreamBase}/chat/completions`, {
      method: 'POST',
      headers: chatHeaders(acc.key),
      body: JSON.stringify({ model, messages: [{ role: 'user', content: 'ping' }], max_tokens: 256 }),
    }, 30000);
    const ok200 = status === 200 && !json?.error;
    const msg = ok200 ? 'ok' : (errText(json?.error) || `HTTP ${status}`);
    // 判据：拿回一个 2xx、且网关没有以账号身份拒绝它，就算这个号能用。
    // 只认这个号自己的 429/401 这类拒绝；5xx/超时/空正文是上游和模型的事，
    // 宁可保守留在禁用里等下一轮，也不要在上游抽风时把号放进轮询来回抖。
    const ok = status >= 200 && status < 300 && classifyAccountError(status, msg) === null;
    return { ok, status, msg, accountBlocked: !ok && status !== 0 };
  } catch (e) {
    return { ok: false, status: 0, msg: String((e && e.message) || e), accountBlocked: false };
  }
}

let autoRecoverRunning = false;
async function autoRecoverTick() {
  if (autoRecoverRunning || config.autoRecover === false) return;
  const now = Date.now();
  const due = (config.accounts || []).filter((a) => a && a.key
    && a.enabled === false
    && effectiveDisabledKind(a) !== 'auth'
    && (!a.recoverNextAt || Date.parse(a.recoverNextAt) <= now));
  if (!due.length) return;
  autoRecoverRunning = true;
  try {
    for (const acc of due) {
      const r = await probeAccountUsable(acc);
      acc.recoverLastAt = new Date().toISOString();
      if (r.ok) {
        acc.enabled = true;
        acc.disabledKind = null;
        acc.disabledReason = null;
        acc.disabledAt = null;
        acc.recoverTries = 0;
        acc.recoverNextAt = null;
        acc.recoveredAt = acc.recoverLastAt;
        logEvent(`auto-recover ${acc.name} 已恢复，放回轮询`);
      } else {
        acc.recoverTries = (Number(acc.recoverTries) || 0) + 1;
        acc.recoverNextAt = new Date(now + autoRecoverDelayMs(acc.recoverTries)).toISOString();
        logEvent(`auto-recover ${acc.name} 仍不可用（${String(r.msg).slice(0, 120)}），下次 ${acc.recoverNextAt}`);
      }
      saveConfig();
    }
  } finally {
    autoRecoverRunning = false;
  }
}

function startAutoRecover() {
  if (typeof setInterval !== 'function') return; // 测试沙箱没有定时器
  setInterval(() => { autoRecoverTick().catch((e) => logEvent(`auto-recover 巡查异常 ${e?.stack || e}`)); }, AUTO_RECOVER_TICK_MS);
}

// 代理密钥：非空时，/v1/* 与 /api/* 均需鉴权（Authorization: Bearer <key> 或 X-Admin-Key: <key>）；
// 控制台页面本身保持开放（不含任何敏感数据，数据由带鉴权的 /api/* 提供）。
// 可通过 POST /api/security 在运行期修改（下游密钥 = 客户端访问代理的凭据）。
let PROXY_KEY = config.proxyKey || '';
function isLoopback(req) {
  const a = (req.socket && req.socket.remoteAddress) || '';
  return a === '127.0.0.1' || a === '::1' || a === '::ffff:127.0.0.1';
}

function authOK(req) {
  if (isLoopback(req)) return true;
  if (!PROXY_KEY) return true;
  const bearer = String(req.headers['authorization'] || '').replace(/^Bearer\s+/i, '').trim();
  const admin = String(req.headers['x-admin-key'] || '').trim();
  return bearer === PROXY_KEY || admin === PROXY_KEY;
}
function unauthorized(res) {
  return sendJSON(res, 401, { error: { message: 'unauthorized: 代理密钥缺失或错误', type: 'auth_error' } });
}
function publicProxyBase() {
  return config.publicBaseUrl
    ? `${config.publicBaseUrl.replace(/\/+$/, '')}/v1`
    : `http://127.0.0.1:${config.port}/v1`;
}

const OR_API = 'https://openrouter.ai/api/v1';

async function fetchJSON(url, opts = {}, timeoutMs = 60000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...opts, signal: ctrl.signal });
    const text = await res.text();
    let json = null;
    try { json = JSON.parse(text); } catch { json = { raw: text }; }
    return { status: res.status, json };
  } finally {
    clearTimeout(t);
  }
}

// ---------- OpenRouter 目录缓存与 slug 归一化 ----------
async function orModelList() {
  if (META.orModelList && Date.now() - META.orModelsFetchedAt < 6 * 3600e3) return META.orModelList;
  const { json } = await fetchJSON(`${OR_API}/models`);
  const ids = (json?.data || []).map((m) => m.id);
  if (ids.length) {
    META.orModelList = ids;
    META.orModelsFetchedAt = Date.now();
    saveMeta();
  }
  return ids;
}
const norm = (s) => s.toLowerCase().replace(/[^a-z0-9]/g, '');

async function orEndpoints(slug) {
  // canonicalSlug 与 OpenRouter 目录 id 可能存在连字符差异（zai/... vs z-ai/...），先精确后归一匹配
  const ids = await orModelList();
  let real = ids.find((id) => id === slug) || ids.find((id) => norm(id) === norm(slug));
  if (!real) return { slug, endpoints: [] };
  const { json } = await fetchJSON(`${OR_API}/models/${real}/endpoints`);
  if (!Array.isArray(json?.data?.endpoints)) throw new Error('OpenRouter 指标不可用');
  return { slug: real, endpoints: endpointMetrics(json.data.endpoints, 'OpenRouter') };
}

function metricNumber(value) {
  if ((typeof value !== 'number' && typeof value !== 'string') || (typeof value === 'string' && !value.trim())) return null;
  const n = Number(value);
  return Number.isFinite(n) && n >= 0 ? n : null;
}

// 单位统一为美元/百万 token、毫秒、token/秒；缺失指标保持 null。
function endpointMetrics(endpoints, source) {
  const detail = new Map();
  for (const e of endpoints) {
    const slug = source === 'Vercel' ? slugify(e.provider_name || '')
      : String(e.tag || '').split('/')[0] || slugify(e.provider_name || '');
    if (!validUpstream(slug)) continue;
    const d = detail.get(slug) || { slug, name: e.provider_name || slug, endpoints: 0, context: 0,
      source, fetchedAt: Date.now(), window: source === 'Vercel' ? '1h' : '30m',
      input: null, output: null, cost: null, ttft: null, tps: null, offers: [] };
    d.endpoints++;
    d.context = Math.max(d.context, metricNumber(e.context_length) || 0);
    const input = metricNumber(e.pricing?.prompt);
    const output = metricNumber(e.pricing?.completion);
    const percentage = (value) => { const n = metricNumber(value); return n !== null && n <= 100 ? n : null; };
    const cacheRead = metricNumber(e.pricing?.input_cache_read);
    d.offers.push({
      endpoint: String(e.tag || e.name || slug),
      input: input === null ? null : input * 1e6,
      output: output === null ? null : output * 1e6,
      cacheRead: cacheRead === null ? null : cacheRead * 1e6,
      shortWindow: source === 'Vercel' ? '15m' : '5m',
      uptimeShort: percentage(source === 'Vercel' ? e.uptime_last_15m : e.uptime_last_5m),
      uptimeDay: percentage(e.uptime_last_1d),
    });
    // 成本按输入输出 1:1 的基础报价比较；两项必须来自同一个 endpoint。
    if (input !== null && output !== null && (d.cost === null || (input + output) * 1e6 < d.cost)) {
      d.input = input * 1e6;
      d.output = output * 1e6;
      d.cost = d.input + d.output;
    }
    const latency = source === 'Vercel' ? e.latency_last_1h : e.latency_last_30m;
    const throughput = source === 'Vercel' ? e.throughput_last_1h : e.throughput_last_30m;
    const ttft = metricNumber(typeof latency === 'object' ? latency?.p50 : latency);
    const tps = metricNumber(typeof throughput === 'object' ? throughput?.p50 : throughput);
    if (ttft !== null) d.ttft = Math.min(d.ttft ?? Infinity, ttft * (source === 'Vercel' ? 1 : 1000));
    if (tps !== null) d.tps = Math.max(d.tps ?? 0, tps);
    detail.set(slug, d);
  }
  return [...detail.values()];
}

async function vercelEndpoints(slug) {
  const modelPath = slug.split('/').map(encodeURIComponent).join('/');
  const { json } = await fetchJSON(`https://ai-gateway.vercel.sh/v1/models/${modelPath}/endpoints`, {}, 20000);
  if (!Array.isArray(json?.data?.endpoints)) throw new Error('Vercel 指标不可用');
  return { slug, endpoints: endpointMetrics(json.data.endpoints, 'Vercel') };
}

function validUpstream(value) {
  return typeof value === 'string' && value.length <= 128
    && /^[a-z0-9][a-z0-9._-]*(?:\/[a-z0-9][a-z0-9._-]*)*$/.test(value)
    && !['__proto__', 'constructor', 'prototype'].includes(value);
}

// 只从明确的渠道清单字段提取名称，不把错误里的普通词语当作上游。
function errorProviders(error) {
  const found = new Set();
  function visit(value, depth = 0) {
    if (depth > 5 || value == null) return;
    if (typeof value === 'object') {
      for (const [key, child] of Object.entries(value)) {
        if (['available_providers', 'availableProviders'].includes(key) && Array.isArray(child)) {
          child.filter(validUpstream).forEach((p) => found.add(p));
        } else if (['error', 'message', 'metadata', 'raw'].includes(key)) visit(child, depth + 1);
      }
    } else if (typeof value === 'string') {
      for (const match of value.matchAll(/Available providers are:\s*([a-z0-9-]+(?:\s*,\s*[a-z0-9-]+)*)/gi)) {
        match[1].split(/\s*,\s*/).filter(validUpstream).forEach((p) => found.add(p));
      }
      const start = value.indexOf('{');
      if (start >= 0) { try { visit(JSON.parse(value.slice(start)), depth + 1); } catch {} }
    }
  }
  visit(error);
  return [...found];
}

// ---------- 探测单个模型 ----------
// 两条管道（实测）：
// - planner：响应带 provider_metadata.gateway.routing（canonicalSlug/finalProvider/fallbacksAvailable），
//   请求体 provider.* 被网关丢弃。
// - direct：响应顶层带 provider（显示名）与 model（真实 OpenRouter ID），provider.only 会透传到
//   OpenRouter，可精确钉住。
function slugify(s) { return String(s).toLowerCase().replace(/\s+/g, '-'); }

function parseRouting(json) {
  const d = json?.data && json.data.choices ? json.data : json;
  const msg = d?.choices?.[0]?.message;
  const rt = msg?.provider_metadata?.gateway?.routing || d?.provider_metadata?.gateway?.routing || {};
  const direct = typeof d?.provider === 'string' ? d.provider : null;
  return {
    content: msg?.content ?? null,
    usage: d?.usage || null,
    pipeline: rt.finalProvider ? 'planner' : direct ? 'direct' : null,
    canonicalSlug: rt.canonicalSlug || (typeof d?.model === 'string' && d.model.includes('/') ? d.model : null),
    finalProvider: rt.finalProvider || (direct ? slugify(direct) : null),
    finalProviderName: rt.finalProvider || direct,
    fallbacks: rt.fallbacksAvailable || [],
    plan: rt.planningReasoning || '',
  };
}

// 故意携带不存在的 only，让网关在路由层报错并列出可用上游（不产生 token 消耗）。
// - 直连管道（OpenRouter）：provider.only → 404 错误 JSON 里的 metadata.available_providers
// - 规划器管道（Vercel AI Gateway）：providerOptions.gateway.only → 400 错误文本里的 "Available providers are: ..."
async function harvestAvailableProviders(modelId, pipeline) {
  const acc = pickAccount();
  const base = { model: modelId, messages: [{ role: 'user', content: 'hi' }], max_tokens: 16 };
  const body = pipeline === 'planner'
    ? { ...base, providerOptions: { gateway: { only: ['__probe__'] } } }
    : { ...base, provider: { only: ['__probe__'] } };
  const { json } = await fetchJSON(`${config.upstreamBase}/chat/completions`, { method: 'POST', headers: chatHeaders(acc.key), body: JSON.stringify(body) }, 60000);
  const providers = errorProviders(json?.error);
  return providers.length ? providers : null;
}
function parseTier0(plan) {
  const m = /([\w-]+) won tier 0 over ([^."]+)/.exec(plan || '');
  if (!m) return [];
  return [...new Set([m[1], ...m[2].split(/,\s*|\s+and\s+/).map((s) => s.trim()).filter(Boolean)])];
}

async function probeModel(modelId) {
  const acc = pickAccount();
  const t0 = Date.now();
  const body = { model: modelId, messages: [{ role: 'user', content: 'Reply with the word OK' }], max_tokens: 256 };
  const { json } = await fetchJSON(`${config.upstreamBase}/chat/completions`, {
    method: 'POST',
    headers: chatHeaders(acc.key),
    body: JSON.stringify(body),
  }, 180000);
  const ms = Date.now() - t0;
  if (json?.error && !json?.data) {
    return { ok: false, error: typeof json.error === 'string' ? json.error : JSON.stringify(json.error) };
  }
  const r = parseRouting(json);
  let harvest = null;
  if (r.pipeline) harvest = await harvestAvailableProviders(modelId, r.pipeline);
  let endpoints = [];
  let orSlug = null;
  if (r.pipeline && r.canonicalSlug) {
    try {
      const res = await (r.pipeline === 'planner' ? vercelEndpoints(r.canonicalSlug) : orEndpoints(r.canonicalSlug));
      endpoints = res.endpoints;
      if (r.pipeline === 'direct') orSlug = res.slug;
    } catch { /* 公开接口失败不影响探测结果 */ }
  }
  const prev = META.models[modelId] || {};
  const detail = {};
  for (const e of endpoints) detail[e.slug] = e;
  const upstreams = r.pipeline === 'planner'
    ? [...new Set([...(harvest || []), ...r.fallbacks, ...Object.keys(detail), ...(prev.watchedUpstreams || [])])]
    : [...new Set([...r.fallbacks, ...(harvest || []), ...Object.keys(detail), ...(prev.watchedUpstreams || [])])];
  const tier0 = [...new Set([...(prev.tier0 || []), ...parseTier0(r.plan)])];
  META.models[modelId] = {
    ...prev,
    ok: true,
    pipeline: r.pipeline,
    pinnable: !!r.pipeline,
    availableProviders: harvest || prev.availableProviders || [],
    canonicalSlug: r.canonicalSlug,
    openrouterSlug: orSlug,
    upstreamDetail: detail,
    upstreams,
    tier0,
    lastProvider: r.finalProvider || prev.lastProvider,
    lastMs: ms,
    probedAt: Date.now(),
  };
  extendDeepseekFlashUpstreams(modelId);
  pruneDeletedUpstreams(modelId);
  saveMeta();
  return { ok: true, ms, ...META.models[modelId] };
}

// 上游渠道可用性分类：渠道被单独钉住时的真实状态
function classifyUpstreamError(msg) {
  const m = String(msg || '');
  if (/empty response content/i.test(m)) return 'ok';                     // 请求已到达模型（推理耗尽 max_tokens 导致内容为空）
  if (/429|rate-?limited|temporarily rate/i.test(m)) return 'limited';   // 渠道有效，共享池限流中
  if (/invalid_request|modelid|no allowed providers|no available providers|not found|unsupported/i.test(m)) return 'bad'; // 不可钉住
  if (/unauthorized|re-authenticate|401/i.test(m)) return 'auth';        // 账号 key 问题，与渠道无关
  return 'unknown';
}
// 钉住请求失败时自动学习该渠道状态（仅确定性失败，瞬时限流标 limited 不拉黑）
function learnUpstreamStatus(modelId, upstream, errMsg) {
  if (!upstream || !errMsg) return;
  const st = classifyUpstreamError(errMsg);
  if (st === 'unknown') return;
  const meta = (META.models[modelId] ||= {});
  meta.upstreamStatus = { ...(meta.upstreamStatus || {}), [upstream]: { status: st, note: String(errMsg).slice(0, 160), checkedAt: Date.now() } };
}

// 自动+排除模式：only 白名单与网关侧渠道清单不一致时，网关报错会附最新清单，合并学习
// （触发场景：探测缓存过期，网关侧新增了渠道而本地 known 列表没有——白名单漏掉新渠道）
function learnAvailableProviders(modelId, errMsg) {
  const m = /Available providers are:\s*([^.]+)/.exec(String(errMsg || ''));
  if (!m) return;
  const toks = m[1].split(/,\s*/).map((s) => s.trim()).filter((t) => /^[a-z0-9][a-z0-9-]*$/.test(t));
  if (!toks.length) return;
  const meta = (META.models[modelId] ||= {});
  const before = (meta.upstreams || []).length;
  meta.upstreams = [...new Set([...(meta.upstreams || []), ...toks])];
  pruneDeletedUpstreams(modelId);
  if (meta.upstreams.length !== before) saveMeta();
}

// 批量校验：把模型的每个上游渠道用最小请求各钉一次，标记真实可用性
async function validateUpstreams(modelId) {
  const meta = META.models[modelId] || {};
  const list = meta.upstreams || [];
  const pipeline = meta.pipeline;
  const acc = pickAccount();
  const results = {};
  const batch = 5;
  for (let i = 0; i < list.length; i += batch) {
    await Promise.all(list.slice(i, i + batch).map(async (slug) => {
      const t0 = Date.now();
      const base = { model: modelId, messages: [{ role: 'user', content: 'hi' }], max_tokens: 16 };
      const body = pipeline === 'planner'
        ? { ...base, providerOptions: { gateway: { only: [slug] } } }
        : { ...base, provider: { only: [slug] } };
      const { json } = await fetchJSON(`${config.upstreamBase}/chat/completions`, {
        method: 'POST', headers: chatHeaders(acc.key), body: JSON.stringify(body),
      }, 60000).catch(() => ({ json: { error: 'network error' } }));
      let status = 'unknown';
      let note = '';
      if (json?.error && !json?.data) {
        const msg = typeof json.error === 'string' ? json.error : JSON.stringify(json.error);
        status = classifyUpstreamError(msg);
        note = msg.slice(0, 160);
      } else if (json?.data?.choices || json?.choices) {
        status = 'ok';
      }
      results[slug] = { status, ms: Date.now() - t0, note };
    }));
  }
  META.models[modelId] = { ...meta, upstreamStatus: { ...(meta.upstreamStatus || {}), ...results }, validatedAt: Date.now() };
  saveMeta();
  return results;
}

// 从官方文档与社区注册表拉取最新 ClinePass 订阅模型清单（只增不删）
async function fetchOfficialModels() {
  const found = new Set();
  const sources = [];
  try {
    const { json } = await fetchJSON('https://models.dev/api.json', {}, 30000);
    const cp = json?.providers?.['cline-pass'];
    if (cp?.models) {
      Object.keys(cp.models).forEach((id) => found.add(id.startsWith('cline-pass/') ? id : `cline-pass/${id}`));
      sources.push('models.dev');
    }
  } catch { /* 来源不可用则跳过 */ }
  try {
    const res = await fetch('https://docs.cline.bot/getting-started/clinepass', { signal: AbortSignal.timeout(30000) });
    const text = await res.text();
    const ids = text.match(/cline-pass\/[a-z0-9._-]+/gi) || [];
    if (ids.length) { ids.forEach((id) => found.add(id.toLowerCase())); sources.push('docs.cline.bot'); }
  } catch { /* 来源不可用则跳过 */ }
  const valid = [...found].filter((id) => /^cline-pass\/[a-z0-9._-]+$/.test(id));
  const added = valid.filter((id) => !config.knownModels.includes(id));
  if (added.length) {
    config.knownModels.push(...added);
    saveConfig();
  }
  META.officialModelsFetch = { ts: Date.now(), sources, found: valid.length, added, total: config.knownModels.length };
  saveMeta();
  return { sources, found: valid.length, added, knownModels: config.knownModels, ...META.officialModelsFetch };
}

// 从上游 usage 提取记账 token 数；缺失或无有效数值时返回 null（该笔不计 token）
function usageTokens(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const prompt = metricNumber(raw.prompt_tokens);
  const completion = metricNumber(raw.completion_tokens);
  if (prompt === null && completion === null) return null;
  const cached = metricNumber(raw.prompt_tokens_details?.cached_tokens);
  return {
    prompt: prompt ?? 0,
    completion: completion ?? 0,
    cached: cached ?? 0,
    total: (prompt ?? 0) + (completion ?? 0),
  };
}

// ---------- 持久账本（累计 + 按天）----------
// META.usage 是 5000 条滚动窗口（metadata.json 不能无限大），只够最近两三天的明细。
// 旧版的"累计 TOKEN"直接对窗口行求和 —— 窗口一满，数字就永远停在最近 5000 笔的和上，
// 几个星期看都是十几亿不动（旧版账本出现过这个问题）。所以总额与按天桶单独持久化，
// 不随行窗口淘汰；行缓冲此后只当"最近明细"与 5 小时窗口用。
const DAY_KEEP = 400; // 按天桶保留上限（星图最多画 35 天，留一年余量）
const emptyBucket = () => ({ requests: 0, prompt: 0, completion: 0, cached: 0, total: 0 });
function localDayKey(ts) {
  const d = new Date(ts);
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}
function bucketAdd(bucket, row) {
  bucket.requests += 1;
  bucket.prompt += row.prompt || 0;
  bucket.completion += row.completion || 0;
  bucket.cached += row.cached || 0;
  bucket.total += row.total || 0;
}
// 维度桶：day 级只留 requests/total（控体积），totals 级留全套（缎带表要用）
function dimAdd(map, key, row, full) {
  const b = (map[key] ||= full ? emptyBucket() : { requests: 0, total: 0 });
  b.requests += 1;
  b.total += row.total || 0;
  if (full) { b.prompt += row.prompt || 0; b.completion += row.completion || 0; b.cached += row.cached || 0; }
}
// 播种/迁移：只在账本还不存在时跑一次，从给定的行（默认 META.usage 缓冲）重建累计与按天桶。
// 行缓冲里已经滚掉的更早历史无法找回 —— 播种起点即账本的"可信起点"（totals.since）。
function ensureLedger(seedRows) {
  if (META.usageTotals && META.usageDaily) return;
  const rows = Array.isArray(seedRows) ? seedRows : META.usage;
  const totals = { ...emptyBucket(), since: null, byModel: {}, byAcc: {} };
  const daily = {};
  for (const r of rows) {
    if (!r) continue;
    const ts = typeof r.ts === 'number' ? r.ts : Date.parse(r.ts);
    if (!Number.isFinite(ts)) continue;
    if (totals.since === null || ts < totals.since) totals.since = ts;
    bucketAdd(totals, r);
    const day = (daily[localDayKey(ts)] ||= { ...emptyBucket(), byModel: {}, byAcc: {} });
    bucketAdd(day, r);
    if (r.model) { dimAdd(day.byModel, r.model, r, false); dimAdd(totals.byModel, r.model, r, true); }
    if (r.account) { dimAdd(day.byAcc, r.account, r, false); dimAdd(totals.byAcc, r.account, r, false); }
  }
  META.usageTotals = totals;
  META.usageDaily = daily;
  saveMeta();
}
// 每记一笔真实 usage 就进账（和行缓冲相互独立；行被滚掉、账还在）
function ledgerAdd(row) {
  ensureLedger(); // 保证账本已播种（record() 在 unshift 之前调用过一次，不会把这笔算重）
  const totals = META.usageTotals;
  if (totals.since === null || row.ts < totals.since) totals.since = row.ts;
  bucketAdd(totals, row);
  const day = (META.usageDaily[localDayKey(row.ts)] ||= { ...emptyBucket(), byModel: {}, byAcc: {} });
  bucketAdd(day, row);
  if (row.model) { dimAdd(day.byModel, row.model, row, false); dimAdd(totals.byModel, row.model, row, true); }
  if (row.account) { dimAdd(day.byAcc, row.account, row, false); dimAdd(totals.byAcc, row.account, row, false); }
  const keys = Object.keys(META.usageDaily);
  if (keys.length > DAY_KEEP) {
    keys.sort();
    for (const k of keys.slice(0, keys.length - DAY_KEEP)) delete META.usageDaily[k];
  }
}

function record(modelId, info) {
  META.models[modelId] = { ...(META.models[modelId] || {}), ...info };
  META.history.unshift({ ts: Date.now(), model: modelId, ...info });
  if (META.history.length > 100) META.history.length = 100;
  // Token 记账：代理与测试台的真实请求都会走到这里（info.usage 为上游原始 usage 对象）；
  // 测速不走 record，不进账本；上游没给有效 usage 的请求不计 token
  const t = usageTokens(info.usage);
  if (t) {
    ensureLedger(); // 先播种账本（若首次），再记这笔 —— 否则会漏掉缓冲里已有的历史
    const row = {
      ts: Date.now(), model: info.requestedModel || modelId, resolvedModel: modelId,
      account: info.account ?? null, provider: info.provider ?? null,
      canonical: info.canonical ?? null, ms: info.ms ?? null, stream: !!info.stream, error: info.error ?? null,
      attempts: Array.isArray(info.attempts) ? info.attempts : [],
      ...t,
    };
    META.usage.unshift(row);
    if (META.usage.length > 5000) META.usage.length = 5000;
    ledgerAdd(row); // 持久账本独立于行缓冲：行被滚掉，累计/按天不动
  }
  if (info.account) {
    META.stats = META.stats || {};
    const st = (META.stats[info.account] ||= { requests: 0, lastUsed: 0, lastError: null });
    st.requests += 1;
    st.lastUsed = Date.now();
    st.lastError = info.error || null;
  }
  saveMeta();
}

// ---------- Cline 官方账号数据（/api/plan-usage） ----------
// 官方账单端点（sk_ 静态 key 实测 200，全部只读 GET）：
//   GET {origin}/api/v1/users/me/plan               → data.plan.displayName/interval/isActive、data.currentPeriodEnd、
//                                                     data.plan.entitlements.cline_pass.inferenceCapThreshold（额度阈值）
//   GET {origin}/api/v1/users/me/plan/usage-limits  → data.limits: [{type: five_hour|weekly|monthly, percentUsed, resetsAt}]
// 金额单位 micro-USD（/1e8）。官方失败时降级为本地账本估算（口径是 token 数，非官方 USD，仅参考）。

// 把官方 plan + usage-limits 两份 JSON 归一成统一结构（纯函数，便于测试）
function parseOfficialPlan(planJson, limitsJson) {
  const root = planJson && typeof planJson === 'object' ? planJson : {};
  const data = root.data && typeof root.data === 'object' ? root.data : root; // 兼容已解包的 data
  const plan = data.plan && typeof data.plan === 'object' ? data.plan : {};
  const ent = plan.entitlements && typeof plan.entitlements === 'object' ? plan.entitlements : {};
  const cp = ent.cline_pass && typeof ent.cline_pass === 'object' ? ent.cline_pass : {};
  const cap = cp.inferenceCapThreshold && typeof cp.inferenceCapThreshold === 'object' ? cp.inferenceCapThreshold : {};
  const microToUsd = (v) => (typeof v === 'number' && Number.isFinite(v) ? v / 1e8 : undefined);
  const byType = new Map();
  const limits = limitsJson && limitsJson.data && Array.isArray(limitsJson.data.limits) ? limitsJson.data.limits : [];
  for (const item of limits) {
    if (item && typeof item === 'object' && typeof item.type === 'string') byType.set(item.type, item);
  }
  const win = (type) => {
    const it = byType.get(type) || {};
    return {
      percentUsed: typeof it.percentUsed === 'number' ? it.percentUsed : 0,
      resetsAt: typeof it.resetsAt === 'string' ? it.resetsAt : null,
    };
  };
  return {
    planName: typeof plan.displayName === 'string' ? plan.displayName : null,
    interval: typeof plan.interval === 'string' ? plan.interval : null,
    isActive: plan.isActive !== false,
    currentPeriodEnd: typeof data.currentPeriodEnd === 'string' ? data.currentPeriodEnd : null,
    caps: {
      fiveHour: { ...win('five_hour'), limitUsd: microToUsd(cap.last5HoursUsageCostUSDPerUser) },
      weekly: { ...win('weekly'), limitUsd: microToUsd(cap.last7daysUsageCostUSDPerUser) },
      monthly: { ...win('monthly'), limitUsd: microToUsd(cap.last30daysUsageCostUSDPerUser) },
    },
  };
}

// 本地降级估算：某账号在 5 小时滚动 / 日历周（周一起）/ 本月 内的 token 合计。
// 5h 窗只有行级数据能精确到小时（行缓冲约覆盖最近两三天）；周/月走持久按天账本，
// 不随行窗口滚动（旧版三个窗全吃行窗口，月窗实际只剩两三天）。
function localWindowTotals(usageRows, account, now = Date.now()) {
  const HOUR = 3600_000;
  const d = new Date(now);
  const dow = (d.getDay() + 6) % 7; // 周一=0
  const weekStartDay = localDayKey(new Date(d.getFullYear(), d.getMonth(), d.getDate() - dow).getTime());
  const monthStartDay = localDayKey(new Date(d.getFullYear(), d.getMonth(), 1).getTime());
  const totals = { fiveHour: 0, weekly: 0, monthly: 0, requests: { fiveHour: 0, weekly: 0, monthly: 0 } };
  for (const r of Array.isArray(usageRows) ? usageRows : []) {
    if (!r || r.account !== account) continue;
    const ts = typeof r.ts === 'number' ? r.ts : Date.parse(r.ts);
    if (!Number.isFinite(ts)) continue;
    if (now - ts <= 5 * HOUR) { totals.fiveHour += r.total || 0; totals.requests.fiveHour += 1; }
  }
  ensureLedger(usageRows); // 账本还不存在时用行缓冲播种（正式环境 usageRows 即 META.usage）
  for (const [k, day] of Object.entries(META.usageDaily)) {
    const a = day.byAcc && day.byAcc[account];
    if (!a) continue;
    if (k >= weekStartDay) { totals.weekly += a.total || 0; totals.requests.weekly += a.requests || 0; }
    if (k >= monthStartDay) { totals.monthly += a.total || 0; totals.requests.monthly += a.requests || 0; }
  }
  return totals;
}

// 月付到期估算：(renewStart || addedAt) + 30 天（官方 currentPeriodEnd 不可用时兜底）
const THIRTY_DAYS_MS = 30 * 24 * 3600_000;
function estimatePeriodEnd(acc, now = Date.now()) {
  const base = acc && (acc.renewStart || acc.addedAt);
  const t = base ? Date.parse(base) : NaN;
  return Number.isFinite(t) ? new Date(t + THIRTY_DAYS_MS).toISOString() : null;
}

const PLAN_TTL_MS = 60_000;
let planCache = { at: 0, data: null, promise: null };

async function fetchOfficialPlanForAccount(acc) {
  const headers = { Authorization: `Bearer ${acc.key}` };
  let origin = 'https://api.cline.bot';
  try { origin = new URL(config.upstreamBase).origin; } catch { /* 用默认 */ }
  const req = (p) => fetchJSON(origin + p, { headers }, 15000);
  const [planRes, limitsRes] = await Promise.all([
    req('/api/v1/users/me/plan'),
    req('/api/v1/users/me/plan/usage-limits'),
  ]);
  if (planRes.status !== 200 || limitsRes.status !== 200) {
    throw new Error(`官方端点 ${planRes.status}/${limitsRes.status}`);
  }
  return parseOfficialPlan(planRes.json, limitsRes.json);
}

// 官方三个窗口里最近一次"会重置"的时间点，给禁用号当"额度什么时候回来"的参考
function nextResetAtOf(official) {
  const caps = (official && official.caps) || {};
  const times = [caps.fiveHour, caps.weekly, caps.monthly]
    .map((w) => (w && w.resetsAt ? Date.parse(w.resetsAt) : NaN))
    .filter((t) => Number.isFinite(t));
  if (!times.length) return null;
  const now = Date.now();
  const future = times.filter((t) => t > now).sort((a, b) => a - b);
  return new Date(future.length ? future[0] : Math.max(...times)).toISOString();
}

// 熔断痕迹 + 自动恢复状态：启用与否都带上，前端据此显示"什么时候能回来、在不在自己重试"
function accountRecoveryMeta(acc) {
  const off = acc.enabled === false;
  return {
    disabledReason: off ? (acc.disabledReason || null) : null,
    disabledAt: off ? (acc.disabledAt || null) : null,
    disabledKind: off ? effectiveDisabledKind(acc) : null,
    quotaResetAt: acc.quotaResetAt || null,
    recoverTries: acc.recoverTries || 0,
    recoverLastAt: acc.recoverLastAt || null,
    recoverNextAt: off ? (acc.recoverNextAt || null) : null,
    recoveredAt: acc.recoveredAt || null,
    autoRecoverOn: config.autoRecover !== false,
  };
}

// 单账号的官方/降级数据条目（getPlanUsage 与单账号刷新共用）
async function fetchAccountEntry(acc, now) {
  const recover = accountRecoveryMeta(acc);
  const totals = () => localWindowTotals(META.usage, acc.name, now);
  try {
    // 禁用号照样拉官方账单：usage-limits 只是只读 GET、不消耗额度，
    // 这样才能显示"重置时间 + 真实百分比"，而不是一片 0% 的空白
    const official = await fetchOfficialPlanForAccount(acc);
    const resetAt = nextResetAtOf(official);
    if (acc.enabled === false && resetAt && resetAt !== acc.quotaResetAt) {
      acc.quotaResetAt = resetAt;
      saveConfig();
    }
    return {
      name: acc.name, source: 'official', enabled: acc.enabled !== false, ...official, ...recover,
      quotaResetAt: acc.quotaResetAt || resetAt || null,
      addedAt: acc.addedAt || null, renewStart: acc.renewStart || null,
    };
  } catch (e) {
    const t = totals();
    return {
      name: acc.name, source: 'local', estimated: true, enabled: acc.enabled !== false,
      error: String((e && e.message) || e).slice(0, 160), ...recover,
      planName: null, interval: null, isActive: null,
      currentPeriodEnd: estimatePeriodEnd(acc, now),
      addedAt: acc.addedAt || null, renewStart: acc.renewStart || null,
      caps: {
        fiveHour: { percentUsed: null, resetsAt: null, limitUsd: null, localTokens: t.fiveHour, localRequests: t.requests.fiveHour },
        weekly: { percentUsed: null, resetsAt: null, limitUsd: null, localTokens: t.weekly, localRequests: t.requests.weekly },
        monthly: { percentUsed: null, resetsAt: null, limitUsd: null, localTokens: t.monthly, localRequests: t.requests.monthly },
      },
    };
  }
}

// onlyName：单账号强刷 —— 只拉这一个账号的官方账单，其余账号沿用缓存（点谁的「刷」只拉谁的，
// 不把整座机房全部重打一遍官方端点）。onlyName 账号不在缓存时整表补齐。
async function getPlanUsage(force = false, onlyName = null) {
  const now = Date.now();
  const cached = planCache.data;
  if (onlyName) {
    const acc = (config.accounts || []).find((a) => a && a.key && a.name === onlyName);
    if (!acc) return cached || { at: now, ttlSeconds: PLAN_TTL_MS / 1000, accounts: [] };
    const entry = await fetchAccountEntry(acc, now);
    const others = (cached?.accounts || []).filter((a) => a.name !== onlyName);
    const byName = new Map(others.map((a) => [a.name, a]));
    byName.set(entry.name, entry);
    // 按当前 config 顺序输出；新出现的账号排在末尾
    const ordered = (config.accounts || []).filter((a) => a && a.key).map((a) => byName.get(a.name)).filter(Boolean);
    for (const rest of others) if (!ordered.some((o) => o.name === rest.name)) ordered.push(rest);
    const data = { at: now, ttlSeconds: PLAN_TTL_MS / 1000, accounts: ordered };
    planCache = { at: now, data, promise: null };
    return data;
  }
  if (!force && cached && now - planCache.at < PLAN_TTL_MS) return cached;
  if (planCache.promise) return planCache.promise;
  const p = (async () => {
    // 含已禁用账号（前端显示禁用卡与原因），只跳过没有 key 的残条目
    const accounts = (config.accounts || []).filter((a) => a && a.key);
    const list = await Promise.all(accounts.map((acc) => fetchAccountEntry(acc, now)));
    const data = { at: now, ttlSeconds: PLAN_TTL_MS / 1000, accounts: list };
    planCache = { at: now, data, promise: null };
    return data;
  })();
  planCache.promise = p;
  try { return await p; } finally { planCache.promise = null; }
}

// smart 模式的后台预热：官方账本缓存过期/未就绪时拉一次，不阻塞当前请求
// （当前请求用上一次缓存顶上，选不出来就退回轮询）；30 秒节流，官方不可达时不会每请求重打。
let planKickAt = 0;
function maybeRefreshPlanData() {
  const now = Date.now();
  if (planCache.data && now - planCache.at < PLAN_TTL_MS) return;
  if (now - planKickAt < 30_000) return;
  planKickAt = now;
  getPlanUsage(false).catch(() => {});
}

// ---------- 聊天代理 ----------
const CHAT_PATHS = new Set(['/chat/completions', '/v1/chat/completions', '/api/v1/chat/completions']);

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > 50 * 1024 * 1024) { reject(new Error('body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function unwrap(json) {
  const d = json?.data && json.data.choices ? json.data : json;
  if (d?.error && !d?.choices) {
    const msg = typeof d.error === 'string' ? d.error : JSON.stringify(d.error);
    const status = /model not found/i.test(msg) ? 404 : 502;
    return { status, body: { error: { message: msg, type: 'upstream_error' } }, routing: {} };
  }
  const r = parseRouting(d);
  return { status: 200, body: d, routing: r };
}

// 按管道注入上游偏好（实测结论）：
// - 规划器管道（Vercel AI Gateway）：顶层 provider 简写里的 only/order 会被 Cline 吞掉，
//   必须用 providerOptions.gateway.{only,order,sort}；流式同样生效。
// - 直连管道（OpenRouter）：顶层 provider.{only,order,sort} 生效；providerOptions 被忽略。
// - 管道未知时两种形式同时注入，各自取用、互不干扰。
const OR_SORT = { cost: 'price', ttft: 'latency', tps: 'throughput' };

// upstream: 本次尝试钉住的上游（null=自动）；orderRest: preferred 模式下排在当前上游之后的回退序列；
// excludeList: 排除列表。网关不支持 exclude/ignore 字段（实测被静默忽略），因此排除统一换算成 only 白名单：
// 自动模式 only=已知上游-排除；preferred 钉住模式 order=[当前,...] 且 only=已知上游-排除（防止网关回退到被排除渠道）；
// 严格钉住模式 only=[当前上游]，天然排除其他一切渠道。
function injectPrefs(body, modelId, { upstream, orderRest = [], excludeList = [], strict = true, sort = null }) {
  const b = JSON.parse(JSON.stringify(body));
  const exclude = (excludeList || []).filter((u) => u !== upstream);
  const meta = META.models[modelId] || {};
  const known = meta.upstreams || [];
  const allowList = exclude.length ? known.filter((u) => !exclude.includes(u)) : null;
  if (!upstream && !sort && !(allowList && allowList.length)) return b;
  const pipeline = meta.pipeline || null;
  const useVercel = pipeline === 'planner' || pipeline === null;
  const useOpenRouter = pipeline === 'direct' || pipeline === null;
  if (useVercel) {
    const gw = {};
    if (upstream) {
      if (strict) gw.only = [upstream];
      else {
        gw.order = [upstream, ...orderRest];
        if (allowList && allowList.length) gw.only = allowList;
      }
    } else if (allowList && allowList.length) {
      gw.only = allowList;
    }
    if (sort) gw.sort = sort;
    b.providerOptions = { ...(b.providerOptions || {}), gateway: { ...(b.providerOptions?.gateway || {}), ...gw } };
  }
  if (useOpenRouter) {
    const p = { ...(b.provider || {}) };
    if (upstream) {
      if (strict) p.only = [upstream];
      else {
        p.order = [upstream, ...orderRest];
        if (allowList && allowList.length) p.only = allowList;
      }
    } else if (allowList && allowList.length) {
      p.only = allowList;
    }
    if (sort) p.sort = OR_SORT[sort] || sort;
    b.provider = p;
  }
  return b;
}

// 由 perModel 配置展开出故障转移候选序列：[{ upstream, orderRest, excludeList, strict, sort }, ...]
// - 勾选了上游（排除后非空）：逐个尝试，排除的永不在候选中
// - 未勾选：单候选自动模式，排除换算成 only 白名单注入（见 injectPrefs）
// deepinfra 的 1017 部署实测 27.8 TPS，全局拉黑，防止网关自由路由选回它
const GLOBAL_EXCLUDE = ['deepinfra'];

function buildAttempts(modelId, cfg) {
  const listed = (cfg?.upstreams || []).filter((u) => typeof u === 'string' && u);
  const exclude = [...new Set([
    ...(cfg?.exclude || []).filter((u) => typeof u === 'string' && u),
    ...GLOBAL_EXCLUDE,
  ])];
  const excl = new Set(exclude);
  const wanted = listed.filter((u) => !excl.has(u));
  const strict = (cfg?.pinMode || 'strict') === 'strict';
  const sort = cfg?.sort || null;
  const base = { strict, sort, excludeList: exclude };
  if (wanted.length) {
    // preferred 模式：当前上游排在 order 首位，其余勾选项作为网关侧回退序列；排除列表随行（限制网关回退范围）
    return wanted.map((u, i) => ({ ...base, upstream: u, orderRest: strict ? [] : wanted.filter((_, j) => j !== i) }));
  }
  return [{ ...base, upstream: null, orderRest: [], excludeList: exclude }];
}

// 把上游错误信息归一成短字符串（用于学习与尝试日志）
const errText = (e) => (e == null ? '' : typeof e === 'string' ? e : JSON.stringify(e));

// 单次向上游网关发起非流式请求；返回 { status, out, routing, netError, acc }
// 异常（网络错误/非 JSON/非 200）不抛出，由调用方决定切换
async function attemptOnce(modelId, body, attempt, signal) {
  const send = injectPrefs(body, modelId, attempt);
  const acc = pickAccount(modelId);
  try {
    const res = await fetch(`${config.upstreamBase}/chat/completions`, {
      method: 'POST', headers: chatHeaders(acc.key), body: JSON.stringify(send), signal,
    });
    const json = await res.json().catch(() => null);
    if (!json) return { status: 502, out: { error: { message: 'upstream returned non-JSON', type: 'upstream_error' } }, routing: {}, netError: 'non-JSON response', acc };
    const { status, body: out, routing } = unwrap(json);
    return { status, out, routing, netError: null, acc };
  } catch (e) {
    return { status: 502, out: { error: { message: `upstream fetch failed: ${e.message}`, type: 'upstream_error' } }, routing: {}, netError: e.message, acc };
  }
}

// 顺序故障转移：依次执行候选，非 200 / 网络失败 / 超时即切换下一个；全部失败返回最后一次结果。
// 流式：首包前（网关以 JSON 而非 SSE 应答错误）仍可切换；SSE 一旦开始即透传，无法重试。
// 每次尝试有独立的超时中止（attemptTimeoutMs）；客户端断开会中止当前尝试。
// 返回 { status, out, routing, acc, trace, streamUp? } —— trace 为逐次尝试 [{ upstream, status, ms, note }]
async function runChatChain(req, body, modelId, cfg, { stream = false, attemptTimeoutMs = 120000, res = null } = {}) {
  const trace = [];
  const t0 = Date.now();
  let last = null;
  let activeCtrl = null;            // 当前尝试的 AbortController；流式成功后保持指向该次 fetch，用于断连时中止上游 body
  let keepCloseHook = false;        // 流式 SSE 建立后，close 钩子要保留到流结束
  const onClientClose = () => { if (activeCtrl) activeCtrl.abort(); };
  req.on('close', onClientClose);
  // 实测（2026-10-03）：客户端中途断开时 req 的 'close' 不触发，上游流没人管、继续白烧额度。
  // res 'close' 在连接真正断开时必触发：还没写完就断 = 客户端跑了，立刻掐上游并留标记。
  // res 由调用方经 opts 传入（测试沙箱的 runChatChain 替身是四参，塞 opts 不会拌到它）
  const state = { clientGone: false };
  if (res) res.on('close', () => { if (!res.writableEnded) { state.clientGone = true; onClientClose(); } });
  try {
    // 账号级重跑：某轮失败且熔断了坏账号（额度满/密钥坏）时，用剩余账号重跑一轮（最多 3 轮防打转）
    for (let round = 0; round < 3; round++) {
      let roundDisabled = false;
      for (const attempt of buildAttempts(modelId, cfg)) {
      const t1 = Date.now();
      const ctrl = new AbortController();
      activeCtrl = ctrl;
      const timer = setTimeout(() => ctrl.abort(), attemptTimeoutMs);
      try {
        if (stream) {
          const send = injectPrefs(body, modelId, attempt);
          const acc = pickAccount(modelId);
          let up = null;
          let netError = null;
          try {
            up = await fetch(`${config.upstreamBase}/chat/completions`, { method: 'POST', headers: chatHeaders(acc.key), body: JSON.stringify(send), signal: ctrl.signal });
          } catch (e) { netError = e.message; }
          const ctype = up?.headers?.get('content-type') || '';
          let isSSE = !!up && up.status === 200 && ctype.includes('event-stream');
          // 网关对流式错误可能返回 200 + text/event-stream，body 却是 {"error":...}：
          // 读首个数据块探测，真正的 SSE 第一行是 "data: {...}" 且非纯错误对象
          let firstChunk = null;
          if (isSSE) {
            let reader = null;
            try {
              reader = up.body.getReader();
              const { value, done } = await reader.read();
              if (done) {
                isSSE = false;
                netError = 'empty stream';
              } else {
                firstChunk = Buffer.from(value);
                const head = firstChunk.toString('utf8').trimStart().slice(0, 200);
                if (head.startsWith('data:')) {
                  const payload = head.replace(/^data:\s*/, '').slice(0, 160);
                  if (payload.startsWith('{"error"')) { isSSE = false; netError = `stream error: ${payload.slice(0, 120)}`; }
                } else {
                  isSSE = false;
                  netError = `unexpected stream head: ${head.slice(0, 60)}`;
                }
              }
            } catch (e) {
              isSSE = false;
              netError = e.message;
            } finally {
              try { reader?.releaseLock(); } catch {}
            }
          }
          const ms = Date.now() - t1;
          if (up && !isSSE) {
            let text = '';
            let json = null;
            if (firstChunk) {
              // 已消费的块 + 剩余 body 拼回完整错误文本
              const rest = await up.text().catch(() => '');
              text = firstChunk.toString('utf8') + rest;
            } else {
              text = await up.text();
            }
            try { json = JSON.parse(text); } catch {}
            const msg = errText(json?.error) || text.slice(0, 160) || netError;
            trace.push({ upstream: attempt.upstream, status: up.status, ms, note: msg.slice(0, 160) });
            if (attempt.upstream) learnUpstreamStatus(modelId, attempt.upstream, msg);
            if (!attempt.upstream && (attempt.excludeList || []).length) learnAvailableProviders(modelId, msg);
            // 账号级失败（额度满/密钥坏）：熔断该账号，下个 attempt / 下轮重跑会换好号
            const accKind = classifyAccountError(up.status, msg);
            if (accKind && acc?.name && noteAccountFailure(acc.name, accKind, msg)) roundDisabled = true;
            last = { status: json?.error ? 502 : up.status, out: json || { error: { message: text.slice(0, 400) || netError, type: 'upstream_error' } }, routing: parseRouting(json || {}), acc, netError: null };
            continue; // 错误：还未向客户端写任何字节，可切换下一候选
          }
          if (!up) {
            trace.push({ upstream: attempt.upstream, status: 502, ms, note: netError || 'no response' });
            last = { status: 502, out: { error: { message: `upstream fetch failed: ${netError || 'no response'}`, type: 'upstream_error' } }, routing: {}, acc, netError: netError || 'no response' };
            continue;
          }
          // 真 SSE：firstChunk 与剩余 body 串联透传（SSE 开始后无法重试）；close 钩子保留用于客户端断开时中止上游
          keepCloseHook = true;
          trace.push({ upstream: attempt.upstream, status: 200, ms, note: 'stream' });
          return { status: 200, streamUp: up, streamHead: firstChunk, acc, trace, t0, state };
        }
        // 非流式
        const r = await attemptOnce(modelId, body, attempt, ctrl.signal);
        const ms = Date.now() - t1;
        const note = r.netError || (r.status !== 200 ? errText(r.out?.error?.message).slice(0, 160) : 'ok');
        trace.push({ upstream: attempt.upstream, status: r.status, ms, note });
        if (r.status !== 200 && attempt.upstream) learnUpstreamStatus(modelId, attempt.upstream, errText(r.out?.error?.message));
        if (r.status !== 200 && !attempt.upstream && (attempt.excludeList || []).length) learnAvailableProviders(modelId, r.netError || note);
        // 账号级失败（额度满/密钥坏）：熔断该账号，下个 attempt / 下轮重跑会换好号
        if (r.status !== 200) {
          const accMsg = errText(r.out?.error?.message);
          const accKind = classifyAccountError(r.status, accMsg);
          if (accKind && r.acc?.name && noteAccountFailure(r.acc.name, accKind, accMsg)) roundDisabled = true;
        }
        last = r;
        if (r.status === 200) break;
      } finally {
        clearTimeout(timer);
      }
    }
    if (last?.status === 200 || !roundDisabled) break;
    }
  } finally {
    if (!keepCloseHook) req.off('close', onClientClose);
  }
  return { ...last, status: last?.status ?? 502, trace, t0, netError: last?.netError || null };
}

// ---------- 流式中继：终止帧扣留 + 空流/断流自动更换上游重试（2026-10-04） ----------
// 网关偶发两种病：①流正常结束但整条没有 content / tool_calls（客户端收到"空回复"）；
// ②流被掐在终止帧之前（客户端报 stream ended before terminal chunk）。
// 代理把终止帧（finish_reason / usage / [DONE]）先扣住，等这一轮流完整结束再决定：
// 有可用产出就放行；没有任何可用产出就丢弃这一轮、换上游重来，客户端全程看不到空轮。
// 可用产出 = 非空 content 或 tool_calls；纯 reasoning 在客户端眼里等于空回复，不算数。
const STREAM_ROUNDS = 3;           // 同一条客户端响应最多接几轮上游流（1 首发 + 最多 2 重试）
const STREAM_DEADLINE_MS = 90_000; // 重试总时限：超过就不再换轮，防客户端先超时
const SYNTH_FINISH_FRAME = `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`;
function streamHasOutput(text) { return /"tool_calls"\s*:|"content"\s*:\s*"[^"]/.test(text); }
function streamHasFinish(text) { return /"finish_reason"\s*:\s*"(?!null)[a-z_]/.test(text); }
function isTerminalFrame(frame) { return streamHasFinish(frame) || /"usage"\s*:/.test(frame) || frame.includes('[DONE]'); }
function takeSseFrame(text) {
  for (const sep of ['\n\n', '\r\n\r\n']) {
    const i = text.indexOf(sep);
    if (i >= 0) return [text.slice(0, i + sep.length), text.slice(i + sep.length)];
  }
  return [null, text];
}
// 平衡括号取 JSON 对象（usage 里还有嵌套，正则截不准）
function extractJsonObject(text, key) {
  const i = text.lastIndexOf(`"${key}":`);
  if (i < 0) return null;
  const start = text.indexOf('{', i);
  if (start < 0) return null;
  let depth = 0, inStr = false, esc = false;
  for (let j = start; j < text.length && j < start + 20000; j++) {
    const ch = text[j];
    if (inStr) { if (esc) esc = false; else if (ch === '\\') esc = true; else if (ch === '"') inStr = false; continue; }
    if (ch === '"') inStr = true;
    else if (ch === '{') depth++;
    else if (ch === '}') { depth--; if (depth === 0) { try { return JSON.parse(text.slice(start, j + 1)); } catch { return null; } } }
  }
  return null;
}
// 从流文本回读路由元数据与 usage（沿用原 tap 口径：尾部倒扫 + planner 兜底）
function parseStreamTail(text) {
  let provider = null, canonical = null, usage = null;
  const full = String(text || '');
  const lines = full.split('\n');
  for (let i = lines.length - 1; i >= 0 && i >= lines.length - 30 && !(provider && usage); i--) {
    const l = lines[i];
    if (!l.startsWith('data: ') || l.includes('[DONE]')) continue;
    try {
      const c = JSON.parse(l.slice(6));
      if (!provider && typeof c.provider === 'string') { provider = slugify(c.provider); canonical = c.model || null; }
      if (!usage && c.usage && typeof c.usage === 'object') usage = c.usage;
    } catch { /* 跳过不完整行 */ }
  }
  if (!provider) {
    const fp = /"finalProvider":"([^"]+)"/.exec(full);
    const cs = /"canonicalSlug":"([^"]+)"/.exec(full);
    provider = fp ? fp[1] : null;
    canonical = cs ? cs[1] : null;
  }
  if (!usage) usage = extractJsonObject(full, 'usage');
  return { provider, canonical, usage };
}
function writeStreamErrorFrame(res, message) {
  try {
    if (!res.writableEnded && !res.destroyed) {
      res.write(`data: ${JSON.stringify({ error: { message, type: 'upstream_error' } })}\n\n`);
      res.write('data: [DONE]\n\n');
      res.end();
    }
  } catch { /* 客户端已断开 */ }
}
// 中继一轮上游 SSE；返回 { ok, usable, clientGone }
async function relayStreamRound(req, res, chain, ctx) {
  const up = chain.streamUp;
  const acc = chain.acc;
  const t0 = chain.t0;
  if (!res.headersSent) {
    res.writeHead(up.status, {
      'Content-Type': up.headers.get('content-type') || 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'Access-Control-Allow-Origin': '*',
      'X-Cline-Target-Upstream': ctx.targets.length ? ctx.targets.join('>') : 'auto',
      'X-Cline-Attempts': String(chain.trace.length),
      'X-Cline-Account': headerSafe(acc.name),
      'X-Cline-Requested-Model': headerSafe(ctx.requestedModelId),
      'X-Cline-Resolved-Model': headerSafe(ctx.modelId),
    });
  }
  const decoder = new TextDecoder('utf-8');
  const held = [];
  const attempts = chain.trace.map((t) => t.upstream || 'auto');
  let pending = chain.streamHead ? chain.streamHead.toString('utf8') : '';
  let tailText = '';
  let sawOutput = false;
  let frames = 0, reasoningFrames = 0, netError = null;
  const writeSafe = (s) => {
    try { if (!res.writableEnded && !res.destroyed) { res.write(s); return true; } } catch { /* 客户端断开 */ }
    return false;
  };
  try {
    for await (const chunk of Readable.fromWeb(up.body)) {
      pending += decoder.decode(chunk, { stream: true });
      let got;
      while ((got = takeSseFrame(pending))[0] !== null) {
        pending = got[1];
        const frame = got[0];
        frames++;
        if (/"reasoning"/.test(frame)) reasoningFrames++;
        if (streamHasOutput(frame)) sawOutput = true;
        if (isTerminalFrame(frame)) held.push(frame);
        else writeSafe(frame);
        tailText = (tailText + frame).slice(-65536);
      }
    }
  } catch (e) {
    netError = String((e && e.message) || e);
  }
  pending += decoder.decode();
  if (pending) { // 被掐在半帧上的尾巴：只入判定，不发给客户端
    frames++;
    if (streamHasOutput(pending)) sawOutput = true;
    if (isTerminalFrame(pending)) held.push(pending);
    tailText = (tailText + pending).slice(-65536);
  }
  const heldText = held.join('');
  const usable = sawOutput || streamHasOutput(heldText);
  const finished = streamHasFinish(heldText) || heldText.includes('[DONE]');
  const ms = Date.now() - t0;
  const tail = parseStreamTail(heldText || tailText);
  const clientGone = !!(chain.state && chain.state.clientGone) || !!res.destroyed;
  if (clientGone) {
    logEvent(`stream-client-abort model=${ctx.modelId} account=${acc && acc.name} ms=${ms} round=${ctx.round}`);
    try { record(ctx.modelId, { provider: null, canonical: null, ms, stream: true, error: 'client disconnected mid-stream', account: acc?.name || null, attempts }); } catch {}
    return { ok: false, usable, clientGone: true };
  }
  if (!usable) {
    logEvent(`stream-empty-round model=${ctx.modelId} provider=${tail.provider || '?'} account=${acc?.name} ms=${ms} round=${ctx.round} frames=${frames} reasoning=${reasoningFrames} finish=${finished ? 1 : 0} cut=${netError ? JSON.stringify(netError.slice(0, 100)) : 'none'}`);
    try { record(ctx.modelId, { provider: tail.provider, canonical: tail.canonical, ms, stream: true, error: 'stream ended without content', account: acc?.name || null, attempts, usage: tail.usage }); } catch {}
    return { ok: false, usable: false, clientGone: false };
  }
  // 有可用产出：放行扣住的终止帧；被掐在内容中间就补人工终止帧，让客户端能收下已到的部分
  for (const frame of held) writeSafe(frame);
  if (!finished) {
    if (!streamHasFinish(heldText)) writeSafe(SYNTH_FINISH_FRAME);
    writeSafe('data: [DONE]\n\n');
    logEvent(`stream-cut-partial model=${ctx.modelId} provider=${tail.provider || '?'} account=${acc?.name} ms=${ms} round=${ctx.round} ${netError ? 'err=' + JSON.stringify(netError.slice(0, 100)) : ''}`);
  } else if (ctx.round > 1) {
    logEvent(`stream-retry-ok model=${ctx.modelId} provider=${tail.provider || '?'} account=${acc?.name} ms=${ms} round=${ctx.round}`);
  }
  try { record(ctx.modelId, { provider: tail.provider, canonical: tail.canonical, ms, stream: true, error: null, account: acc?.name || null, attempts, usage: tail.usage, requestedModel: ctx.requestedModelId === ctx.modelId ? null : ctx.requestedModelId }); } catch {}
  try { if (!res.writableEnded) res.end(); } catch {}
  return { ok: true, usable: true, clientGone: false, cut: !finished };
}
async function handleChatStream(req, res, body, modelId, requestedModelId, cfg, targets) {
  const startedAt = Date.now();
  let chain = await runChatChain(req, body, modelId, cfg, { stream: true, res });
  for (let round = 1; ; round++) {
    if (!chain.streamUp) {
      // 上游在 SSE 建立前就失败：还没向客户端发过任何头，沿用原错误返回
      if (!res.headersSent) {
        const attempts = chain.trace.map((t) => t.upstream || 'auto');
        if (chain.status === 200 && chain.out && !chain.out.error) {
          // 上游没按 SSE 回但给了完整 JSON：原样交付（老路径行为）
          try { record(modelId, { provider: chain.routing?.finalProvider || null, canonical: chain.routing?.canonicalSlug || null, ms: Date.now() - startedAt, stream: false, attempts, error: null, account: chain.acc?.name || null, usage: chain.out?.usage }); } catch {}
          return sendJSON(res, 200, chain.out);
        }
        try { record(modelId, { provider: null, canonical: null, ms: Date.now() - startedAt, stream: true, error: chain.netError || 'no upstream stream', account: chain.acc?.name || null, attempts, usage: chain.out?.usage }); } catch {}
        const status = chain.status === 200 ? 502 : (chain.status || 502);
        return sendJSON(res, status, chain.out || { error: { message: chain.netError || 'no upstream response', type: 'upstream_error' } });
      }
      const gone = !!(chain.state && chain.state.clientGone) || !!res.destroyed;
      logEvent(`stream-error model=${modelId} account=${chain.acc?.name || '?'} round=${round} ${gone ? 'client disconnected' : 'pre-stream failed ' + JSON.stringify(String(chain.netError || chain.status || '').slice(0, 120))}`);
      if (!gone) writeStreamErrorFrame(res, `上游流中断：${chain.netError || 'no upstream response'}`);
      return;
    }
    const verdict = await relayStreamRound(req, res, chain, { modelId, requestedModelId, targets, round });
    if (verdict.ok || verdict.clientGone) return;
    if (res.destroyed) return;
    if (round >= STREAM_ROUNDS || Date.now() - startedAt > STREAM_DEADLINE_MS) {
      logEvent(`stream-empty-given-up model=${modelId} rounds=${round} ms=${Date.now() - startedAt}`);
      writeStreamErrorFrame(res, `上游连续 ${round} 轮没有正文（网关侧抖动），已放弃`);
      return;
    }
    chain = await runChatChain(req, body, modelId, cfg, { stream: true, res });
  }
}

async function handleChat(req, res) {
  const raw = await readBody(req);
  let body;
  try { body = JSON.parse(raw.toString('utf8')); } catch { return sendJSON(res, 400, { error: { message: 'invalid JSON body' } }); }
  const requestedModelId = body.model;
  if (!requestedModelId) return sendJSON(res, 400, { error: { message: 'model is required' } });
  const modelId = resolveModelId(requestedModelId, body);
  // 上游必须收到实际模型名，不能收到本地别名；保留 requestedModelId 只用于响应头和账本标记。
  if (modelId !== requestedModelId) body = { ...body, model: modelId };
  body = stripAstrbotRouteDirective(body, requestedModelId);
  body = enrichAstrbotRouteBody(body, requestedModelId, modelId);

  const cfg = config.perModel[modelId] || {};
  const targets = buildAttempts(modelId, cfg).map((a) => a.upstream).filter(Boolean);
  const isStream = !!body.stream;
  // 记账：给流式请求注入 include_usage，网关会在流末尾追加标准 usage 块（OpenAI 规范行为，实测网关支持）
  if (isStream && !body.stream_options?.include_usage) body.stream_options = { ...body.stream_options, include_usage: true };

  // 流式：交给「终止帧扣留 + 空流重试」中继（2026-10-04）；非流式走原来的单次链
  if (isStream) return handleChatStream(req, res, body, modelId, requestedModelId, cfg, targets);

  const chain = await runChatChain(req, body, modelId, cfg, { stream: false, res });

  const { status, out, routing, acc } = chain;
  if (!out) return sendJSON(res, 502, { error: { message: 'no upstream response', type: 'upstream_error' } });
  // 客户端实际使用成功的新订阅模型自动收录进列表
  if (status === 200 && /^cline-pass\//.test(String(modelId)) && !config.knownModels.includes(modelId)) {
    config.knownModels.push(modelId);
    saveConfig();
  }
  record(modelId, {
    provider: routing.finalProvider || null,
    canonical: routing.canonicalSlug || null,
    ms: Date.now() - chain.t0,
    stream: false,
    attempts: chain.trace.map((t) => t.upstream || 'auto'),
    trace: chain.trace,
    error: status !== 200 ? out?.error?.message || null : null,
    account: acc ? acc.name : null,
    usage: out.usage,
    requestedModel: requestedModelId === modelId ? null : requestedModelId,
  });
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'X-Cline-Target-Upstream': targets.length ? targets.join('>') : 'auto',
    'X-Cline-Actual-Upstream': routing.finalProvider || 'unknown',
    'X-Cline-Canonical-Model': routing.canonicalSlug || '',
    'X-Cline-Attempts': String(chain.trace.length),
    'X-Cline-Account': headerSafe(acc ? acc.name : ''),
    'X-Cline-Requested-Model': headerSafe(requestedModelId),
    'X-Cline-Resolved-Model': headerSafe(modelId),
  });
  res.end(JSON.stringify(out));
}

// ---------- HTTP 服务 ----------
// 测速单独读取流，不走故障转移，避免把多家供应商的时间混算。
async function measureStream(response, startedAt) {
  if (!response.body || !/text\/event-stream/i.test(response.headers.get('content-type') || '')) {
    const error = await response.text();
    throw new Error(`上游未返回事件流：${error.slice(0, 500)}`);
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '', dataLines = [], firstPacket = null, firstGenerated = null, lastGenerated = null;
  let completedAt = null, done = false, usage = null, preview = '', size = 0;
  let routing = {};
  function event() {
    if (!dataLines.length) return;
    const data = dataLines.join('\n');
    dataLines = [];
    if (data.trim() === '[DONE]') { done = true; completedAt = performance.now(); return; }
    let chunk;
    try { chunk = JSON.parse(data); } catch { throw new Error('上游事件流包含无效 JSON'); }
    const d = chunk?.data && typeof chunk.data === 'object' ? chunk.data : chunk;
    if (d?.error) throw new Error(errText(d.error?.message || d.error).slice(0, 1000));
    if (d?.usage) usage = d.usage;
    const delta = d?.choices?.[0]?.delta || {};
    const r = parseRouting(d || {});
    const deltaRouting = parseRouting({ choices: [{ message: delta }] });
    for (const info of [r, deltaRouting]) {
      for (const key of ['pipeline', 'canonicalSlug', 'finalProvider']) if (info[key]) routing[key] = info[key];
    }
    const text = typeof delta.content === 'string' ? delta.content : '';
    const reasoning = [delta.reasoning, delta.reasoning_content, ...(Array.isArray(delta.reasoning_details) ? delta.reasoning_details : []).map((item) => item?.text)]
      .some((value) => typeof value === 'string' && value.length > 0);
    if (text || reasoning) {
      const now = performance.now();
      firstGenerated ??= now;
      lastGenerated = now;
      preview = (preview + text).slice(0, 1200);
    }
    if (d?.choices?.some((choice) => choice.finish_reason === 'error')) throw new Error('上游生成失败');
  }
  function lines(flush = false) {
    let end;
    while (!done && (end = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, end).replace(/\r$/, '');
      buffer = buffer.slice(end + 1);
      if (!line) event();
      else if (line.startsWith('data:')) dataLines.push(line.slice(5).replace(/^ /, ''));
    }
    if (flush && !done) {
      if (buffer.startsWith('data:')) dataLines.push(buffer.slice(5).replace(/^ /, '').replace(/\r$/, ''));
      buffer = '';
      event();
    }
  }
  try {
    while (!done) {
      const part = await reader.read();
      if (part.done) { buffer += decoder.decode(); lines(true); break; }
      if (!part.value.length) continue;
      firstPacket ??= performance.now();
      size += part.value.length;
      if (size > 2 * 1024 * 1024) throw new Error('测速响应超出大小限制');
      buffer += decoder.decode(part.value, { stream: true });
      lines();
    }
    completedAt ??= performance.now();
    if (!done) throw new Error('上游流提前断开，测速未完成');
    if (firstGenerated === null) throw new Error('上游未返回生成内容');
    const reportedTokens = metricNumber(usage?.completion_tokens);
    const tokens = Number.isInteger(reportedTokens) && reportedTokens > 0 ? reportedTokens : null;
    const generatedMs = lastGenerated - firstGenerated;
    const totalMs = completedAt - startedAt;
    return {
      firstPacketMs: firstPacket === null ? null : firstPacket - startedAt,
      generationSpeed: tokens !== null && generatedMs > 0 ? tokens * 1000 / generatedMs : null,
      perceivedSpeed: tokens !== null && totalMs > 0 ? tokens * 1000 / totalMs : null,
      completionTokens: tokens, totalMs, preview, ...routing,
    };
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

async function speedTest(model, upstream, signal) {
  const acc = pickAccount();
  if (!acc.key) throw new Error('请先配置上游账号');
  const body = injectPrefs({ model, stream: true, stream_options: { include_usage: true }, max_tokens: 1024,
    messages: [{ role: 'user', content: '直接用中文写一篇约600字的科普短文，介绍雨滴的形成过程，不要列提纲。' }],
  }, model, { upstream, strict: true });
  const startedAt = performance.now();
  const response = await fetch(`${config.upstreamBase}/chat/completions`, {
    method: 'POST', headers: chatHeaders(acc.key), body: JSON.stringify(body), signal,
  });
  if (!response.ok) throw new Error(`上游 HTTP ${response.status}：${(await response.text()).slice(0, 500)}`);
  const measured = await measureStream(response, startedAt);
  const actual = measured.finalProvider || null;
  let detail = null;
  // 只展示本次响应确认的网关和供应商数据；查询时间不计入测速。
  if (actual && measured.pipeline && measured.canonicalSlug) {
    try {
      const source = measured.pipeline === 'planner' ? 'Vercel' : 'OpenRouter';
      const base = source === 'Vercel' ? 'https://ai-gateway.vercel.sh/v1' : OR_API;
      const modelPath = measured.canonicalSlug.split('/').map(encodeURIComponent).join('/');
      const res = await fetch(`${base}/models/${modelPath}/endpoints`, { signal });
      if (res.ok) {
        const json = await res.json();
        const endpoints = Array.isArray(json?.data?.endpoints) ? endpointMetrics(json.data.endpoints, source) : [];
        detail = endpoints.find((d) => norm(d.slug) === norm(actual) || norm(d.name) === norm(actual)) || null;
      }
    } catch (e) { if (signal?.aborted) throw e; /* 公共指标不可用时只展示本次实测 */ }
  }
  const matched = actual ? norm(actual) === norm(upstream) || (detail && norm(detail.slug) === norm(upstream)) : null;
  if (!matched) return { ok: false, error: actual ? `网关实际使用 ${actual}，与指定供应商 ${upstream} 不符，本次测速无效` : '响应未确认实际供应商，本次测速无效' };
  return { ok: true, model, upstream, actual, matched: matched === null ? null : !!matched,
    ...measured, detail, testedAt: Date.now() };
}

function sendJSON(res, status, obj) {
  // 头部可能已经发出（流式中途出错走这里兜底），此时不能再 writeHead，否则抛 ERR_HTTP_HEADERS_SENT
  if (res.headersSent) { try { res.end(); } catch {} return; }
  try {
    res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
  } catch { try { res.end(); } catch {} return; }
  res.end(JSON.stringify(obj));
}

async function catalog() {
  if (META.catalog && Date.now() - (META.catalogFetchedAt || 0) < 3600e3) return META.catalog;
  const { json } = await fetchJSON(`${config.upstreamBase}/models`, { headers: chatHeaders(pickAccount().key) });
  const ids = (json?.data || []).map((m) => m.id);
  if (ids.length) { META.catalog = ids; META.catalogFetchedAt = Date.now(); saveMeta(); }
  return META.catalog || [];
}

const server = http.createServer(async (req, res) => {
  // 客户端中途断线 / 写失败只吞掉，不许升级成进程级未捕获异常
  res.on('error', () => {});
  req.on('error', () => {});
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
      'Access-Control-Allow-Headers': '*',
    });
    return res.end();
  }
  try {
    if (req.method === 'GET' && p === '/api/meta') {
      return sendJSON(res, 200, { authRequired: !!PROXY_KEY && !isLoopback(req), proxyBase: publicProxyBase(), configured: isConfigured() });
    }
    if (p.startsWith('/api/') || p.startsWith('/v1/') || CHAT_PATHS.has(p)) {
      if (!authOK(req)) return unauthorized(res);
    }
    if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
      // 面板每次现读磁盘：改完前端刷新即生效；no-store 免得手机浏览器拿旧的缓存页面
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(fs.readFileSync(path.join(PUBLIC_DIR, 'index-v2.html')));
    }
    if (req.method === 'GET' && p.startsWith('/assets/')) {
      // 控制台静态素材（看板娘图等）；只允许单层文件名，防路径穿越
      const file = p.slice('/assets/'.length);
      if (!/^[\w.-]+$/.test(file)) { res.writeHead(404); return res.end(); }
      const full = path.join(PUBLIC_DIR, 'assets', file);
      if (!fs.existsSync(full)) { res.writeHead(404); return res.end(); }
      const ext = path.extname(file).toLowerCase();
      const mime = { '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.webp': 'image/webp', '.gif': 'image/gif', '.svg': 'image/svg+xml' }[ext] || 'application/octet-stream';
      res.writeHead(200, { 'Content-Type': mime, 'Cache-Control': 'public, max-age=86400' });
      return res.end(fs.readFileSync(full));
    }
    if (req.method === 'GET' && p === '/api/models') {
      const cat = await catalog();
      const sub = config.knownModels.map((id) => ({ id, config: config.perModel[id] || {}, meta: META.models[id] || null, pinState: pinStateOf(id) }));
      return sendJSON(res, 200, { subscription: sub, catalogCount: cat.length, catalog: cat, proxyBase: publicProxyBase(), officialFetch: META.officialModelsFetch || null });
    }
    if (req.method === 'POST' && p === '/api/probe') {
      const { model } = await JSON.parse(await readBody(req).then((b) => b.toString()));
      if (!model) return sendJSON(res, 400, { error: 'model required' });
      const r = await probeModel(model);
      return sendJSON(res, r.ok ? 200 : 502, r);
    }
    if (req.method === 'POST' && p === '/api/speed-test') {
      const { model, upstream } = JSON.parse(await readBody(req).then((b) => b.toString()));
      if (typeof model !== 'string' || !Object.hasOwn(META.models, model) || !validUpstream(upstream)
        || !META.models[model]?.upstreams?.includes(upstream)) {
        return sendJSON(res, 400, { ok: false, error: '请选择模型及其列表中的供应商；没有供应商时先探测或手动收录' });
      }
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 120000);
      const onClose = () => { if (!res.writableEnded) ctrl.abort(); };
      res.on?.('close', onClose);
      try {
        const result = await speedTest(model, upstream, ctrl.signal);
        return sendJSON(res, 200, result);
      } catch (e) {
        return sendJSON(res, 502, { ok: false, error: ctrl.signal.aborted ? '测速超时或连接已断开' : e.message });
      } finally {
        clearTimeout(timer);
        res.off?.('close', onClose);
      }
    }
    if (req.method === 'POST' && p === '/api/test') {
      // 临时配置可带 upstreams/exclude（数组）或旧版 upstream（单值），完整走故障转移链路
      const { model, upstream, upstreams, exclude, pinMode } = await JSON.parse(await readBody(req).then((b) => b.toString()));
      if (!model) return sendJSON(res, 400, { error: 'model required' });
      const t0 = Date.now();
      const cfg = { ...(config.perModel[model] || {}) };
      if (upstreams !== undefined) cfg.upstreams = upstreams;
      else if (upstream !== undefined) cfg.upstreams = upstream ? [upstream] : [];
      if (exclude !== undefined) cfg.exclude = exclude;
      if (pinMode === 'strict' || pinMode === 'preferred') cfg.pinMode = pinMode;
      if (!Array.isArray(cfg.upstreams || []) || !(cfg.upstreams || []).every(validUpstream)) {
        return sendJSON(res, 400, { ok: false, error: '上游名称格式无效' });
      }
      const body = { model, messages: [{ role: 'user', content: 'Reply with the word OK' }], max_tokens: 256 };
      const chain = await runChatChain(req, body, model, cfg, { stream: false, attemptTimeoutMs: 180000, res });
      const trace = chain.trace || [];
      if (chain.status !== 200) {
        const providers = errorProviders(chain.out?.error);
        for (const t of trace) providers.push(...errorProviders(t.note));
        return sendJSON(res, 200, {
          ok: false, error: errText(chain.out?.error?.message || chain.out?.error || 'upstream error').slice(0, 12000),
          errorProviders: [...new Set(providers)],
          targets: (cfg.upstreams || []).filter(Boolean), exclude: cfg.exclude || [], trace,
        });
      }
      const r = parseRouting(chain.out);
      record(model, { provider: r.finalProvider, canonical: r.canonicalSlug, ms: Date.now() - t0, stream: false, attempts: trace.map((t) => t.upstream || 'auto'), error: null, account: chain.acc?.name || null, usage: chain.out?.usage });
      return sendJSON(res, 200, {
        ok: true, ms: Date.now() - t0,
        targets: (cfg.upstreams || []).filter(Boolean), exclude: cfg.exclude || [],
        actual: r.finalProvider, actualName: r.finalProviderName, pipeline: r.pipeline, pinnable: r.pipeline !== null,
        canonicalSlug: r.canonicalSlug, fallbacks: r.fallbacks, content: (r.content || '').slice(0, 120),
        account: chain.acc?.name || null, trace,
      });
    }
    if (req.method === 'POST' && (p === '/api/watch-upstream' || p === '/api/delete-upstream')) {
      const { model, upstream } = JSON.parse(await readBody(req).then((b) => b.toString()));
      if (typeof model !== 'string' || !/^[a-z0-9._-]+\/[a-z0-9._:-]+$/i.test(model)
        || !validUpstream(upstream)) return sendJSON(res, 400, { ok: false, error: '模型或上游名称格式无效' });
      if (!config.knownModels.includes(model) && !Object.hasOwn(META.models, model)) {
        return sendJSON(res, 400, { ok: false, error: '请先选择或探测对应模型' });
      }
      const meta = (META.models[model] ||= {});
      if (p === '/api/delete-upstream') {
        meta.deletedUpstreams = [...new Set([...(meta.deletedUpstreams || []), upstream])];
        pruneDeletedUpstreams(model);
        saveConfig();
      } else {
        meta.deletedUpstreams = (meta.deletedUpstreams || []).filter((u) => u !== upstream);
        meta.watchedUpstreams = [...new Set([...(meta.watchedUpstreams || []), upstream])];
        meta.upstreams = [...new Set([...(meta.upstreams || []), upstream])];
      }
      saveMeta();
      return sendJSON(res, 200, { ok: true, meta, config: config.perModel[model] || {} });
    }
    if (req.method === 'GET' && p === '/api/accounts') {
      return sendJSON(res, 200, {
        accounts: config.accounts,
        mode: config.accountMode,
        active: config.activeAccount,
        autoRecover: config.autoRecover !== false,
        stats: META.stats || {},
      });
    }
    if (req.method === 'POST' && p === '/api/accounts') {
      const body = JSON.parse(await readBody(req).then((b) => b.toString()));
      // 同名账号保留 addedAt/renewStart（前端整体保存账号池时不丢导入时间与订阅日修正）
      // 同时保留熔断/自动恢复的痕迹：否则每次「保存账号配置」都会把禁用原因和复检进度抹掉
      const prev = new Map((config.accounts || []).map((a) => [a.name, a]));
      const keepOff = (a, old, field) => {
        if (a.enabled !== false) return null;          // 重新启用的号：痕迹清空
        const v = a[field] !== undefined ? a[field] : old[field];
        return v === undefined ? null : v;
      };
      const accs = (Array.isArray(body.accounts) ? body.accounts : [])
        .map((a, i) => {
          const name = String(a.name || `账号${i + 1}`).slice(0, 50);
          const old = prev.get(name) || {};
          return {
            name,
            key: String(a.key || '').trim(),
            enabled: a.enabled !== false,
            addedAt: a.addedAt ? String(a.addedAt) : (old.addedAt || new Date().toISOString()),
            renewStart: a.renewStart ? String(a.renewStart) : (old.renewStart || null),
            disabledKind: keepOff(a, old, 'disabledKind'),
            disabledReason: keepOff(a, old, 'disabledReason'),
            disabledAt: keepOff(a, old, 'disabledAt'),
            quotaResetAt: a.quotaResetAt !== undefined ? a.quotaResetAt : (old.quotaResetAt ?? null),
            recoverTries: old.recoverTries ?? 0,
            recoverLastAt: old.recoverLastAt ?? null,
            recoverNextAt: a.enabled !== false ? null : (a.recoverNextAt !== undefined ? a.recoverNextAt : (old.recoverNextAt ?? null)),
            recoveredAt: a.recoveredAt !== undefined ? a.recoveredAt : (old.recoveredAt ?? null),
          };
        })
        .filter((a) => a.key);
      if (!accs.length) return sendJSON(res, 400, { error: { message: '至少需要一个有效账号（key 非空）' } });
      config.accounts = accs;
      if (body.autoRecover !== undefined) config.autoRecover = body.autoRecover !== false;
      config.accountMode = ['roundrobin', 'smart'].includes(body.mode) ? body.mode : 'single';
      config.activeAccount = Math.min(Math.max(0, Number(body.active) || 0), accs.length - 1);
      saveConfig();
      RR_COUNTER = 0;
      SMART_COUNTER = 0;
      if (config.accountMode === 'smart') maybeRefreshPlanData();
      return sendJSON(res, 200, { ok: true, accounts: config.accounts.length, mode: config.accountMode, active: config.activeAccount, autoRecover: config.autoRecover !== false });
    }
    if (req.method === 'POST' && p === '/api/accounts/test') {
      const { key } = JSON.parse(await readBody(req).then((b) => b.toString()));
      const k = String(key || '').trim();
      if (!k) return sendJSON(res, 400, { error: { message: 'key required' } });
      const t0 = Date.now();
      const model = config.knownModels[0] || 'cline-pass/glm-5.3-flash';
      const { json } = await fetchJSON(`${config.upstreamBase}/chat/completions`, {
        method: 'POST',
        headers: chatHeaders(k),
        body: JSON.stringify({ model, messages: [{ role: 'user', content: 'Say OK' }], max_tokens: 512 }),
      }, 120000);
      if (json?.error && !json?.data) {
        const msg = typeof json.error === 'string' ? json.error : JSON.stringify(json.error);
        const authFail = /unauthorized|re-authenticate|invalid\s*api|401/i.test(msg);
        // 密钥无效会直接 Unauthorized；其他错误（如推理模型耗尽 max_tokens 的 empty response）
        // 说明鉴权已通过，不应误报为密钥问题
        return sendJSON(res, 200, authFail
          ? { ok: false, ms: Date.now() - t0, error: `密钥无效或未授权：${msg.slice(0, 160)}` }
          : { ok: true, ms: Date.now() - t0, model, note: `密钥鉴权通过；网关提示：${msg.slice(0, 120)}` });
      }
      return sendJSON(res, 200, { ok: true, ms: Date.now() - t0, model });
    }
    if (req.method === 'POST' && p === '/api/accounts/auto-trim') {
      // 逐账号发真实小请求体检，按错误类别自动禁用额度满/密钥坏的账号（网络失败只报告不误禁）
      // 判据与后台自动恢复共用 probeAccountUsable：只有网关把"这个号"挡掉才算坏，空正文/上游抖动不算
      const probeModel = config.knownModels.includes('cline-pass/glm-5.3-flash') ? 'cline-pass/glm-5.3-flash' : (config.knownModels[0] || 'cline-pass/glm-5.3-flash');
      const results = await Promise.all((config.accounts || []).filter((a) => a && a.key).map(async (a) => {
        const t = Date.now();
        const p = await probeAccountUsable(a);
        const status = p.status, msg = p.msg;
        const ok = p.ok;
        const kind = ok ? null : classifyAccountError(status, msg);
        let disabled = false, recovered = false;
        if (ok && a.enabled === false && effectiveDisabledKind(a) !== 'auth') {
          // 手动体检顺手把已经恢复的号放回去（auth 类除外，那个必须换 key）
          a.enabled = true;
          a.disabledKind = null;
          a.disabledReason = null;
          a.disabledAt = null;
          a.recoverTries = 0;
          a.recoverNextAt = null;
          a.recoveredAt = new Date().toISOString();
          recovered = true;
          saveConfig();
        } else if (kind) {
          disabled = noteAccountFailure(a.name, kind, msg);
        }
        return { name: a.name, status, ok, kind, disabled, recovered, enabled: a.enabled !== false, msg: String(msg).slice(0, 120), ms: Date.now() - t };
      }));
      return sendJSON(res, 200, { ok: true, probed: probeModel, results });
    }
    if (req.method === 'POST' && p === '/api/accounts/recover-now') {
      // 手动催一次复检：把退避计时清零后立刻巡查一遍（跳过等待，但不跳过探测本身）
      for (const a of config.accounts || []) {
        if (a && a.enabled === false && effectiveDisabledKind(a) !== 'auth') a.recoverNextAt = null;
      }
      await autoRecoverTick();
      return sendJSON(res, 200, {
        ok: true,
        accounts: (config.accounts || []).map((a) => ({
          name: a.name, enabled: a.enabled !== false, disabledKind: a.enabled === false ? effectiveDisabledKind(a) : null,
          recoverTries: a.recoverTries || 0, recoverLastAt: a.recoverLastAt || null,
          recoverNextAt: a.recoverNextAt || null, recoveredAt: a.recoveredAt || null,
        })),
      });
    }
    if (req.method === 'GET' && p === '/api/security') {
      return sendJSON(res, 200, { proxyKey: config.proxyKey || '', publicBaseUrl: config.publicBaseUrl || '', authRequired: !!PROXY_KEY && !isLoopback(req), exposeCatalog: !!config.exposeCatalog });
    }
    if (req.method === 'POST' && p === '/api/security') {
      const body = JSON.parse(await readBody(req).then((b) => b.toString()));
      if (body.proxyKey !== undefined) config.proxyKey = String(body.proxyKey).trim();
      if (body.publicBaseUrl !== undefined) config.publicBaseUrl = String(body.publicBaseUrl).trim().replace(/\/+$/, '');
      if (body.exposeCatalog !== undefined) config.exposeCatalog = !!body.exposeCatalog;
      saveConfig();
      PROXY_KEY = config.proxyKey || '';
      return sendJSON(res, 200, { ok: true, proxyKey: config.proxyKey, publicBaseUrl: config.publicBaseUrl, authRequired: !!PROXY_KEY && !isLoopback(req), proxyBase: publicProxyBase(), exposeCatalog: !!config.exposeCatalog });
    }
    if (req.method === 'POST' && p === '/api/validate-upstreams') {
      const { model } = JSON.parse(await readBody(req).then((b) => b.toString()));
      if (!model) return sendJSON(res, 400, { error: { message: 'model required' } });
      const results = await validateUpstreams(model);
      const summary = { ok: 0, limited: 0, bad: 0, auth: 0, unknown: 0 };
      for (const r of Object.values(results)) summary[r.status] = (summary[r.status] || 0) + 1;
      return sendJSON(res, 200, { ok: true, summary, results, upstreams: META.models[model]?.upstreams || [] });
    }
    if (req.method === 'POST' && p === '/api/fetch-official-models') {
      const r = await fetchOfficialModels();
      return sendJSON(res, 200, { ok: true, ...r });
    }
    if (req.method === 'GET' && p === '/api/history') return sendJSON(res, 200, { history: META.history });
    if (req.method === 'GET' && p === '/api/usage') {
      ensureLedger(); // 首次访问补播种（旧数据升级过来时）
      return sendJSON(res, 200, { usage: META.usage, totals: META.usageTotals, daily: META.usageDaily });
    }
    if (req.method === 'GET' && p === '/api/plan-usage') {
      const force = url.searchParams.get('refresh') === '1';
      const onlyName = url.searchParams.get('name') || null;
      return sendJSON(res, 200, await getPlanUsage(force, onlyName));
    }
    if (req.method === 'GET' && p === '/api/config') return sendJSON(res, 200, { port: config.port, perModel: config.perModel, knownModels: config.knownModels });
    if (req.method === 'POST' && p === '/api/config') {
      const body = JSON.parse(await readBody(req).then((b) => b.toString()));
      if (body.perModel) {
        for (const [m, c] of Object.entries(body.perModel)) {
          const limit = Math.max(10, META.models[m]?.upstreams?.length || 0);
          const normalize = (v) => [...new Set((Array.isArray(v) ? v : []).map((s) => String(s).trim()).filter(Boolean))].slice(0, limit);
          let upstreams = normalize(c.upstreams);
          const exclude = normalize(c.exclude);
          const excl = new Set(exclude);
          const deleted = new Set(META.models[m]?.deletedUpstreams || []);
          upstreams = upstreams.filter((u) => !excl.has(u) && !deleted.has(u)); // 同时出现以 exclude 为准
          const upstream = upstreams[0] || null; // 旧字段兼容镜像
          // 账号钉选（perModel.accountPin）：面板只编辑上游字段，保存时必须原样带过去；
          // body 显式传 accountPin（字符串=钉住 / null=解除 / 未传=保持原值）才覆盖。
          const prevPin = config.perModel[m]?.accountPin ?? null;
          const nextPin = c.accountPin === null ? null
            : (typeof c.accountPin === 'string' ? (c.accountPin.trim() || null) : prevPin);
          config.perModel[m] = {
            upstream,
            upstreams,
            exclude: exclude.filter((u) => !deleted.has(u)),
            pinMode: c.pinMode === 'preferred' ? 'preferred' : 'strict',
            sort: ['cost', 'ttft', 'tps'].includes(c.sort) ? c.sort : null,
            accountPin: nextPin,
          };
        }
        saveConfig();
      }
      return sendJSON(res, 200, { ok: true });
    }
    if (req.method === 'GET' && (p === '/v1/models' || p === '/api/v1/models' || p === '/models')) {
      // 默认只暴露订阅模型，避免目录模型淹没客户端的模型选择器；exposeCatalog=true 时合并完整目录
      const ids = config.exposeCatalog
        ? [...new Set([...config.knownModels, ...(await catalog())])]
        : [...new Set([...config.knownModels, ...Object.keys(config.perModel)])];
      return sendJSON(res, 200, { object: 'list', data: ids.map((id) => ({ id, object: 'model' })) });
    }
    if (CHAT_PATHS.has(p) && req.method === 'POST') {
      // 必须自己接住：handleChat 的拒绝一旦逃逸就是 unhandledRejection，Node 会当场杀掉整个进程
      return handleChat(req, res).catch((e) => {
        logEvent(`chat-uncaught ${p} ${e?.stack || e}`);
        console.error('[chat] 未捕获异常：', e?.stack || e);
        try { if (!res.headersSent) sendJSON(res, 502, { error: { message: `proxy error: ${e?.message || e}` } }); else res.end(); } catch {}
      });
    }
    return sendJSON(res, 404, { error: { message: `no route: ${req.method} ${p}` } });
  } catch (e) {
    return sendJSON(res, 500, { error: { message: e.message } });
  }
});

// HTTP 响应头只允许 Latin-1，账号名里的中文等字符需要清洗（历史/统计仍用原名）
const headerSafe = (s) => String(s ?? '').replace(/[^\x20-\x7E]/g, '').trim().slice(0, 80) || '-';

server.on('error', (e) => {
  logEvent(`listen-error port=${config.port} ${e?.message || e}`);
  console.error(`[错误] 端口 ${config.port} 监听失败（可能被占用）：${e.message}`);
  process.exit(1);
});

const BIND_HOST = process.env.BIND_HOST || '127.0.0.1';
startAutoRecover(); // 被自动禁用的号，后台自己复检、恢复了自己回到轮询
server.listen(config.port, BIND_HOST, () => {
  console.log(`Cline Pass 上游控制台:  http://127.0.0.1:${config.port}/`);
  console.log(`OpenAI 兼容代理地址:   http://127.0.0.1:${config.port}/v1`);
  if (!['127.0.0.1', 'localhost', '::1'].includes(BIND_HOST) && !PROXY_KEY) {
    console.warn('[警告] 监听非回环地址但未设置代理密钥：控制台和代理对整个网络开放，请先在「访问与安全」里设置代理密钥。');
  }
});
