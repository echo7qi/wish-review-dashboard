/* 祈愿单项目复盘（独立站点 wish-review-dashboard）：目录绑定单独存 IndexedDB，避免与 echo7qi.github.io/cursor-demo
 * 等同源下的「运营宣推」看板共用 ops-dashboard-local-db 导致互相覆盖。各子夹内多个 CSV 按修改时间从旧到新合并。 */

const DB_NAME = 'wish-review-dashboard-local-db';
const DB_STORE = 'kv';
const DB_KEY_DIR_HANDLE = 'boundDirHandle';

/** 绑定根目录下进入「祈愿收入复盘」 */
const REVIEW_ROOT_CANDIDATES = ['祈愿收入复盘'];

/** 与生成脚本 --data-bundle 子目录名一致 */
const BUNDLE_SUBS = {
  main: ['整体数据监测'],
  work: ['作品明细表'],
  layer: ['分层用户监测'],
};

const SUB_LABEL = {
  main: '整体数据监测',
  work: '作品明细表',
  bench: '历史品类池（与整体数据监测同夹）',
  layer: '分层用户监测',
};

async function resolveReviewRootFromBoundRoot(rootHandle) {
  if (!rootHandle) return null;
  const rootName = typeof rootHandle.name === 'string' ? rootHandle.name : '';
  if (rootName && REVIEW_ROOT_CANDIDATES.includes(rootName)) {
    return { handle: rootHandle, name: rootName };
  }
  // Fallback: if the bound directory already contains "整体数据监测" directly,
  // treat it as the review root (user may bind "祈愿收入复盘" itself).
  try {
    if (BUNDLE_SUBS?.main?.length) {
      for (const mainSubName of BUNDLE_SUBS.main) {
        const h = await rootHandle.getDirectoryHandle(mainSubName, { create: false });
        if (h) return { handle: rootHandle, name: rootName || '祈愿收入复盘' };
      }
    }
  } catch (_) {
    // ignore and continue
  }
  return resolveFirstChildDir(rootHandle, REVIEW_ROOT_CANDIDATES);
}

// ─── Local "fish" full 5D HTML index (read via File System Access API) ───
// Map: `${topic}||${periodNo}` -> { fileHandle, fileName, topic, periodNo }
let fishHtmlIndex = null;

function normTopicForFishKey(s) {
  return String(s || '').trim();
}

function fishKey(topic, periodNo) {
  return `${normTopicForFishKey(topic)}||${String(periodNo)}`;
}

function parseFishReportFromFileName(fileName) {
  const name = String(fileName || '');
  // Example: 人鱼陷落-第15期祈愿-五维复盘.html
  let m = name.match(/^(.*?)-第(\d+)期.*五维复盘\.html$/i);
  if (!m) {
    // Example: 人鱼陷落 · 第15期祈愿-五维复盘.html
    m = name.match(/^(.*?)[·\- ]+第(\d+)期.*五维复盘\.html$/i);
  }
  if (!m) return null;
  const topic = String(m[1] || '').trim();
  const periodNo = parseInt(String(m[2] || ''), 10);
  if (!topic || !Number.isFinite(periodNo)) return null;
  return { topic, periodNo };
}

async function scanFishHtmlHandlesRecursive(dirHandle, depth) {
  const out = [];
  if (!dirHandle || typeof dirHandle.values !== 'function') return out;
  const nextDepth = typeof depth === 'number' ? depth : 3;

  // eslint-disable-next-line no-restricted-syntax
  for await (const entry of dirHandle.values()) {
    if (!entry) continue;
    if (entry.kind === 'file') {
      const lower = String(entry.name || '').toLowerCase();
      if (!lower.endsWith('.html')) continue;
      if (!/五维复盘/i.test(entry.name)) continue;
      const parsed = parseFishReportFromFileName(entry.name);
      if (!parsed) continue;
      out.push({
        ...parsed,
        fileHandle: entry,
        fileName: entry.name,
      });
      continue;
    }

    if (entry.kind === 'directory' && nextDepth > 0) {
      // eslint-disable-next-line no-await-in-loop
      const sub = await scanFishHtmlHandlesRecursive(entry, nextDepth - 1);
      out.push(...sub);
    }
  }

  return out;
}

async function ensureFishHtmlIndex() {
  if (fishHtmlIndex) return fishHtmlIndex;

  const root = await getBoundDirHandle();
  if (!root) {
    fishHtmlIndex = new Map();
    return fishHtmlIndex;
  }

  const perm = await root.queryPermission?.({ mode: 'read' });
  if (perm !== 'granted') {
    const req = await root.requestPermission?.({ mode: 'read' });
    if (req !== 'granted') {
      fishHtmlIndex = new Map();
      return fishHtmlIndex;
    }
  }

  const review = await resolveReviewRootFromBoundRoot(root);
  if (!review) {
    fishHtmlIndex = new Map();
    return fishHtmlIndex;
  }

  const items = await scanFishHtmlHandlesRecursive(review.handle, 4);
  const idx = new Map();
  for (const it of items) {
    idx.set(fishKey(it.topic, it.periodNo), it);
  }
  fishHtmlIndex = idx;
  return fishHtmlIndex;
}

async function readFishReportHtml(topic, periodNo) {
  const index = await ensureFishHtmlIndex();
  const key = fishKey(topic, periodNo);
  const it = index.get(key);
  if (!it) {
    return {
      ok: false,
      error: `未找到本地完整五维复盘 HTML：${String(topic)} 第${String(periodNo)}期（请检查文件名是否为 *-第N期*-五维复盘.html）。`,
    };
  }
  const file = await it.fileHandle.getFile();
  const html = await file.text();
  return { ok: true, html, fileName: it.fileName, topic: it.topic, periodNo: it.periodNo };
}

async function scanMonitorCsvMetaFromBoundRoot() {
  const root = await getBoundDirHandle();
  if (!root) {
    return { ok: false, error: '尚未绑定数据文件夹。请先在左侧点击「绑定数据文件夹」。' };
  }

  const perm = await root.queryPermission?.({ mode: 'read' });
  if (perm !== 'granted') {
    const req = await root.requestPermission?.({ mode: 'read' });
    if (req !== 'granted') return { ok: false, error: '未获得文件夹读取权限。' };
  }

  const review = await resolveReviewRootFromBoundRoot(root);
  if (!review) return { ok: false, error: '未找到祈愿收入复盘目录（在绑定根目录下）。' };

  const mainSub = await resolveFirstChildDir(review.handle, BUNDLE_SUBS.main);
  if (!mainSub) return { ok: false, error: '未找到整体数据监测子文件夹。' };

  const mainFiles = await listCsvWithMtime(mainSub.handle);
  const mainMerge = listMonitoringCsvMetasForMerge(mainFiles);
  if (!mainMerge.length) {
    return { ok: false, error: '整体数据监测下未找到可合并的监测 CSV。' };
  }

  const lastModified = Math.max(...mainMerge.map((x) => x.lastModified));
  return {
    ok: true,
    lastModified,
    fileName: mainMerge.length === 1 ? mainMerge[0].name : `合并 ${mainMerge.length} 个 CSV`,
  };
}

function openDb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, 1);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(DB_STORE)) db.createObjectStore(DB_STORE);
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function dbGet(key) {
  const db = await openDb();
  try {
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(DB_STORE, 'readonly');
      const store = tx.objectStore(DB_STORE);
      const req = store.get(key);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  } finally {
    db.close();
  }
}

async function dbSet(key, value) {
  const db = await openDb();
  try {
    await new Promise((resolve, reject) => {
      const tx = db.transaction(DB_STORE, 'readwrite');
      const store = tx.objectStore(DB_STORE);
      const req = store.put(value, key);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  } finally {
    db.close();
  }
}

async function getBoundDirHandle() {
  try {
    return (await dbGet(DB_KEY_DIR_HANDLE)) || null;
  } catch (_) {
    return null;
  }
}

async function pickAndBindFolder() {
  if (!window.showDirectoryPicker) {
    throw new Error('当前浏览器不支持「绑定文件夹」（需 Chrome/Edge 的 File System Access API）。');
  }
  if (window.top && window.top !== window.self) {
    throw new Error('当前在 iframe 内，请新窗口打开本页再绑定。');
  }
  const handle = await window.showDirectoryPicker({ mode: 'read' });
  await dbSet(DB_KEY_DIR_HANDLE, handle);
  return handle;
}

async function resolveFirstChildDir(parent, names) {
  for (const name of names) {
    try {
      const h = await parent.getDirectoryHandle(name, { create: false });
      return { handle: h, name };
    } catch (_) {}
  }
  return null;
}

async function listCsvWithMtime(dirHandle) {
  const out = [];
  // eslint-disable-next-line no-restricted-syntax
  for await (const entry of dirHandle.values()) {
    if (!entry || entry.kind !== 'file') continue;
    if (!entry.name.toLowerCase().endsWith('.csv')) continue;
    const file = await entry.getFile();
    out.push({ name: entry.name, lastModified: file.lastModified });
  }
  return out;
}

function pickBenchmarkFromMainDir(list) {
  if (!list.length) return null;
  const noMonitor = list.filter((x) => !x.name.includes('整体数据监测'));
  if (!noMonitor.length) return null;
  const prefer = noMonitor.filter(
    (x) =>
      /池.*历史|历史.*池|池历史数据|漫改耽美池/i.test(x.name),
  );
  const pool = prefer.length ? prefer : noMonitor;
  pool.sort((a, b) => b.lastModified - a.lastModified);
  return pool[0];
}

/** 文件名是否像「品类池/历史池」对标表（需与 pickBenchmarkFromMainDir 逻辑一致，用于多 CSV 合并时排除） */
function isLikelyBenchmarkPoolCsvFileName(fileName) {
  const name = String(fileName || '');
  if (!name.toLowerCase().endsWith('.csv')) return true;
  if (name.includes('整体数据监测') || /^①/.test(name)) return false;
  return /池.*历史|历史.*池|池历史数据|漫改耽美池/i.test(name);
}

/** 整体数据监测夹内：参与合并的全部监测 CSV（排除对标池文件名），按修改时间从旧到新，便于去重时新文件覆盖旧文件 */
function listMonitoringCsvMetasForMerge(list) {
  const metas = list.filter((f) => !isLikelyBenchmarkPoolCsvFileName(f.name));
  metas.sort((a, b) => a.lastModified - b.lastModified);
  return metas;
}

/** 子文件夹内全部 .csv，按修改时间从旧到新（合并时后读覆盖同键） */
function listAllCsvMetasSortedAsc(list) {
  const metas = list.filter((f) => f.name.toLowerCase().endsWith('.csv'));
  metas.sort((a, b) => a.lastModified - b.lastModified);
  return metas;
}

/** 与整体监测同夹：文件名判定为对标池的 CSV，按时间从旧到新；无则回退 pickBenchmarkFromMainDir 的单个文件 */
function listBenchmarkCsvMetasForMerge(mainFileList) {
  const metas = mainFileList.filter(
    (f) => f.name.toLowerCase().endsWith('.csv') && isLikelyBenchmarkPoolCsvFileName(f.name),
  );
  metas.sort((a, b) => a.lastModified - b.lastModified);
  if (metas.length) return metas;
  const one = pickBenchmarkFromMainDir(mainFileList);
  return one ? [one] : [];
}

async function scanBundleFromRoot(rootHandle) {
  const perm = await rootHandle.queryPermission?.({ mode: 'read' });
  if (perm !== 'granted') {
    const req = await rootHandle.requestPermission?.({ mode: 'read' });
    if (req !== 'granted') throw new Error('未获得文件夹读取权限。');
  }

  const review = await resolveReviewRootFromBoundRoot(rootHandle);
  if (!review) {
    throw new Error(
      `未找到「${REVIEW_ROOT_CANDIDATES.join('」或「')}」文件夹。请在绑定的「资源位数据更新」目录下创建「祈愿收入复盘」。`,
    );
  }

  const result = {
    reviewFolderName: review.name,
    rows: [],
  };

  async function oneMergeAllInSubdir(key, subNames) {
    const label = SUB_LABEL[key];
    const sub = await resolveFirstChildDir(review.handle, subNames);
    if (!sub) {
      result.rows.push({ key, label, ok: false, detail: `缺少子文件夹「${subNames[0]}」` });
      return;
    }
    const files = await listCsvWithMtime(sub.handle);
    const toMerge = listAllCsvMetasSortedAsc(files);
    if (!toMerge.length) {
      result.rows.push({
        key,
        label,
        ok: false,
        detail: `「${sub.name}」下无 CSV`,
        sub: sub.name,
      });
      return;
    }
    const names = toMerge.map((x) => x.name);
    result.rows.push({
      key,
      label,
      ok: true,
      sub: sub.name,
      file: names.join(' + '),
      files: names,
      lastModified: Math.max(...toMerge.map((x) => x.lastModified)),
    });
  }

  const mainSub = await resolveFirstChildDir(review.handle, BUNDLE_SUBS.main);
  if (!mainSub) {
    result.rows.push({
      key: 'main',
      label: SUB_LABEL.main,
      ok: false,
      detail: `缺少子文件夹「${BUNDLE_SUBS.main[0]}」`,
    });
    result.rows.push({
      key: 'bench',
      label: SUB_LABEL.bench,
      ok: false,
      detail: '依赖「整体数据监测」文件夹',
    });
  } else {
    const mainFiles = await listCsvWithMtime(mainSub.handle);
    const toMerge = listMonitoringCsvMetasForMerge(mainFiles);
    if (!toMerge.length) {
      result.rows.push({
        key: 'main',
        label: SUB_LABEL.main,
        ok: false,
        detail: `「${mainSub.name}」下无可合并的监测 CSV（已排除对标池类文件名）`,
      });
    } else {
      const names = toMerge.map((x) => x.name);
      result.rows.push({
        key: 'main',
        label: SUB_LABEL.main,
        ok: true,
        sub: mainSub.name,
        file: names.join(' + '),
        files: names,
        lastModified: Math.max(...toMerge.map((x) => x.lastModified)),
      });
    }
    const benchMerge = listBenchmarkCsvMetasForMerge(mainFiles);
    if (!benchMerge.length) {
      result.rows.push({
        key: 'bench',
        label: SUB_LABEL.bench,
        ok: false,
        detail: `「${mainSub.name}」内未找到可对标的池类 CSV（如 *池历史数据*.csv）`,
      });
    } else {
      const bnames = benchMerge.map((x) => x.name);
      result.rows.push({
        key: 'bench',
        label: SUB_LABEL.bench,
        ok: true,
        sub: `${mainSub.name}（同夹）`,
        file: bnames.join(' + '),
        files: bnames,
        lastModified: Math.max(...benchMerge.map((x) => x.lastModified)),
      });
    }
  }

  await oneMergeAllInSubdir('work', BUNDLE_SUBS.work);
  await oneMergeAllInSubdir('layer', BUNDLE_SUBS.layer);

  return result;
}

async function runScan() {
  try {
    // Invalidate cache so newly generated fish HTML can be picked up.
    fishHtmlIndex = null;

    const root = await getBoundDirHandle();
    if (!root) return;
    const scan = await scanBundleFromRoot(root);
    window.__WISH_REVIEW_BUNDLE_SCAN__ = scan;
  } catch (e) {
    console.warn('[wish-review] 扫描失败', e?.message || e);
  }
}

async function readTextPartsFromDir(dirHandle, metas) {
  const parts = [];
  for (let i = 0; i < metas.length; i++) {
    const meta = metas[i];
    const fh = await dirHandle.getFileHandle(meta.name);
    const file = await fh.getFile();
    parts.push({
      name: file.name,
      text: await file.text(),
      lastModified: file.lastModified,
    });
  }
  return parts;
}

function finalizeCsvPartsResult(parts) {
  const lastModified = Math.max(...parts.map((p) => p.lastModified));
  return {
    ok: true,
    parts,
    fileNames: parts.map((p) => p.name),
    fileName: parts.length === 1 ? parts[0].name : `合并 ${parts.length} 个 CSV`,
    lastModified,
  };
}

/**
 * 一次读取祈愿复盘包：整体数据监测（多表合并）+ 同夹对标池 + 分层用户监测 + 作品明细表。
 * 各夹内均为全部 .csv 按修改时间从旧到新读入（监测夹内仍排除池类文件名）。
 */
async function readFullWishReviewBundleFromBoundRoot() {
  const root = await getBoundDirHandle();
  if (!root) {
    return {
      ok: false,
      error: '尚未绑定数据文件夹。请先在左侧点击「绑定数据文件夹」。',
      main: null,
    };
  }
  const perm = await root.queryPermission?.({ mode: 'read' });
  if (perm !== 'granted') {
    const req = await root.requestPermission?.({ mode: 'read' });
    if (req !== 'granted') {
      return { ok: false, error: '未获得文件夹读取权限。', main: null };
    }
  }
  const review = await resolveReviewRootFromBoundRoot(root);
  if (!review) {
    return {
      ok: false,
      error: `未找到「${REVIEW_ROOT_CANDIDATES.join('」或「')}」文件夹。`,
      main: null,
    };
  }
  const mainSub = await resolveFirstChildDir(review.handle, BUNDLE_SUBS.main);
  if (!mainSub) {
    return { ok: false, error: `未找到子文件夹「${BUNDLE_SUBS.main[0]}」。`, main: null };
  }
  const mainFiles = await listCsvWithMtime(mainSub.handle);
  const mainMerge = listMonitoringCsvMetasForMerge(mainFiles);
  if (!mainMerge.length) {
    return {
      ok: false,
      error:
        '「整体数据监测」内未找到可合并的监测 CSV（已排除文件名像品类池/历史池的对标表）。',
      main: null,
    };
  }

  const mainParts = await readTextPartsFromDir(mainSub.handle, mainMerge);
  const main = finalizeCsvPartsResult(mainParts);

  const benchMerge = listBenchmarkCsvMetasForMerge(mainFiles);
  const bench =
    benchMerge.length > 0
      ? finalizeCsvPartsResult(await readTextPartsFromDir(mainSub.handle, benchMerge))
      : {
          ok: false,
          skipped: true,
          parts: [],
          fileNames: [],
          error: '同夹内无对标池 CSV',
        };

  const layerSub = await resolveFirstChildDir(review.handle, BUNDLE_SUBS.layer);
  let layer = {
    ok: false,
    skipped: true,
    parts: [],
    fileNames: [],
    error: '缺少「分层用户监测」文件夹',
  };
  if (layerSub) {
    const layerMetas = listAllCsvMetasSortedAsc(await listCsvWithMtime(layerSub.handle));
    if (layerMetas.length) {
      layer = finalizeCsvPartsResult(await readTextPartsFromDir(layerSub.handle, layerMetas));
    } else {
      layer = {
        ok: false,
        skipped: true,
        parts: [],
        fileNames: [],
        error: '「分层用户监测」内无 CSV',
      };
    }
  }

  const workSub = await resolveFirstChildDir(review.handle, BUNDLE_SUBS.work);
  let work = {
    ok: false,
    skipped: true,
    parts: [],
    fileNames: [],
    error: '缺少「作品明细表」文件夹',
  };
  if (workSub) {
    const workMetas = listAllCsvMetasSortedAsc(await listCsvWithMtime(workSub.handle));
    if (workMetas.length) {
      work = finalizeCsvPartsResult(await readTextPartsFromDir(workSub.handle, workMetas));
    } else {
      work = {
        ok: false,
        skipped: true,
        parts: [],
        fileNames: [],
        error: '「作品明细表」内无 CSV',
      };
    }
  }

  return {
    ok: true,
    main,
    bench,
    layer,
    work,
  };
}

/**
 * 仅读取「整体数据监测」合并 CSV。
 * 不与 readFull 共用实现：全包会同时载入对标池/分层/作品明细，大文件下易导致标签页 OOM（Chrome 错误代码 5）。
 */
async function readMonitorCsvFromBoundRoot() {
  const root = await getBoundDirHandle();
  if (!root) {
    return { ok: false, error: '尚未绑定数据文件夹。请先在左侧点击「绑定数据文件夹」。' };
  }
  const perm = await root.queryPermission?.({ mode: 'read' });
  if (perm !== 'granted') {
    const req = await root.requestPermission?.({ mode: 'read' });
    if (req !== 'granted') {
      return { ok: false, error: '未获得文件夹读取权限。' };
    }
  }
  const review = await resolveReviewRootFromBoundRoot(root);
  if (!review) {
    return {
      ok: false,
      error: `未找到「${REVIEW_ROOT_CANDIDATES.join('」或「')}」文件夹。`,
    };
  }
  const mainSub = await resolveFirstChildDir(review.handle, BUNDLE_SUBS.main);
  if (!mainSub) {
    return { ok: false, error: `未找到子文件夹「${BUNDLE_SUBS.main[0]}」。` };
  }
  const mainFiles = await listCsvWithMtime(mainSub.handle);
  const mainMerge = listMonitoringCsvMetasForMerge(mainFiles);
  if (!mainMerge.length) {
    return {
      ok: false,
      error:
        '「整体数据监测」内未找到可合并的监测 CSV（已排除文件名像品类池/历史池的对标表）。',
    };
  }
  const mainParts = await readTextPartsFromDir(mainSub.handle, mainMerge);
  return { ok: true, ...finalizeCsvPartsResult(mainParts) };
}

/** 慎用：会一次性读入监测同夹+分层+作品明细全部 CSV，数据大时易 OOM；看板默认只用 wishReviewReadMonitorCsv */
window.wishReviewReadFullBundle = readFullWishReviewBundleFromBoundRoot;
window.wishReviewReadMonitorCsv = readMonitorCsvFromBoundRoot;
window.wishReviewReadFishReportHtml = readFishReportHtml;
window.wishReviewScanMonitorCsvMeta = scanMonitorCsvMetaFromBoundRoot;

function onBindClick() {
  (async () => {
    try {
      await pickAndBindFolder();
      await runScan();
      document.dispatchEvent(new CustomEvent('wishreview:datasource-updated'));
    } catch (e) {
      if (e?.name === 'AbortError') return;
      console.warn('[wish-review] 绑定失败', e?.message || e);
    }
  })();
}

function bindClicks(selector, handler) {
  document.querySelectorAll(selector).forEach((el) => {
    el.addEventListener('click', handler);
  });
}

function setup() {
  bindClicks('.js-wish-review-bind', onBindClick);
  bindClicks('.js-wish-review-scan', async () => {
    await runScan();
    document.dispatchEvent(new CustomEvent('wishreview:datasource-updated'));
  });

  getBoundDirHandle().then(async (h) => {
    if (h) {
      await runScan();
      document.dispatchEvent(new CustomEvent('wishreview:datasource-updated'));
    }
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', setup);
} else {
  setup();
}
