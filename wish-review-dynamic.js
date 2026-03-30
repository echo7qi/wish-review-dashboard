/* 祈愿单项目复盘 · 动态看板：解析整体数据监测 CSV，按专题展示、搜索、本周/近7日新上线 */

(function () {
  const $ = (id) => document.getElementById(id);

  /**
   * 监测表「当前累计·上线 n 日内」的 n 上限为 30（与业务窗口一致；解析与快照匹配共用）。
   * 各期实际 n = min(30, floor(已上线天数))。
   */
  const MONITOR_SNAP_DAY_MAX = 30;

  function esc(s) {
    if (s == null || s === '') return '';
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function toNum(v) {
    if (v == null || v === '') return null;
    const n = parseFloat(String(v).replace(/,/g, ''));
    return Number.isFinite(n) ? n : null;
  }

  /** 单元格字符串（去首尾空白，避免导出表含不可见空格导致匹配失败） */
  function val(r, key) {
    if (!r) return '';
    const v = r[key];
    if (v == null) return '';
    return String(v).trim();
  }

  /** 单行：去掉表头 BOM、列名首尾空白 */
  function normalizeRowKeys(r) {
    if (!r || typeof r !== 'object') return {};
    const out = {};
    Object.keys(r).forEach((k) => {
      const nk = k.replace(/^\uFEFF/, '').trim();
      out[nk] = r[k];
    });
    return out;
  }

  function isSummaryAllRow(r) {
    return (
      val(r, '数据分类') === '汇总' &&
      val(r, '数据周期') === '汇总' &&
      val(r, '是否目标用户') === '全部'
    );
  }

  /**
   * 大表瘦身：只保留专题列表与复盘用到的行，避免百万行明细把 Chrome 撑爆（错误代码 5 ≈ OOM）。
   * - 汇总行：汇总 / 汇总 / 全部
   * - 快照行：当前累计 + 数据周期「上线n日内」（n 与各行「已上线天数」口径一致，解析时仅做上限防护）
   */
  function isDashboardUsefulRow(r) {
    if (isSummaryAllRow(r)) return true;
    if (val(r, '数据分类') !== '当前累计') return false;
    const dp = String(val(r, '数据周期') || '').trim();
    const m = dp.match(/^上线(\d+)日内$/);
    if (!m) return false;
    const n = parseInt(m[1], 10);
    return n >= 1 && n <= MONITOR_SNAP_DAY_MAX;
  }

  /**
   * 解析监测 CSV：逐行过滤后再入内存（见 isDashboardUsefulRow）。
   * @returns {{ rows: object[], scanned: number, kept: number }}
   */
  function parseMonitoringCsvToAllRows(text, logLabel) {
    const acc = [];
    const errs = [];
    let scanned = 0;
    Papa.parse(text, {
      header: true,
      skipEmptyLines: 'greedy',
      worker: false,
      error(e) {
        errs.push({ type: 'fatal', message: String((e && e.message) || e) });
      },
      step(results) {
        if (results.errors && results.errors.length) {
          for (let i = 0; i < results.errors.length; i++) errs.push(results.errors[i]);
        }
        const raw = results.data;
        if (raw == null || typeof raw !== 'object' || Array.isArray(raw)) return;
        scanned += 1;
        const row = normalizeRowKeys(raw);
        if (!isDashboardUsefulRow(row)) return;
        acc.push(row);
      },
      complete(results) {
        if (results && results.errors && results.errors.length) {
          for (let i = 0; i < results.errors.length; i++) errs.push(results.errors[i]);
        }
      },
    });
    if (errs.length) {
      console.warn('[wish-review-dynamic]', logLabel || 'CSV', errs.slice(0, 10));
    }
    return { rows: acc, scanned, kept: acc.length };
  }

  /** 多文件合并：同一活动+数据分类+周期+用户类型 只保留一行（后读入的文件覆盖先读的，时间序由 wish-review.js 从旧到新） */
  function rowDedupeKey(r) {
    return [
      val(r, '活动标识'),
      val(r, '数据分类'),
      val(r, '数据周期'),
      val(r, '是否目标用户'),
    ].join('\x01');
  }

  function dedupeMonitoringRows(rows) {
    const m = new Map();
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      m.set(rowDedupeKey(r), r);
    }
    return Array.from(m.values());
  }

  /** 分层用户监测 CSV：与生成脚本 get_layer_matched_sorted 口径一致（当前累计 + 上线 n 日内 + 有分层列） */
  function isLayerUsefulRow(r) {
    const aid = val(r, '活动标识');
    if (!aid) return false;
    if (!String(val(r, '目标用户分层') || '').trim()) return false;
    const pk = String(val(r, '第x次祈愿') || '').trim();
    if (!pk) return false;
    if (val(r, '数据分类') !== '当前累计') return false;
    const dp = String(val(r, '数据周期') || '').trim();
    const m = dp.match(/^上线(\d+)日内$/);
    if (!m) return false;
    const n = parseInt(m[1], 10);
    return n >= 1 && n <= MONITOR_SNAP_DAY_MAX;
  }

  function parseLayerCsvToRows(text, logLabel) {
    const acc = [];
    const errs = [];
    let scanned = 0;
    Papa.parse(text, {
      header: true,
      skipEmptyLines: 'greedy',
      worker: false,
      error(e) {
        errs.push({ type: 'fatal', message: String((e && e.message) || e) });
      },
      step(results) {
        if (results.errors && results.errors.length) {
          for (let i = 0; i < results.errors.length; i++) errs.push(results.errors[i]);
        }
        const raw = results.data;
        if (raw == null || typeof raw !== 'object' || Array.isArray(raw)) return;
        scanned += 1;
        const row = normalizeRowKeys(raw);
        if (!isLayerUsefulRow(row)) return;
        acc.push(row);
      },
      complete(results) {
        if (results && results.errors && results.errors.length) {
          for (let i = 0; i < results.errors.length; i++) errs.push(results.errors[i]);
        }
      },
    });
    if (errs.length) {
      console.warn('[wish-review-dynamic] layer', logLabel || 'CSV', errs.slice(0, 10));
    }
    return { rows: acc, scanned, kept: acc.length };
  }

  function layerRowDedupeKey(r) {
    return [
      val(r, '活动标识'),
      String(val(r, '第x次祈愿') || '').trim(),
      val(r, '数据分类'),
      val(r, '数据周期'),
      String(val(r, '目标用户分层') || '').trim(),
    ].join('\x01');
  }

  function dedupeLayerRows(rows) {
    const m = new Map();
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      m.set(layerRowDedupeKey(r), r);
    }
    return Array.from(m.values());
  }

  const LAYER_ORDER = [
    '[200+)',
    '[100-200)',
    '[50-100)',
    '[20-50)',
    '[1-20)',
    '(0-1)',
    '[0]',
    '非目标用户',
  ];

  function layerSortKey(name) {
    const s = String(name || '').trim();
    const ix = LAYER_ORDER.indexOf(s);
    return ix >= 0 ? ix : 500;
  }

  function buildLayerRowIndex(rowsLayer) {
    const idx = new Map();
    for (let i = 0; i < rowsLayer.length; i++) {
      const r = rowsLayer[i];
      const aid = val(r, '活动标识');
      if (!aid) continue;
      const pk = String(val(r, '第x次祈愿') || '').trim();
      const dc = val(r, '数据分类');
      const dr = val(r, '数据周期');
      const key = [aid, pk, dc, dr].join('\x01');
      let arr = idx.get(key);
      if (!arr) {
        arr = [];
        idx.set(key, arr);
      }
      arr.push(r);
    }
    return idx;
  }

  function getLayerMatchedSorted(layerIndex, aid, periodNo, nSnap) {
    if (!layerIndex || !layerIndex.size) return [];
    const dc = `上线${parseInt(String(nSnap), 10) || 1}日内`;
    const pk = String(periodNo).trim();
    const key = [String(aid || '').trim(), pk, '当前累计', dc].join('\x01');
    const matched = layerIndex.get(key);
    if (!matched || !matched.length) return [];
    const out = matched.slice();
    out.sort((a, b) => layerSortKey(val(a, '目标用户分层')) - layerSortKey(val(b, '目标用户分层')));
    return out;
  }

  function layerMetricShare(raw) {
    const x = toNum(raw);
    if (x == null || !Number.isFinite(x)) return null;
    return x > 1.0001 ? x / 100 : x;
  }

  function fmtLayerPctCell(raw) {
    const v = toNum(raw);
    if (v == null || !Number.isFinite(v)) return '—';
    if (v > 1.0001) return `${v.toFixed(1)}%`;
    return `${(v * 100).toFixed(1)}%`;
  }

  function fmtLayerNumCell(raw) {
    const v = toNum(raw);
    if (v == null || !Number.isFinite(v)) return '—';
    return `${v.toFixed(1)}`;
  }

  function buildLayerTableHtml(matched) {
    if (!matched.length) return '';
    const cols = [
      { k: '目标用户分层', kind: 'text' },
      { k: '对应人群收入占比', kind: 'pct' },
      { k: '参与付费率', kind: 'pct' },
      { k: '复抽率', kind: 'pct' },
      { k: '抽到最高等级用户占比', kind: 'pct' },
      { k: '抽到最高等级卡的人均抽数', kind: 'num' },
      { k: '付费抽-抽到最高等级卡人均抽数', kind: 'num' },
      { k: '抽卡用户-人均付费抽数', kind: 'num' },
      { k: '金爱心礼包贡献收入占比', kind: 'pct' },
      { k: '付费祈愿券贡献收入占比', kind: 'pct' },
    ];
    const ths = [
      '目标用户分层',
      '对应人群收入占比',
      '参与付费率',
      '复抽率',
      '顶配用户占比',
      '抽到最高等级卡人均抽数',
      '付费抽·顶配人均抽数',
      '抽卡用户人均付费抽',
      '礼包收入占比',
      '祈愿券收入占比',
    ];
    const head =
      '<thead><tr>' + ths.map((t) => `<th scope="col">${esc(t)}</th>`).join('') + '</tr></thead>';
    const bodyRows = [];
    for (let ri = 0; ri < matched.length; ri++) {
      const r = matched[ri];
      const tds = [];
      for (let ci = 0; ci < cols.length; ci++) {
        const c = cols[ci];
        const raw = val(r, c.k);
        if (c.kind === 'text') {
          tds.push(`<td>${esc(String(raw || '').trim() || '—')}</td>`);
        } else if (c.kind === 'pct') {
          tds.push(`<td>${esc(fmtLayerPctCell(raw))}</td>`);
        } else {
          tds.push(`<td>${esc(fmtLayerNumCell(raw))}</td>`);
        }
      }
      bodyRows.push('<tr>' + tds.join('') + '</tr>');
    }
    return (
      '<div class="layer-table-scroll">' +
      '<table class="layer-table">' +
      head +
      '<tbody>' +
      bodyRows.join('') +
      '</tbody></table></div>'
    );
  }

  /** 分层块顶部条形：与生成脚本条形维度一致，不输出长段结论文案 */
  function buildLayerInsightBarsHtml(matched) {
    if (!matched.length) return '';
    const parsed = [];
    for (let i = 0; i < matched.length; i++) {
      const r = matched[i];
      parsed.push({
        name: String(val(r, '目标用户分层') || '').trim() || '—',
        rs: layerMetricShare(val(r, '对应人群收入占比')),
        jp: layerMetricShare(val(r, '参与付费率')),
        rr: layerMetricShare(val(r, '复抽率')),
        gr: layerMetricShare(val(r, '金爱心礼包贡献收入占比')),
      });
    }
    const validRs = parsed.filter((x) => x.rs != null);
    if (!validRs.length) return '';
    let dom = validRs[0];
    for (let i = 1; i < validRs.length; i++) {
      if (validRs[i].rs > dom.rs) dom = validRs[i];
    }
    let bars = hbarHtml(
      '收入占比最高·' + dom.name.slice(0, 22),
      dom.rs,
      fmtPct(dom.rs),
      '#4f46e5',
    );
    const rHi = parsed.find((x) => {
      const s = x.name;
      return s.startsWith('[200') || s.startsWith('200');
    });
    if (rHi && rHi.gr != null && Number.isFinite(rHi.gr)) {
      bars += hbarHtml('高价层·礼包占该层收入', rHi.gr, fmtPct(rHi.gr), '#0d9488');
    }
    const repeatVals = parsed.map((x) => x.rr).filter((x) => x != null && Number.isFinite(x));
    if (repeatVals.length) {
      let rrMin = repeatVals[0];
      for (let i = 1; i < repeatVals.length; i++) {
        if (repeatVals[i] < rrMin) rrMin = repeatVals[i];
      }
      bars += hbarHtml('各层复抽率·最低档', rrMin, fmtPct(rrMin), '#6366f1');
    }
    return bars ? '<div class="review-layer-insight">' + bars + '</div>' : '';
  }

  function buildLayerModuleHtml(sumRow, layerIndex, nSnap, periodNo) {
    const aid = val(sumRow, '活动标识');
    if (!state.layerRows || !state.layerRows.length) {
      return '<p class="review-mod-note">未读取到「分层用户监测」文件夹内 CSV。</p>';
    }
    const matched = getLayerMatchedSorted(layerIndex, aid, periodNo, nSnap);
    if (!matched.length) {
      return '<p class="review-mod-note">本期未匹配到分层行（核对活动标识、期次与「当前累计·上线' + esc(String(nSnap)) + '日内」）。</p>';
    }
    const insight = buildLayerInsightBarsHtml(matched);
    const table = buildLayerTableHtml(matched);
    return insight + '<div class="review-layer-inner">' + table + '</div>';
  }

  function periodNum(row) {
    const n = parseInt(String(val(row, '第x次祈愿') || '0'), 10);
    return Number.isFinite(n) ? n : 0;
  }

  function parseLaunchDate(raw) {
    if (raw == null) return null;
    const s = String(raw).trim().slice(0, 10);
    if (!s) return null;
    const t = Date.parse(s.replace(/\//g, '-'));
    if (Number.isNaN(t)) return null;
    return t;
  }

  function startOfWeekMondayMs() {
    const d = new Date();
    const day = d.getDay();
    const diff = day === 0 ? -6 : 1 - day;
    d.setDate(d.getDate() + diff);
    d.setHours(0, 0, 0, 0);
    return d.getTime();
  }

  function snapAll(rows, aid, n) {
    const key = `上线${n}日内`;
    const aidNorm = String(aid || '').trim();
    let allRow = null;
    let yesRow = null;
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (val(r, '活动标识') !== aidNorm || val(r, '数据分类') !== '当前累计' || val(r, '数据周期') !== key) {
        continue;
      }
      if (val(r, '是否目标用户') === '全部') allRow = r;
      if (val(r, '是否目标用户') === '是') yesRow = r;
    }
    return { pa: allRow, pyes: yesRow, n };
  }

  function snapRowN(rows, aid, maxDays) {
    const mdRaw = Number(maxDays);
    let md = Math.floor(mdRaw);
    if (!Number.isFinite(mdRaw) || Number.isNaN(md) || md < 1) md = 1;
    const n = Math.min(MONITOR_SNAP_DAY_MAX, md);
    return snapAll(rows, aid, n);
  }

  // ─── 30 日收入预估 + 同品类分位（与 生成_人鱼全期结论表.py 口径对齐）────────────────
  function buildSnapRevCache(rows) {
    const c = Object.create(null);
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (val(r, '数据分类') !== '当前累计' || val(r, '是否目标用户') !== '全部') continue;
      const aid = val(r, '活动标识');
      const per = val(r, '数据周期');
      if (!aid || !per) continue;
      const rev = toNum(val(r, '总收入'));
      c[`${aid}\x01${per}`] = rev != null && Number.isFinite(rev) ? rev : 0;
    }
    return c;
  }

  function snapRevCacheGet(cache, aid, nDays) {
    const n = parseInt(String(nDays), 10);
    if (!Number.isFinite(n) || n < 1) return null;
    const key = `${String(aid || '').trim()}\x01上线${n}日内`;
    const v = cache[key];
    if (v == null || !Number.isFinite(v) || v <= 0) return null;
    return v;
  }

  function buildSummaryMapFiltered(rows, genreFilter, topicFilter) {
    const m = new Map();
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (!isSummaryAllRow(r)) continue;
      if (genreFilter != null && genreFilter !== '' && val(r, '品类') !== genreFilter) continue;
      if (topicFilter != null && topicFilter !== '' && val(r, '专题名称') !== topicFilter) continue;
      const aid = val(r, '活动标识');
      if (aid) m.set(aid, r);
    }
    return m;
  }

  function buildSummaryMap(rows, genreFilter) {
    return buildSummaryMapFiltered(rows, genreFilter, null);
  }

  function medianNum(arr) {
    if (!arr || !arr.length) return null;
    const a = arr.slice().sort((x, y) => x - y);
    const mid = Math.floor(a.length / 2);
    return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
  }

  function ratioClipVal(r) {
    return Math.max(0.35, Math.min(8, r));
  }

  function blendMultiplier30d(fishRatios, catRatios) {
    const mf = medianNum(fishRatios);
    const mc = medianNum(catRatios);
    if (mf != null && mc != null) return 0.55 * mf + 0.45 * mc;
    if (mf != null) return mf;
    if (mc != null) return mc;
    return 30 / 9;
  }

  function collectR30OverR9Ratios(cache, summaryMap) {
    const out = [];
    summaryMap.forEach((summ, aid) => {
      const d = Math.floor(toNum(val(summ, '已上线天数')) || 0);
      if (d < 30) return;
      const r9 = snapRevCacheGet(cache, aid, 9);
      const r30 = snapRevCacheGet(cache, aid, 30);
      if (r9 == null || r30 == null || r9 <= 0) return;
      const ratio = r30 / r9;
      if (ratio >= 0.35 && ratio <= 8) out.push(ratio);
    });
    return out;
  }

  function loocvBlendK(bt, idx, catR30R9) {
    const ratios = [];
    for (let j = 0; j < bt.length; j++) {
      if (j === idx) continue;
      ratios.push(ratioClipVal(bt[j].r30 / bt[j].r9));
    }
    return blendMultiplier30d(ratios, catR30R9);
  }

  function mapeMdapePair(preds, trues) {
    const apes = [];
    for (let i = 0; i < preds.length; i++) {
      const p = preds[i];
      const t = trues[i];
      if (t > 0 && p != null && p > 0) apes.push((Math.abs(p - t) / t) * 100);
    }
    if (!apes.length) return null;
    const mean = apes.reduce((a, b) => a + b, 0) / apes.length;
    const s = apes.slice().sort((x, y) => x - y);
    const md = s[Math.floor(s.length / 2)];
    return { mean, md };
  }

  function buildFishBacktestRows(cacheMain, summaryMap, rows) {
    const out = [];
    summaryMap.forEach((summ, aid) => {
      const d = Math.floor(toNum(val(summ, '已上线天数')) || 0);
      if (d < 30) return;
      const r9 = snapRevCacheGet(cacheMain, aid, 9);
      const r30 = snapRevCacheGet(cacheMain, aid, 30);
      if (r9 == null || r30 == null || r9 <= 0 || r30 <= 0) return;
      const sn = snapAll(rows, aid, 9);
      if (!sn.pa) return;
      const pa9 = sn.pa;
      const j = rate01(val(pa9, '参与付费率'));
      const th = rate01(val(pa9, '目标触达率'));
      out.push({
        aid,
        r9,
        r30,
        days: d,
        tgt_hit: th != null ? th : 0,
        t_arpu: toNum(val(pa9, '触达ARPU')) || 0,
        tgt_arpu: toNum(val(pa9, '目标付费ARPU')) || 0,
        join_pay: j != null ? j : 0,
        tgt_uv: toNum(val(pa9, '目标用户数')) || 0,
      });
    });
    return out;
  }

  function solveLinearSystem(A, b) {
    const n = b.length;
    const M = [];
    for (let i = 0; i < n; i++) M.push(A[i].concat(b[i]));
    for (let col = 0; col < n; col++) {
      let pivot = col;
      for (let r = col + 1; r < n; r++) {
        if (Math.abs(M[r][col]) > Math.abs(M[pivot][col])) pivot = r;
      }
      if (Math.abs(M[pivot][col]) < 1e-12) return null;
      if (pivot !== col) {
        const t = M[col];
        M[col] = M[pivot];
        M[pivot] = t;
      }
      const div = M[col][col];
      for (let j = col; j <= n; j++) M[col][j] /= div;
      for (let r = 0; r < n; r++) {
        if (r === col) continue;
        const f = M[r][col];
        if (f === 0) continue;
        for (let j = col; j <= n; j++) M[r][j] -= f * M[col][j];
      }
    }
    return M.map((row) => row[n]);
  }

  function lstsqOlsBeta(bt, keysFeat) {
    const m = bt.length;
    const p = 1 + keysFeat.length;
    const Xa = [];
    const yv = [];
    for (let i = 0; i < m; i++) {
      const row = bt[i];
      yv.push(row.r30 / row.r9);
      const x = [1];
      for (let k = 0; k < keysFeat.length; k++) x.push(row[keysFeat[k]]);
      Xa.push(x);
    }
    const XtX = [];
    const Xty = [];
    for (let i = 0; i < p; i++) {
      XtX[i] = [];
      for (let j = 0; j < p; j++) {
        let s = 0;
        for (let k = 0; k < m; k++) s += Xa[k][i] * Xa[k][j];
        XtX[i][j] = s;
      }
      let sy = 0;
      for (let k = 0; k < m; k++) sy += Xa[k][i] * yv[k];
      Xty[i] = sy;
    }
    return solveLinearSystem(XtX, Xty);
  }

  function run30dModelBacktest(bt, catR30R9) {
    if (!bt.length) return null;
    const trues = bt.map((r) => r.r30);
    const n = bt.length;
    const rows = {};

    const predsLin = bt.map((r) => r.r9 * (30 / 9));
    rows.linear_30_9 = {
      label: '线性外推 30/9（无业务特征）',
      preds: predsLin,
      mape: mapeMdapePair(predsLin, trues),
    };

    const kCat = medianNum(catR30R9) || 30 / 9;
    const predsCat = bt.map((r) => r.r9 * kCat);
    rows.cat_median_k = {
      label: '对标池 R30/R9 中位数 k（全局常数）',
      preds: predsCat,
      mape: mapeMdapePair(predsCat, trues),
    };

    const predsFish = [];
    for (let i = 0; i < n; i++) {
      const ratios = [];
      for (let j = 0; j < n; j++) {
        if (j === i) continue;
        ratios.push(ratioClipVal(bt[j].r30 / bt[j].r9));
      }
      const kf = medianNum(ratios) || kCat;
      predsFish.push(bt[i].r9 * kf);
    }
    rows.fish_loocv_median = {
      label: '监测同品类往期留一法 R30/R9 中位 k',
      preds: predsFish,
      mape: mapeMdapePair(predsFish, trues),
    };

    const predsBlend = [];
    for (let i = 0; i < n; i++) {
      predsBlend.push(bt[i].r9 * loocvBlendK(bt, i, catR30R9));
    }
    rows.blend_loocv = {
      label: '往期留一 + 对标池混合 k（0.55/0.45）',
      preds: predsBlend,
      mape: mapeMdapePair(predsBlend, trues),
    };

    const predsArpu = [];
    for (let i = 0; i < n; i++) {
      const k = loocvBlendK(bt, i, catR30R9);
      const others = [];
      for (let j = 0; j < n; j++) {
        if (j !== i && bt[j].t_arpu > 0) others.push(bt[j].t_arpu);
      }
      const medT = medianNum(others) || bt[i].t_arpu || 1;
      let adj = medT > 0 ? bt[i].t_arpu / medT : 1;
      adj = Math.max(0.65, Math.min(1.35, adj));
      predsArpu.push(bt[i].r9 * k * adj);
    }
    rows.arpu_tuned_loocv = {
      label: '混合 k × 触达 ARPU 相对留一中位校正',
      preds: predsArpu,
      mape: mapeMdapePair(predsArpu, trues),
    };

    const predsJoin = [];
    for (let i = 0; i < n; i++) {
      const k = loocvBlendK(bt, i, catR30R9);
      const others = [];
      for (let j = 0; j < n; j++) {
        if (j !== i && bt[j].join_pay > 0) others.push(bt[j].join_pay);
      }
      const medJ = medianNum(others) || bt[i].join_pay || 0.2;
      let adj = medJ > 0 ? bt[i].join_pay / medJ : 1;
      adj = Math.max(0.65, Math.min(1.35, adj));
      predsJoin.push(bt[i].r9 * k * adj);
    }
    rows.join_tuned_loocv = {
      label: '混合 k × 参与付费率相对留一中位校正',
      preds: predsJoin,
      mape: mapeMdapePair(predsJoin, trues),
    };

    const keysFeat = ['tgt_hit', 't_arpu', 'tgt_arpu', 'join_pay'];
    const predsOls = [];
    if (n >= 7) {
      for (let i = 0; i < n; i++) {
        const train = bt.filter((_, j) => j !== i);
        if (train.length < 6) {
          predsOls.push(bt[i].r9 * loocvBlendK(bt, i, catR30R9));
          continue;
        }
        const beta = lstsqOlsBeta(train, keysFeat);
        if (!beta) {
          predsOls.push(bt[i].r9 * loocvBlendK(bt, i, catR30R9));
          continue;
        }
        const xi = [
          1,
          bt[i].tgt_hit,
          bt[i].t_arpu,
          bt[i].tgt_arpu,
          bt[i].join_pay,
        ];
        let rr = 0;
        for (let t = 0; t < xi.length; t++) rr += xi[t] * beta[t];
        predsOls.push(bt[i].r9 * ratioClipVal(rr));
      }
    }
    rows.ols_9d_feats = {
      label: 'OLS：目标触达率+触达ARPU+目标付费ARPU+参与付费率→R30/R9',
      preds: n >= 7 && predsOls.length === n ? predsOls : null,
      mape:
        n >= 7 && predsOls.length === n ? mapeMdapePair(predsOls, trues) : null,
    };

    let bestKey = null;
    let bestMape = Infinity;
    Object.keys(rows).forEach((key) => {
      const mm = rows[key].mape;
      if (!mm || mm.mean == null) return;
      if (mm.mean < bestMape) {
        bestMape = mm.mean;
        bestKey = key;
      }
    });
    if (!bestKey) bestKey = 'blend_loocv';
    return { bt, rows, winner: bestKey, trues };
  }

  function paFeatsFromRow(pa) {
    if (!pa) return { tgt_hit: 0, t_arpu: 0, tgt_arpu: 0, join_pay: 0 };
    const th = rate01(val(pa, '目标触达率'));
    const j = rate01(val(pa, '参与付费率'));
    return {
      tgt_hit: th != null ? th : 0,
      t_arpu: toNum(val(pa, '触达ARPU')) || 0,
      tgt_arpu: toNum(val(pa, '目标付费ARPU')) || 0,
      join_pay: j != null ? j : 0,
    };
  }

  function buildProd30dPredictor(winnerKey, bt, catR30R9, fishR30R9) {
    const kGlobal = blendMultiplier30d(fishR30R9, catR30R9);
    const kFish = medianNum(fishR30R9) || kGlobal;
    const kCat = medianNum(catR30R9) || kGlobal;
    let medT = medianNum(bt.map((r) => r.t_arpu).filter((x) => x > 0)) || 1;
    let medJ = medianNum(bt.map((r) => r.join_pay).filter((x) => x > 0)) || 0.2;
    if (medT <= 0) medT = 1;
    if (medJ <= 0) medJ = 0.2;

    const keysFeat = ['tgt_hit', 't_arpu', 'tgt_arpu', 'join_pay'];
    let betaProd = null;
    let rankProdOk = false;
    if (winnerKey === 'ols_9d_feats' && bt && bt.length >= 6) {
      betaProd = lstsqOlsBeta(bt, keysFeat);
      rankProdOk = !!betaProd;
    }

    return function pred30(rev9, pa) {
      if (!rev9 || rev9 <= 0) return null;
      const f = paFeatsFromRow(pa);
      if (winnerKey === 'linear_30_9') return rev9 * (30 / 9);
      if (winnerKey === 'cat_median_k') return rev9 * kCat;
      if (winnerKey === 'fish_loocv_median') return rev9 * kFish;
      if (winnerKey === 'blend_loocv') return rev9 * kGlobal;
      if (winnerKey === 'arpu_tuned_loocv') {
        const adj = Math.max(
          0.65,
          Math.min(1.35, medT > 0 ? f.t_arpu / medT : 1),
        );
        return rev9 * kGlobal * adj;
      }
      if (winnerKey === 'join_tuned_loocv') {
        const adj = Math.max(
          0.65,
          Math.min(1.35, medJ > 0 ? f.join_pay / medJ : 1),
        );
        return rev9 * kGlobal * adj;
      }
      if (winnerKey === 'ols_9d_feats' && rankProdOk && betaProd) {
        const xi = [1, f.tgt_hit, f.t_arpu, f.tgt_arpu, f.join_pay];
        let rr = 0;
        for (let t = 0; t < xi.length; t++) rr += xi[t] * betaProd[t];
        return rev9 * ratioClipVal(rr);
      }
      return rev9 * kGlobal;
    };
  }

  function format30dRevenueKpi(revBase, pred30v) {
    if (pred30v == null || pred30v <= 0 || !revBase || revBase <= 0) return null;
    const estI = Math.round(pred30v);
    if (estI <= 0) return null;
    return Math.round(estI).toLocaleString('zh-CN');
  }

  function getOrBuildModelPack(genreRaw, topicRaw) {
    const g =
      genreRaw && String(genreRaw).trim() && String(genreRaw).trim() !== '—'
        ? String(genreRaw).trim()
        : '';
    const t =
      topicRaw && String(topicRaw).trim() ? String(topicRaw).trim() : '';
    const key = `${g || '__G__'}\x01${t || '__T__'}`;
    if (state.modelPackByGenre.has(key)) return state.modelPackByGenre.get(key);

    const cacheMain = state.snapCacheMain;
    const cacheBench = buildSnapRevCache(state.benchRows);
    const summaryMainG = buildSummaryMapFiltered(
      state.rows,
      g !== '' ? g : null,
      t !== '' ? t : null,
    );
    const summaryBenchG = buildSummaryMap(state.benchRows, g !== '' ? g : null);
    const fishR30R9 = collectR30OverR9Ratios(cacheMain, summaryMainG);
    const catR30R9 = collectR30OverR9Ratios(cacheBench, summaryBenchG);
    const bt = buildFishBacktestRows(cacheMain, summaryMainG, state.rows);
    const calibration = bt.length ? run30dModelBacktest(bt, catR30R9) : null;
    const winKey = calibration ? calibration.winner : 'blend_loocv';
    const predFn = buildProd30dPredictor(winKey, bt, catR30R9, fishR30R9);
    const pack = {
      calibration,
      predFn,
      winKey,
      fishR30R9,
      catR30R9,
      genreKey: key,
    };
    state.modelPackByGenre.set(key, pack);
    return pack;
  }

  function percentileRankSameGenre(currentRev, genreRaw) {
    if (currentRev == null || !Number.isFinite(currentRev) || currentRev <= 0) return null;
    const g =
      genreRaw && String(genreRaw).trim() && String(genreRaw).trim() !== '—'
        ? String(genreRaw).trim()
        : '';
    const sm = buildSummaryMap(state.rows, g || null);
    const sb = buildSummaryMap(state.benchRows, g || null);
    const all = new Map([...sm.entries(), ...sb.entries()]);
    const vals = [];
    all.forEach((summ, aid) => {
      const dRaw = Math.floor(toNum(val(summ, '已上线天数')) || 0);
      if (dRaw < 1) return;
      const d = Math.min(MONITOR_SNAP_DAY_MAX, dRaw);
      const r = snapRevCacheGet(state.snapCacheMerged, aid, d);
      if (r != null && r > 0 && Number.isFinite(r)) vals.push(r);
    });
    if (!vals.length) return null;
    vals.sort((a, b) => a - b);
    let lo = 0;
    let hi = vals.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (vals[mid] < currentRev) lo = mid + 1;
      else hi = mid;
    }
    return (100 * lo) / vals.length;
  }

  /**
   * 与脚本一致：只认「汇总 / 汇总 / 全部」汇总行；同一专题下同一活动标识只保留一行（取期次更大者），
   * 避免导出重复行把「期数」和「专题数」撑大。
   */
  function buildTopicModels(rows) {
    const summaries = rows.filter(isSummaryAllRow);
    const rawSummaryCount = summaries.length;

    /** 专题名称 -> 活动标识 -> 行 */
    const byTopic = new Map();
    for (let i = 0; i < summaries.length; i++) {
      const r = summaries[i];
      const name = val(r, '专题名称');
      if (!name) continue;
      const aid = val(r, '活动标识');
      if (!aid) continue;
      if (!byTopic.has(name)) byTopic.set(name, new Map());
      const m = byTopic.get(name);
      const prev = m.get(aid);
      if (!prev || periodNum(r) >= periodNum(prev)) {
        m.set(aid, r);
      }
    }

    const topics = [];
    const weekStart = startOfWeekMondayMs();
    const now = Date.now();
    const sevenAgo = now - 7 * 86400000;

    let activityDedupCount = 0;
    byTopic.forEach((aidMap, name) => {
      const arr = Array.from(aidMap.values());
      activityDedupCount += arr.length;
      arr.sort((a, b) => periodNum(b) - periodNum(a));
      const latest = arr[0];
      const launchMs = parseLaunchDate(val(latest, '上线日期'));
      const inThisWeek = launchMs != null && launchMs >= weekStart;
      const in7d = launchMs != null && launchMs >= sevenAgo;
      topics.push({
        name,
        periods: arr,
        latest,
        launchMs,
        inThisWeek,
        in7d,
      });
    });

    topics.sort((a, b) => (b.launchMs || 0) - (a.launchMs || 0));
    return {
      topics,
      rawSummaryCount,
      activityDedupCount,
      topicCount: topics.length,
    };
  }

  let state = {
    rows: [],
    benchRows: [],
    mergedRows: [],
    snapCacheMain: Object.create(null),
    snapCacheMerged: Object.create(null),
    modelPackByGenre: new Map(),
    layerRows: [],
    layerIndex: new Map(),
    workRows: [],
    topics: [],
    fileName: '',
    selected: null,
    filter: '',
    // 右侧详情默认只挂载最新一期 DOM；历史期次在 <details> 内首次展开时再生成。
    // Auto refresh: compare monitor CSV latest mtime to decide reload.
    lastMainMonitorLastModified: null,
  };

  // ─── Auto refresh (weekly data update friendly) ─────────────────────────────
  let autoRefreshTimer = null;
  let autoRefreshing = false;
  const AUTO_REFRESH_MS = 24 * 60 * 60 * 1000; // 每天检查一次 mtime；你每周更新数据即可生效

  function ensureAutoRefresh() {
    if (autoRefreshTimer) return;
    const scanFn = window.wishReviewScanMonitorCsvMeta;
    if (typeof scanFn !== 'function') return;

    autoRefreshTimer = setInterval(async () => {
      if (autoRefreshing) return;
      autoRefreshing = true;
      try {
        const meta = await scanFn();
        if (!meta || !meta.ok) return;
        const last = state.lastMainMonitorLastModified;
        if (meta.lastModified && last && meta.lastModified !== last) {
          await loadFromBinding();
        }
      } catch (_) {
        // ignore
      } finally {
        autoRefreshing = false;
      }
    }, AUTO_REFRESH_MS);
  }

  function fmtPct(x) {
    if (x == null) return '—';
    return `${(x * 100).toFixed(1)}%`;
  }

  function fmtInt(x) {
    if (x == null) return '—';
    return Math.round(x).toLocaleString('zh-CN');
  }

  /** 0–1 比例；表中若为百分数则换算 */
  function rate01(v) {
    if (v == null || v === '') return null;
    const x = parseFloat(String(v).replace(/,/g, ''));
    if (!Number.isFinite(x)) return null;
    return x > 1.0001 ? x / 100 : x;
  }

  function hbarHtml(label, frac, displayText, color) {
    if (frac == null || !Number.isFinite(frac)) return '';
    const w = Math.max(2, Math.min(100, frac * 100));
    return (
      '<div class="rv-hbar-row">' +
      `<span class="rv-hbar-lab">${esc(label)}</span>` +
      '<div class="rv-hbar-track">' +
      `<div class="rv-hbar-fill" style="width:${w.toFixed(1)}%;background:${esc(color)}"></div>` +
      '</div>' +
      `<span class="rv-hbar-val">${esc(displayText)}</span>` +
      '</div>'
    );
  }

  function poolStackHtml(giftR, ticketR) {
    const g = Math.max(0, Math.min(1, giftR));
    const tk = Math.max(0, Math.min(1, ticketR));
    let rest = 1 - g - tk;
    if (rest < 0) rest = 0;
    return (
      '<div class="rv-stack-bar" title="礼包 / 祈愿券 / 其余收入占比">' +
      `<span class="rv-stack-s rv-stack-g" style="width:${(g * 100).toFixed(2)}%">礼</span>` +
      `<span class="rv-stack-s rv-stack-t" style="width:${(tk * 100).toFixed(2)}%">券</span>` +
      `<span class="rv-stack-s rv-stack-o" style="width:${(rest * 100).toFixed(2)}%">其</span>` +
      '</div>' +
      `<p class="rv-stack-cap">收入结构 · 礼包 ${(g * 100).toFixed(0)}% · 祈愿券 ${(tk * 100).toFixed(
        0,
      )}% · 其余 ${(rest * 100).toFixed(0)}%</p>`
    );
  }

  /** 预估30日 = n 日累计 × 30/n，进度 = 累计/预估（线性外推） */
  function linearEst30Revenue(revN, nDays) {
    if (revN == null || !Number.isFinite(revN) || revN < 0) {
      return { est: null, progressPct: null };
    }
    const n = parseInt(String(nDays), 10);
    if (!Number.isFinite(n) || n < 1) return { est: null, progressPct: null };
    const est = (revN * 30) / n;
    if (!Number.isFinite(est) || est <= 0) return { est: null, progressPct: null };
    const progressPct = Math.min(100, Math.round((revN / est) * 100));
    return { est, progressPct };
  }

  function oneLinerText(pa, nSnap) {
    if (!pa) {
      return (
        '未匹配到「当前累计·上线' +
        esc(String(nSnap)) +
        '日内·全部」快照；请核对监测表导出列名与活动标识是否一致。'
      );
    }
    const j = rate01(val(pa, '参与付费率'));
    const t = rate01(val(pa, '目标触达率'));
    const bits = [];
    if (j != null) {
      bits.push(
        j < 0.12
          ? '参与付费率偏低'
          : j < 0.18
            ? '参与付费率中等略偏低'
            : '参与付费率相对正常',
      );
    }
    if (t != null) bits.push('目标触达率约 ' + (t * 100).toFixed(1) + '%');
    if (!bits.length) return '—';
    return bits.join('；') + '。';
  }

  /** 综合判断：基于当前快照可计算的指标生成要点，不依赖外部 HTML。 */
  function buildSynthesisModuleHtml(pa, pyes, synthCtx) {
    if (!pa) {
      return (
        '<ol class="review-conclusions">' +
        '<li>当前无「当前累计」快照行，无法生成综合判断；请检查监测表字段与活动标识。</li>' +
        '</ol>'
      );
    }
    const linearEst = synthCtx.linearEst;
    const scriptProgressPct = synthCtx.scriptProgressPct;
    const catPr = synthCtx.catPr;

    const items = [];
    const join = rate01(val(pa, '参与付费率'));
    const tgt = rate01(val(pa, '目标触达率'));
    let freeShare = null;
    const fd = toNum(val(pa, '免费抽卡用户数'));
    const du = toNum(val(pa, '抽卡用户数'));
    if (fd != null && du != null && du > 0) freeShare = fd / du;
    const tgtUserShare = pyes ? rate01(val(pyes, '对应人群收入占比')) : null;
    if (join != null) {
      if (join < 0.12) {
        items.push('参与付费率偏低，可结合礼包/券结构（①）与触达（②）排查转化断点。');
      } else if (join < 0.18) {
        items.push('参与付费率中等略偏低，关注免费抽占比与进付费抽引导。');
      } else {
        items.push('参与付费率在监测快照中处于相对正常区间，可结合收入目标达成度持续观察。');
      }
    }
    if (tgt != null) {
      items.push('目标触达率约 ' + (tgt * 100).toFixed(1) + '%，可对照宣发与渠道效率（②）。');
    }
    if (freeShare != null && freeShare > 0.55 && join != null && join < 0.18) {
      items.push('纯免费抽占比较高且付费转化一般时，建议复盘赠抽与付费入口设计。');
    }
    if (tgtUserShare != null) {
      items.push('目标用户收入占比约 ' + (tgtUserShare * 100).toFixed(1) + '%（「是否目标用户=是」行）。');
    }
    const progShow =
      synthCtx.usedScript30 && scriptProgressPct != null
        ? scriptProgressPct
        : linearEst.progressPct;
    if (progShow != null && progShow >= 90) {
      items.push(
        '预估完成进度约 ' + progShow + '%，可结合「收入目标达成度」评估收尾节奏。',
      );
    }
    if (catPr != null) {
      items.push('同期收入约超同品类 ' + catPr.toFixed(0) + '% 样本。');
    }
    return (
      '<ol class="review-conclusions">' +
      items.map((li) => '<li>' + esc(li) + '</li>').join('') +
      '</ol>'
    );
  }

  function buildPeriodBoardArticle(sumRow) {
    const pNo = periodNum(sumRow);
    const aid = val(sumRow, '活动标识');
    const daysRaw = toNum(val(sumRow, '已上线天数'));
    const daysInt = daysRaw != null ? Math.floor(daysRaw) : 1;
    const snap = snapRowN(state.rows, aid, daysInt);
    const pa = snap.pa;
    const pyes = snap.pyes;
    const n = snap.n;
    const genre = val(sumRow, '品类') || '—';
    const theme = val(sumRow, '活动名称【修正】') || val(sumRow, '活动名称') || '—';
    const daysOnline = val(sumRow, '已上线天数') || '—';

    const join = pa ? rate01(val(pa, '参与付费率')) : null;
    let freeShare = null;
    if (pa) {
      const fd = toNum(val(pa, '免费抽卡用户数'));
      const du = toNum(val(pa, '抽卡用户数'));
      if (fd != null && du != null && du > 0) freeShare = fd / du;
    }
    const repeatR = pa ? rate01(val(pa, '复抽率')) : null;

    const giftR = pa ? rate01(val(pa, '金爱心礼包贡献收入占比')) : null;
    const ticketR = pa ? rate01(val(pa, '付费祈愿券贡献收入占比')) : null;
    const poolBlock =
      giftR != null && ticketR != null
        ? poolStackHtml(giftR, ticketR)
        : '<p class="review-mod-note">缺少金爱心礼包/付费祈愿券收入占比列时无法绘制堆叠条。</p>';

    const tgt = pa ? rate01(val(pa, '目标触达率')) : null;
    const tdr = pa ? rate01(val(pa, '触达抽卡率')) : null;
    const tuv = pa ? toNum(val(pa, '触达用户数')) : null;
    const tarpu = pa ? toNum(val(pa, '触达ARPU')) : null;
    let launchRows = '';
    if (tgt != null) launchRows += hbarHtml('目标触达率', tgt, fmtPct(tgt), '#0891b2');
    if (tdr != null) launchRows += hbarHtml('触达抽卡率', tdr, fmtPct(tdr), '#0e7490');
    if (tuv != null || tarpu != null) {
      launchRows +=
        '<div class="rv-metric-line"><span>触达 UV</span><strong>' +
        fmtInt(tuv) +
        '</strong><span>触达 ARPU</span><strong>' +
        (tarpu != null ? tarpu.toFixed(3) : '—') +
        '</strong></div>';
    }
    if (!launchRows) {
      launchRows = '<p class="review-mod-note">缺少目标触达率/触达 UV 等列。</p>';
    }

    const rev = pa ? toNum(val(pa, '总收入')) : null;
    const paidUv = pa ? toNum(val(pa, '付费抽卡用户数')) : null;
    const ppd = pa ? toNum(val(pa, '付费抽用户-人均付费抽数')) : null;
    const arppu = pa ? toNum(val(pa, '触达付费ARPPU')) : null;
    const goalCell = val(pa, '目标达成度');
    const goalHtml = goalCell !== '' ? esc(goalCell) : '—';

    const nModel = Math.min(9, Math.max(1, daysInt));
    const snapM = snapRowN(state.rows, aid, nModel);
    const revModelBase =
      snapM.pa && toNum(val(snapM.pa, '总收入')) != null
        ? toNum(val(snapM.pa, '总收入'))
        : rev;
    const paModel = snapM.pa || pa;
    const genreForModel = genre === '—' ? '' : genre;
    const topicForModel = val(sumRow, '专题名称') || '';
    const pack = getOrBuildModelPack(genreForModel, topicForModel);
    let pred30v = null;
    if (
      revModelBase != null &&
      revModelBase > 0 &&
      paModel &&
      typeof pack.predFn === 'function'
    ) {
      pred30v = pack.predFn(revModelBase, paModel);
    }
    const linearEst = linearEst30Revenue(rev, n);
    let est30Strong = '';
    let usedScript30 = false;
    let scriptProgressPct = null;
    const formattedScript = format30dRevenueKpi(revModelBase, pred30v);
    if (formattedScript) {
      est30Strong = formattedScript;
      usedScript30 = true;
      if (pred30v != null && pred30v > 0) {
        let dProg = parseInt(String(daysInt), 10);
        if (!Number.isFinite(dProg)) dProg = 1;
        const kp = Math.min(30, Math.max(1, dProg));
        let rWin = snapRevCacheGet(state.snapCacheMain, aid, kp);
        if (rWin == null) rWin = revModelBase;
        scriptProgressPct = Math.round((rWin / pred30v) * 100);
      }
    } else if (linearEst.est != null) {
      est30Strong = fmtInt(linearEst.est);
    } else {
      est30Strong = '—';
    }

    const catPr =
      rev != null && Number.isFinite(rev) && rev > 0
        ? percentileRankSameGenre(rev, genre)
        : null;

    const kpiBlock = pa
      ? `<div class="period-kpis">
          <div class="kpi-pill"><span class="kpi-l">${n}日累计收入</span><strong>${fmtInt(rev)}</strong></div>
          <div class="kpi-pill kpi-pill-goal"><span class="kpi-l">预估30日收入</span><strong>${est30Strong}</strong></div>
          <div class="kpi-pill kpi-pill-goal"><span class="kpi-l">收入目标达成度</span><strong>${goalHtml}</strong></div>
          <div class="kpi-pill"><span class="kpi-l">参与付费率</span><strong>${join != null ? fmtPct(join) : '—'}</strong></div>
          <div class="kpi-pill"><span class="kpi-l">付费抽卡人数</span><strong>${fmtInt(paidUv)}</strong></div>
          <div class="kpi-pill"><span class="kpi-l">付费抽人均抽数</span><strong>${ppd != null ? ppd.toFixed(2) : '—'}</strong></div>
          <div class="kpi-pill"><span class="kpi-l">付费ARPPU</span><strong>${arppu != null ? arppu.toFixed(2) : '—'}</strong></div>
        </div>`
      : '<p class="review-mod-note">无当前累计快照，顶栏 KPI 略。</p>';

    const pps = pa ? toNum(val(pa, '付费单抽均价')) : null;
    const topShare = pa ? rate01(val(pa, '抽到最高等级用户占比')) : null;
    const tgtUserShare = pyes ? rate01(val(pyes, '对应人群收入占比')) : null;

    const poolOl = '';
    const launchOl = '';

    const miniGrid =
      '<div class="period-grid">' +
      '<section class="mini-card" aria-label="收入规模">' +
      '<h4 class="mini-card-title">收入规模</h4>' +
      '' +
      '<div class="mini-stats"><div class="data-line">' +
      esc(String(n)) +
      '日累计收入 <strong>' +
      fmtInt(rev) +
      '</strong></div></div>' +
      '' +
      '</section>' +
      '<section class="mini-card" aria-label="触达效率">' +
      '<h4 class="mini-card-title">触达效率</h4>' +
      '' +
      '<div class="mini-stats"><div class="data-line">触达 UV <strong>' +
      fmtInt(tuv) +
      '</strong>｜触达 ARPU <strong>' +
      (tarpu != null ? tarpu.toFixed(3) : '—') +
      '</strong>｜目标触达率 <strong>' +
      (tgt != null ? fmtPct(tgt) : '—') +
      '</strong></div></div>' +
      '' +
      '</section>' +
      '<section class="mini-card" aria-label="客单价">' +
      '<h4 class="mini-card-title">客单价</h4>' +
      '' +
      '<div class="mini-stats"><div class="data-line">付费单抽均价 <strong>' +
      (pps != null ? pps.toFixed(2) : '—') +
      '</strong>｜参与付费率 <strong>' +
      (join != null ? fmtPct(join) : '—') +
      '</strong>｜纯免费抽占抽卡用户 <strong>' +
      (freeShare != null ? fmtPct(freeShare) : '—') +
      '</strong>｜复抽率 <strong>' +
      (repeatR != null ? fmtPct(repeatR) : '—') +
      '</strong>｜顶配用户占比 <strong>' +
      (topShare != null ? fmtPct(topShare) : '—') +
      '</strong></div></div>' +
      '' +
      '</section>' +
      '<section class="mini-card" aria-label="结构">' +
      '<h4 class="mini-card-title">结构</h4>' +
      '' +
      '<div class="mini-stats"><div class="data-line">目标用户收入占比 <strong>' +
      (tgtUserShare != null ? fmtPct(tgtUserShare) : '—') +
      '</strong>｜礼包 <strong>' +
      (giftR != null ? fmtPct(giftR) : '—') +
      '</strong>｜祈愿券 <strong>' +
      (ticketR != null ? fmtPct(ticketR) : '—') +
      '</strong></div></div>' +
      '' +
      '</section>' +
      '<section class="mini-card" aria-label="收入节奏">' +
      '<h4 class="mini-card-title">收入节奏</h4>' +
      '' +
      '<div class="mini-stats"><div class="data-line">同期' +
      esc(String(n)) +
      '日累计收入 <strong>' +
      fmtInt(rev) +
      '</strong></div>' +
      (pred30v != null && pred30v > 0
        ? '<div class="data-line">预估 30 日累计 <strong>' +
          fmtInt(Math.round(pred30v)) +
          '</strong></div>'
        : '') +
      (linearEst.est != null && !(pred30v != null && pred30v > 0)
        ? '<div class="data-line">线性预估 30 日累计 <strong>' +
          fmtInt(linearEst.est) +
          '</strong>｜进度 <strong>' +
          esc(String(linearEst.progressPct)) +
          '%</strong></div>'
        : '') +
      '</div>' +
      '' +
      '</section>' +
      '<section class="mini-card" aria-label="同品类表现">' +
      '<h4 class="mini-card-title">同品类表现</h4>' +
      '' +
      '<div class="mini-stats"><div class="data-line">同期' +
      esc(String(n)) +
      '日收入分位 <strong>' +
      (catPr != null ? '约超 ' + catPr.toFixed(0) + '%' : '—') +
      '</strong>｜参与付费率 <strong>' +
      (join != null ? fmtPct(join) : '—') +
      '</strong></div></div>' +
      '' +
      '</section>' +
      '</div>';

    return (
      `<article class="period-board" data-period="${esc(String(pNo))}">` +
      '<header class="period-head">' +
      '<div class="period-head-text">' +
      `<span class="period-badge">第 ${esc(String(pNo))} 期</span>` +
      `<span class="period-theme">${esc(theme)}</span>` +
      `<span class="period-sub">${esc(genre)} · 已上线 ${esc(daysOnline)} 天 · 表内对比 ${esc(String(n))} 日</span>` +
      '</div>' +
      '<p class="period-one-liner"><span class="one-liner-label">一句话总结</span>' +
      oneLinerText(pa, n) +
      '</p>' +
      kpiBlock +
      '</header>' +
      '<div class="review-modules">' +
      buildUserReachFunnelsModuleHtml(sumRow) +
      '<div class="review-mod-grid">' +
      '<section class="review-mod">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">①</span> 抽池策略与收入结构</h3>' +
      poolBlock +
      poolOl +
      '</section>' +
      '<section class="review-mod">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">②</span> 宣发与触达</h3>' +
      launchRows +
      launchOl +
      '</section>' +
      '</div>' +
      '<section class="review-mod review-mod--layer">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">③</span> 付费分层表现</h3>' +
      buildLayerModuleHtml(sumRow, state.layerIndex, n, pNo) +
      '</section>' +
      '<section class="review-mod review-mod--synthesis">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">④</span> 综合判断（跨维度）</h3>' +
      buildSynthesisModuleHtml(pa, pyes, {
        linearEst,
        usedScript30,
        scriptProgressPct,
        catPr,
      }) +
      '</section>' +
      '</div>' +
      '<details class="period-metrics-fold">' +
      '<summary class="period-metrics-fold-sum">展开 / 收起 ⑤ 六格指标明细</summary>' +
      '' +
      miniGrid +
      '</details>' +
      '</article>'
    );
  }

  function valFuzzy(r, keys) {
    for (let i = 0; i < keys.length; i++) {
      const v = val(r, keys[i]);
      if (v != null && v !== '') return v;
    }
    return '';
  }

  /**
   * 漏斗单行：左标签 + 右数值；自第二行起「占上一级」转化率；全宽轨道 + 相对「目标用户数」比例的渐变填充（参考设计稿条形漏斗）。
   */
  function buildFunnelStepHtml(stepIndex, label, value, prevValue, topForBar) {
    const vNum =
      value == null || !Number.isFinite(value) ? null : Math.max(0, value);
    const valText = vNum == null ? '—' : fmtInt(vNum);
    const top = topForBar != null && topForBar > 0 ? topForBar : 0;
    const fillPct = vNum == null || top <= 0 ? 0 : Math.min(100, (vNum / top) * 100);

    let convRow = '';
    if (stepIndex > 0) {
      let convStr = '—';
      if (
        vNum != null &&
        prevValue != null &&
        Number.isFinite(prevValue) &&
        prevValue > 0
      ) {
        convStr = `${((vNum / prevValue) * 100).toFixed(1)}%`;
      }
      convRow = `<div class="rv-funnelStep__conv">占上一级 ${esc(convStr)}</div>`;
    }

    return (
      `<div class="rv-funnelStep${stepIndex === 0 ? ' rv-funnelStep--first' : ''}">` +
      `<div class="rv-funnelStep__head">` +
      `<span class="rv-funnelStep__label">${esc(label)}</span>` +
      `<span class="rv-funnelStep__val">${esc(valText)}</span>` +
      `</div>` +
      convRow +
      `<div class="rv-funnelStep__barTrack" role="presentation">` +
      `<div class="rv-funnelStep__barFill" style="width:${fillPct.toFixed(1)}%"></div>` +
      `</div>` +
      `</div>`
    );
  }

  function resolveIsTargetUserCategoryValue(isTargetUserValues, categoryLabel) {
    const normalized = String(categoryLabel || '').trim();
    const candidates = [];
    if (normalized.includes('全部')) candidates.push('全部', '全部用户');
    if (normalized.includes('目标阅读')) candidates.push('目标阅读用户', '目标阅读', '阅读用户', '阅读');
    if (normalized.includes('目标IP付费')) candidates.push('目标IP付费用户', 'IP付费用户', 'IP付费', '付费IP');
    if (normalized.includes('非目标阅读')) candidates.push('非目标阅读用户', '非目标阅读', '非阅读用户', '非阅读');

    // 1) exact
    for (let i = 0; i < candidates.length; i++) {
      const c = candidates[i];
      const exact = isTargetUserValues.find((v) => String(v).trim() === c);
      if (exact) return exact;
    }
    // 2) substring heuristics
    for (let i = 0; i < candidates.length; i++) {
      const c = candidates[i];
      const hit = isTargetUserValues.find((v) => String(v).includes(c));
      if (hit) return hit;
    }
    // 3) broad by keywords
    const keywords = [];
    if (normalized.includes('全部')) keywords.push('全部');
    if (normalized.includes('阅读')) keywords.push('阅读');
    if (normalized.includes('IP')) keywords.push('IP');
    if (normalized.includes('付费')) keywords.push('付费');
    if (normalized.includes('非')) keywords.push('非');
    const hit2 = isTargetUserValues.find((v) => keywords.every((k) => String(v).includes(k)) && keywords.length);
    return hit2 || '';
  }

  function clearFunnelRowCache() {
    if (buildUserReachFunnelsModuleHtml._cache) {
      buildUserReachFunnelsModuleHtml._cache.clear();
    }
  }

  /**
   * 基于四张人群漏斗快照，与「全部用户」卡对比生成简短结论。
   * @param {{ label: string, v: { target: number|null, reach: number|null, draw: number|null, pay: number|null } }[]} segmentData
   */
  function buildFunnelCrossSegmentInsightHtml(segmentData) {
    const pick = (lab) => {
      for (let i = 0; i < segmentData.length; i++) {
        if (segmentData[i].label === lab) return segmentData[i].v;
      }
      return null;
    };
    const vAll = pick('全部用户');
    const vNt = pick('非目标阅读用户');
    const vRead = pick('目标阅读用户');
    const vIp = pick('目标IP付费用户');

    const ratioPayReach = (v) =>
      v && v.reach != null && v.reach > 0 && v.pay != null && v.pay >= 0 ? v.pay / v.reach : null;
    const ratioDrawReach = (v) =>
      v && v.reach != null && v.reach > 0 && v.draw != null && v.draw >= 0 ? v.draw / v.reach : null;

    const lines = [];
    const payAll = vAll && vAll.pay != null && Number.isFinite(vAll.pay) ? vAll.pay : null;

    if (payAll != null && payAll > 0 && vNt && vNt.pay != null && Number.isFinite(vNt.pay) && vNt.pay >= 0) {
      const pct = (vNt.pay / payAll) * 100;
      if (pct >= 52) {
        lines.push(
          '非目标阅读用户侧付费抽卡用户约占全量付费抽卡用户的 ' +
            pct.toFixed(0) +
            '%，泛曝光或非核心人群在付费人数上贡献明显，可结合投放 ROI 评估触达范围与转化策略。',
        );
      } else if (pct <= 28) {
        lines.push(
          '非目标阅读用户仅占全量付费抽卡用户约 ' +
            pct.toFixed(0) +
            '%，付费人数更依赖核心人群承接。',
        );
      }
    }

    const prAll = ratioPayReach(vAll);
    const prRead = ratioPayReach(vRead);
    if (prAll != null && prAll > 0 && prRead != null) {
      if (prRead < prAll * 0.75) {
        lines.push(
          '目标阅读用户「触达→付费抽」转化率低于全量水平，建议核对人群与素材/活动匹配度，并检查触达后的付费引导链路。',
        );
      } else if (prRead > prAll * 1.1) {
        lines.push('目标阅读用户触达后付费转化优于全量基准，核心阅读盘承接效率较好。');
      }
    }

    const prIp = ratioPayReach(vIp);
    if (prAll != null && prAll > 0 && prIp != null) {
      if (prIp < prAll * 0.75) {
        lines.push(
          '目标 IP 付费用户触达后付费转化偏弱，可从 IP 吸引力、池子与定价权益侧复盘。',
        );
      } else if (prIp > prAll * 1.1) {
        lines.push('目标 IP 付费用户触达后付费转化强于大盘，高意愿人群链路相对顺畅。');
      }
    }

    const drAll = ratioDrawReach(vAll);
    const drRead = ratioDrawReach(vRead);
    if (drAll != null && drAll >= 0.05 && drRead != null) {
      if (drRead < drAll * 0.82) {
        lines.push(
          '目标阅读用户「触达→抽卡」转化低于全量，可先排查触达内容、活动感知与进入抽卡页的引导是否到位。',
        );
      }
    }

    if (!lines.length) {
      lines.push(
        '漏斗已按同一快照窗口对齐；可将各人群「触达→抽卡→付费抽」逐级与「全部用户」卡对照，定位掉队环节并核对监测字段是否完整。',
      );
    }

    const body = lines
      .slice(0, 4)
      .map((s) => '<li>' + esc(s) + '</li>')
      .join('');
    return (
      '<div class="rv-funnelModule__insight" role="note">' +
      '<div class="rv-funnelModule__insight-title">触达转化综合结论</div>' +
      '<ul class="rv-funnelModule__insight-list">' +
      body +
      '</ul></div>'
    );
  }

  function buildUserReachFunnelsModuleHtml(sumRow) {
    const aid = val(sumRow, '活动标识');
    const daysRaw = toNum(val(sumRow, '已上线天数'));
    const daysInt = daysRaw != null ? Math.floor(daysRaw) : 1;
    const snap = snapRowN(state.rows, aid, daysInt);
    const n = snap.n;
    const periodKey = `上线${n}日内`;

    // Cache snapshot rows by (aid|periodKey)
    if (!buildUserReachFunnelsModuleHtml._cache) buildUserReachFunnelsModuleHtml._cache = new Map();
    const cKey = `${aid}||${periodKey}`;
    let snapRows = buildUserReachFunnelsModuleHtml._cache.get(cKey);
    if (!snapRows) {
      snapRows = [];
      for (let i = 0; i < state.rows.length; i++) {
        const r = state.rows[i];
        if (val(r, '活动标识') !== aid) continue;
        if (val(r, '数据分类') !== '当前累计') continue;
        if (val(r, '数据周期') !== periodKey) continue;
        snapRows.push(r);
      }
      buildUserReachFunnelsModuleHtml._cache.set(cKey, snapRows);
    }

    function targetUserLabelFromRow(r) {
      return String(
        valFuzzy(r, ['是否目标用户', '是否为目标用户']) || '',
      ).trim();
    }

    const isTargetValues = Array.from(
      new Set(snapRows.map((r) => targetUserLabelFromRow(r)).filter(Boolean)),
    );

    const pickRowByCategory = (categoryLabel) => {
      const resolved = resolveIsTargetUserCategoryValue(isTargetValues, categoryLabel);
      if (!resolved) return null;
      for (let i = 0; i < snapRows.length; i++) {
        if (targetUserLabelFromRow(snapRows[i]) === resolved) return snapRows[i];
      }
      return null;
    };

    /** 「全部用户」漏斗：触达等指标须与监测表「是否目标用户=全部」的当前累计快照行一致，避免模糊匹配到其它人群行 */
    function pickAllUsersFunnelRow() {
      for (let wi = 0; wi < 2; wi++) {
        const want = wi === 0 ? '全部' : '全部用户';
        for (let i = 0; i < snapRows.length; i++) {
          if (targetUserLabelFromRow(snapRows[i]) === want) return snapRows[i];
        }
      }
      return pickRowByCategory('全部用户');
    }

    const targets = [
      { label: '全部用户' },
      { label: '目标阅读用户' },
      { label: '目标IP付费用户' },
      { label: '非目标阅读用户' },
    ];

    function stepValuesFromRow(row) {
      if (!row) return { target: null, reach: null, draw: null, pay: null };
      const target = toNum(valFuzzy(row, ['目标用户数', '目标用户']));
      const reach = toNum(valFuzzy(row, ['触达用户数', '触达UV']));
      const draw = toNum(valFuzzy(row, ['抽卡用户数', '抽卡用户']));
      const pay = toNum(valFuzzy(row, ['付费抽卡用户数', '付费抽卡用户']));
      return { target, reach, draw, pay };
    }

    const stepDefs = [
      { label: '目标用户数', key: 'target' },
      { label: '触达用户数', key: 'reach' },
      { label: '抽卡用户数', key: 'draw' },
      { label: '付费抽卡用户数', key: 'pay' },
    ];

    const segmentData = targets.map((t) => {
      const r =
        String(t.label || '').trim() === '全部用户'
          ? pickAllUsersFunnelRow()
          : pickRowByCategory(t.label);
      return { label: t.label, v: stepValuesFromRow(r) };
    });

    const funnelCardsHtml = segmentData
      .map(({ label, v }) => {
        const topForBar =
          v.target != null && v.target > 0
            ? v.target
            : Math.max(v.target || 0, v.reach || 0, v.draw || 0, v.pay || 0);
        const vals = stepDefs.map((s) => v[s.key]);
        let stepsInner = '';
        for (let si = 0; si < stepDefs.length; si++) {
          const prevVal = si === 0 ? null : vals[si - 1];
          stepsInner += buildFunnelStepHtml(si, stepDefs[si].label, vals[si], prevVal, topForBar);
        }
        return (
          `<div class="rv-funnelCard">` +
          `<div class="rv-funnelCard__title">${esc(label)}</div>` +
          `<div class="rv-funnel">` +
          stepsInner +
          `</div>` +
          `</div>`
        );
      })
      .join('\n');

    const caliberHtml =
      '<p class="rv-funnelModule__caliber">' +
      esc(
        '口径：数据分类「当前累计」，数据周期「' +
          periodKey +
          '」；四卡对应监测表「是否目标用户」（兼容列名「是否为目标用户」）下「全部 / 目标阅读 / 目标IP付费 / 非目标阅读」各行，与下图漏斗同快照。',
      ) +
      '</p>';

    const insightHtml = buildFunnelCrossSegmentInsightHtml(segmentData);

    return (
      `<section class="review-mod rv-funnelModule">` +
      `<h3 class="review-mod-title"><span class="review-mod-badge">1</span>用户触达</h3>` +
      insightHtml +
      `<div class="rv-funnel-grid">${funnelCardsHtml}</div>` +
      caliberHtml +
      `</section>`
    );
  }

  function buildFishEmbedReportHtml(t) {
    const periods = t.periods || [];
    if (!periods.length) {
      return '<p class="review-mod-note">该专题无汇总期次数据。</p>';
    }
    const latestBoard = buildPeriodBoardArticle(periods[0]);
    let pastBlock = '';
    if (periods.length > 1) {
      const nPast = periods.length - 1;
      pastBlock =
        '<details class="wish-past-periods fish-past-periods-details">' +
        '<summary class="wish-past-periods__sum">历史期次（' +
        esc(String(nPast)) +
        '）</summary>' +
        '<div class="fish-period-stack wish-past-periods__host" data-topic="' +
        esc(t.name) +
        '"></div>' +
        '</details>';
    }
    return (
      '<div class="wishReviewFishRoot fish-report-embedded">' +
      '<div class="fish-period-stack">' +
      latestBoard +
      '</div>' +
      pastBlock +
      '</div>'
    );
  }

  function renderDetail(topicName) {
    const host = $('wishReviewDetailInner');
    if (!host) return;
    clearFunnelRowCache();

    if (!topicName) {
      host.className = 'card__body';
      host.innerHTML =
        '<p class="muted wishReviewDash__empty">绑定后在左侧选择专题；右侧将按监测 CSV 在线计算并生成复盘内容。</p>';
      return;
    }

    const t = state.topics.find((x) => x.name === topicName);
    if (!t) {
      host.className = 'card__body';
      host.innerHTML = '<p class="muted wishReviewDash__empty">未找到该专题</p>';
      return;
    }

    host.className = 'card__body wishReviewDetailInner--fish';
    host.innerHTML = buildFishEmbedReportHtml(t);
  }

  function topicMatchesFilter(t, q) {
    if (!q) return true;
    return t.name.toLowerCase().includes(q);
  }

  function renderList() {
    const q = state.filter.trim().toLowerCase();
    const filtered = state.topics.filter((t) => topicMatchesFilter(t, q));

    if (
      state.selected &&
      !filtered.some((x) => x.name === state.selected)
    ) {
      state.selected = filtered[0] ? filtered[0].name : null;
      renderDetail(state.selected);
    }

    const weekList = filtered.filter((t) => t.inThisWeek);
    const recent7 = filtered.filter((t) => t.in7d && !t.inThisWeek);
    const rest = filtered.filter((t) => !t.inThisWeek && !t.in7d);

    function itemRow(t, badge) {
      const active = state.selected === t.name ? ' is-active' : '';
      const b = badge
        ? `<span class="wishReviewDash__badge">${esc(badge)}</span>`
        : '';
      const ld = t.launchMs
        ? new Date(t.launchMs).toLocaleDateString('zh-CN')
        : '—';
      return `<button type="button" class="wishReviewDash__topicBtn${active}" data-topic="${esc(t.name)}">
        <span class="wishReviewDash__topicName">${esc(t.name)}${b}</span>
        <span class="wishReviewDash__topicSub">第 ${periodNum(t.latest)} 期 · ${esc(ld)}</span>
      </button>`;
    }

    const elWeek = $('wishReviewWeekList');
    const el7 = $('wishReview7dList');
    const elAll = $('wishReviewAllList');
    if (elWeek) {
      elWeek.innerHTML = weekList.length
        ? weekList.map((t) => itemRow(t, '本周')).join('')
        : '<p class="muted wishReviewDash__empty">暂无</p>';
    }
    if (el7) {
      el7.innerHTML = recent7.length
        ? recent7.map((t) => itemRow(t, '近7日')).join('')
        : '<p class="muted wishReviewDash__empty">暂无（已在「本周」列出除外）</p>';
    }
    if (elAll) {
      const show = [...weekList, ...recent7, ...rest];
      elAll.innerHTML = show.length
        ? show.map((t) => itemRow(t, '')).join('')
        : '<p class="muted wishReviewDash__empty">无匹配专题</p>';
    }
  }

  function setSelected(name) {
    state.selected = name;
    renderList();
    renderDetail(name);
  }

  function ingestCsvParts(parts, labelPrefix) {
    let merged = [];
    let scanned = 0;
    let kept = 0;
    if (!parts || !parts.length) return { merged, scanned, kept };
    for (let pi = 0; pi < parts.length; pi++) {
      const one = parseMonitoringCsvToAllRows(parts[pi].text, parts[pi].name || labelPrefix);
      scanned += one.scanned;
      kept += one.kept;
      merged = merged.concat(one.rows);
    }
    return { merged, scanned, kept };
  }

  function ingestLayerCsvParts(parts, labelPrefix) {
    let merged = [];
    let scanned = 0;
    let kept = 0;
    if (!parts || !parts.length) return { merged, scanned, kept };
    for (let pi = 0; pi < parts.length; pi++) {
      const one = parseLayerCsvToRows(parts[pi].text, parts[pi].name || labelPrefix);
      scanned += one.scanned;
      kept += one.kept;
      merged = merged.concat(one.rows);
    }
    return { merged, scanned, kept };
  }

  async function loadFromBinding() {
    const status = $('wishReviewDashStatus');
    const src = $('wishReviewDashSource');
    const readBundle = window.wishReviewReadMonitorAndBenchCsv;
    const readMainOnly = window.wishReviewReadMonitorCsv;
    const readFn = typeof readBundle === 'function' ? readBundle : readMainOnly;
    if (!readFn) {
      if (status) {
        status.hidden = false;
        status.textContent = '脚本未就绪。';
      }
      return;
    }
    if (status) {
      status.hidden = false;
      status.textContent = '加载中…';
    }
    if (src) {
      src.hidden = false;
      src.textContent = '';
    }
    const raw = await readFn();
    if (!raw.ok) {
      if (status) {
        status.hidden = false;
        status.textContent = raw.error || '读取失败';
      }
      if (src) {
        src.hidden = true;
        src.textContent = '';
      }
      state.rows = [];
      state.benchRows = [];
      state.mergedRows = [];
      state.snapCacheMain = Object.create(null);
      state.snapCacheMerged = Object.create(null);
      state.modelPackByGenre.clear();
      state.layerRows = [];
      state.layerIndex = new Map();
      state.workRows = [];
      state.topics = [];
      if (typeof window !== 'undefined') window.__WISH_REVIEW_BUNDLE_DATA__ = null;
      renderList();
      renderDetail(null);
      return;
    }

    const isBundle = raw.main && raw.bench;
    const mainBlock = isBundle ? raw.main : raw;
    const parts = mainBlock.parts && mainBlock.parts.length ? mainBlock.parts : null;

    let mergedRows = [];
    let csvScannedTotal = 0;
    let csvKeptBeforeDedupe = 0;
    let benchMerged = [];
    let benchScanned = 0;
    let benchKept = 0;
    let layerMerged = [];

    try {
      if (parts) {
        const ing = ingestCsvParts(parts, '监测');
        mergedRows = ing.merged;
        csvScannedTotal = ing.scanned;
        csvKeptBeforeDedupe = ing.kept;
      } else if (mainBlock.text) {
        const one = parseMonitoringCsvToAllRows(
          mainBlock.text,
          mainBlock.fileName || '监测表',
        );
        csvScannedTotal = one.scanned;
        csvKeptBeforeDedupe = one.kept;
        mergedRows = one.rows;
      } else {
        if (status) {
          status.hidden = false;
          status.textContent = '读取结果缺少 CSV 内容，请刷新页面后重试。';
        }
        return;
      }

      if (isBundle && raw.bench && raw.bench.parts && raw.bench.parts.length) {
        const ingB = ingestCsvParts(raw.bench.parts, '对标池');
        benchMerged = ingB.merged;
        benchScanned = ingB.scanned;
        benchKept = ingB.kept;
        csvScannedTotal += benchScanned;
        csvKeptBeforeDedupe += benchKept;
      }
      if (isBundle && raw.layer && raw.layer.parts && raw.layer.parts.length) {
        const ingL = ingestLayerCsvParts(raw.layer.parts, '分层');
        layerMerged = ingL.merged;
        csvScannedTotal += ingL.scanned;
        csvKeptBeforeDedupe += ingL.kept;
      }
    } catch (e) {
      if (status) {
        status.hidden = false;
        status.textContent = 'CSV 解析失败';
      }
      return;
    }

    clearFunnelRowCache();
    state.modelPackByGenre.clear();
    state.rows = dedupeMonitoringRows(mergedRows);
    state.benchRows = dedupeMonitoringRows(benchMerged);
    state.mergedRows = dedupeMonitoringRows(mergedRows.concat(benchMerged));
    state.snapCacheMain = buildSnapRevCache(state.rows);
    state.snapCacheMerged = buildSnapRevCache(state.mergedRows);
    state.layerRows = dedupeLayerRows(layerMerged);
    state.layerIndex = buildLayerRowIndex(state.layerRows);
    state.workRows = [];

    const lastMod = isBundle ? raw.lastModified || mainBlock.lastModified : mainBlock.lastModified;

    if (typeof window !== 'undefined') {
      window.__WISH_REVIEW_BUNDLE_DATA__ = {
        mainRowCount: state.rows.length,
        benchRowCount: state.benchRows.length,
        mergedRowCount: state.mergedRows.length,
        csvScannedRows: csvScannedTotal,
        csvKeptBeforeDedupeRows: csvKeptBeforeDedupe,
        layerRowCount: state.layerRows.length,
        workRowCount: 0,
        loadedAt: Date.now(),
        note: '',
      };
    }
    const built = buildTopicModels(state.rows);
    state.topics = built.topics;
    state._meta = built;
    state.fileName = mainBlock.fileName;
    state.fileNames =
      mainBlock.fileNames ||
      (parts ? parts.map((p) => p.name) : mainBlock.fileName ? [mainBlock.fileName] : []);
    if (status) {
      status.textContent = '';
      status.hidden = true;
    }
    if (src) {
      src.textContent = '';
      src.hidden = true;
    }

    // Cache mtime for auto refresh.
    state.lastMainMonitorLastModified = lastMod || null;
    ensureAutoRefresh();

    if (!state.selected || !state.topics.some((x) => x.name === state.selected)) {
      state.selected = state.topics[0] ? state.topics[0].name : null;
    }
    renderList();
    renderDetail(state.selected);
  }

  function init() {
    const search = $('wishReviewTopicSearch');
    const reload = $('wishReviewReloadBtn');
    if (search) {
      search.addEventListener('input', () => {
        state.filter = search.value;
        renderList();
      });
    }
    if (reload) {
      reload.addEventListener('click', () => loadFromBinding());
    }
    const lists = ['wishReviewWeekList', 'wishReview7dList', 'wishReviewAllList'];
    lists.forEach((id) => {
      const el = $(id);
      if (!el) return;
      el.addEventListener('click', (ev) => {
        const btn = ev.target.closest('[data-topic]');
        if (!btn) return;
        setSelected(btn.getAttribute('data-topic'));
      });
    });

    document.addEventListener('wishreview:datasource-updated', () => loadFromBinding());

    const mainScroll = document.querySelector('.reviewEmbedMain--dynamic');
    if (mainScroll) {
      mainScroll.addEventListener('toggle', (ev) => {
        const det = ev.target;
        if (!det || !det.classList || !det.classList.contains('wish-past-periods')) return;
        if (!det.open) return;
        const host = det.querySelector('.wish-past-periods__host');
        if (!host || host.dataset.filled === '1') return;
        const topicName = host.getAttribute('data-topic');
        const topic = state.topics.find((x) => x.name === topicName);
        if (!topic || !topic.periods || topic.periods.length < 2) {
          host.innerHTML = '<p class="review-mod-note">无更多期次数据。</p>';
          host.dataset.filled = '1';
          return;
        }
        const past = topic.periods.slice(1);
        host.innerHTML = past.map((sumRow) => buildPeriodBoardArticle(sumRow)).join('');
        host.dataset.filled = '1';
      });
    }

    loadFromBinding();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
