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

  /** 监测表「大盘/全量」行：是否目标用户=全部/全部用户，或目标用户类型=整体 */
  function aggregateTargetUserLabel(r) {
    return String(
      val(r, '是否目标用户') ||
        val(r, '是否为目标用户') ||
        val(r, '目标用户类型') ||
        '',
    ).trim();
  }

  function isAggregateTargetUserRow(r) {
    const u = aggregateTargetUserLabel(r);
    return u === '全部' || u === '全部用户' || u === '整体';
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
    if (val(r, '数据分类') !== '汇总') return false;
    if (val(r, '数据周期') !== '汇总') return false;
    // 兼容：部分汇总表不含「是否目标用户/目标用户类型」列，默认按「全部」处理
    const u1 = String(val(r, '是否目标用户') || '').trim();
    const u2 = String(val(r, '是否为目标用户') || '').trim();
    const u3 = String(val(r, '目标用户类型') || '').trim();
    if (!u1 && !u2 && !u3) return true;
    return isAggregateTargetUserRow(r);
  }

  function extractOnlineDaysFromPeriod(periodRaw) {
    const dp = String(periodRaw || '').trim();
    if (!dp) return null;
    const m = dp.match(/上线\s*(\d+)\s*日/);
    if (!m) return null;
    const n = parseInt(m[1], 10);
    if (!Number.isFinite(n) || n < 1) return null;
    return n;
  }

  function elapsedDaysFromLaunch(sumRow) {
    const lk =
      val(sumRow, '上线日期') || val(sumRow, '上线时间') || val(sumRow, '活动上线时间') || '';
    const ts = parseLaunchDate(lk);
    if (ts == null) return null;
    const now = Date.now();
    if (now <= ts) return 1;
    const d = Math.floor((now - ts) / 86400000);
    return Math.max(1, d);
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
    const n = extractOnlineDaysFromPeriod(dp);
    if (n != null) return n >= 1 && n <= MONITOR_SNAP_DAY_MAX;
    // 顶栏「累计触达用户数」依赖「当前累计·上线至今类·全量」行；此前仅保留上线 n 日内会导致该行从未入内存
    if (isLaunchToDateDataPeriod(dp) && isAggregateTargetUserRow(r)) return true;
    return false;
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
      String(
        val(r, '是否目标用户') ||
          val(r, '是否为目标用户') ||
          val(r, '目标用户类型') ||
          '',
      ).trim(),
    ].join('\x01');
  }

  function dedupeMonitoringRows(rows) {
    const m = new Map();
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const k = rowDedupeKey(r);
      const prev = m.get(k);
      if (!prev) { m.set(k, r); continue; }
      // 同 key 保留上线天数更大的行（即数据更新的那条）
      const dPrev = toNum(valFuzzy(prev, ['上线天数', '已上线天数'])) || 0;
      const dCur = toNum(valFuzzy(r, ['上线天数', '已上线天数'])) || 0;
      if (dCur >= dPrev) m.set(k, r);
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

  function dedupeTargetSplitRows(rows) {
    const m = new Map();
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const typeKey = String(
        valFuzzy(r, ['目标用户类型', '用户类型', '人群类型', '是否目标用户']) || '',
      ).trim();
      const k = [
        val(r, '活动标识'),
        val(r, '专题名称'),
        String(val(r, '第x次祈愿') || '').trim(),
        typeKey,
      ].join('\x01');
      m.set(k, r);
    }
    return Array.from(m.values());
  }

  function normalizeDateKey(raw) {
    const t = parseLaunchDate(raw);
    if (t == null) return '';
    return new Date(t).toISOString().slice(0, 10);
  }

  function normalizePrimarySourceName(raw) {
    const s = String(raw || '').trim();
    if (!s) return '';
    const compact = s.replace(/[\s_\-—－]+/g, '').toLowerCase();
    if (compact.includes('祈愿bar') || compact.includes('祈愿条')) return '祈愿bar';
    if (compact.includes('漫画页') || compact.includes('漫画详情页')) return '漫画页';
    if (compact.includes('卡片战斗') || compact.includes('卡牌战斗')) return '卡片战斗';
    if (compact.includes('任务')) return '任务';
    if (
      compact.includes('广告资源投放') || compact.includes('广气资源投放') ||
      compact === '广告资源' || compact === '广气资源' ||
      compact.includes('v2资源投放') || compact === 'v2投放' || compact === 'v2资源'
    ) return '运营资源投放';
    return s;
  }

  const ACCESS_SOURCE_WHITELIST = ['祈愿bar', '漫画页', '运营资源投放', '任务', '卡片战斗'];

  function dedupeAccessRows(rows) {
    const seen = new Set();
    const out = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (!r || typeof r !== 'object') continue;
      const k = JSON.stringify(r);
      if (seen.has(k)) continue;
      seen.add(k);
      out.push(r);
    }
    return out;
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
    const nTagged = parseInt(String((row && row.__periodNo) || '0'), 10);
    if (Number.isFinite(nTagged) && nTagged > 0) return nTagged;
    const n = parseInt(String(val(row, '第x次祈愿') || '0'), 10);
    return Number.isFinite(n) ? n : 0;
  }

  function parseTopicPeriodFromActivityName(raw) {
    const s = String(raw || '').trim();
    if (!s) return { topicKey: '', periodKey: '' };
    const parts = s
      .split(/[\.。．]/)
      .map((x) => String(x || '').trim())
      .filter(Boolean);
    return {
      topicKey: parts.length ? parts[0] : '',
      periodKey: parts.length > 1 ? parts[1] : '',
    };
  }

  function periodStartDateKey(row) {
    return (
      normalizeDateKey(
        valFuzzy(row, ['开始日期', '活动开始日期', '开始时间', '上线日期', '上线时间', '活动上线时间']),
      ) || ''
    );
  }

  // 同一期次存在多条汇总行时，优先选择「已上线天数」更大的记录（避免被早期2天数据覆盖8天数据）
  function summaryRowPriority(row) {
    const p = periodNum(row);
    const dRaw = toNum(val(row, '已上线天数'));
    const d = dRaw != null && Number.isFinite(dRaw) ? Math.floor(dRaw) : 0;
    return { p, d };
  }

  function shouldReplaceSummary(prev, next) {
    if (!prev) return true;
    const a = summaryRowPriority(prev);
    const b = summaryRowPriority(next);
    if (b.p !== a.p) return b.p > a.p;
    if (b.d !== a.d) return b.d > a.d;
    return false;
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
    const aidNorm = String(aid || '').trim();
    let allRow = null;
    let yesRow = null;
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (val(r, '活动标识') !== aidNorm || val(r, '数据分类') !== '当前累计') {
        continue;
      }
      const dn = extractOnlineDaysFromPeriod(val(r, '数据周期'));
      if (dn == null || dn !== n) continue;
      if (isAggregateTargetUserRow(r)) allRow = r;
      if (aggregateTargetUserLabel(r) === '是') yesRow = r;
    }
    return { pa: allRow, pyes: yesRow, n };
  }

  function snapRowN(rows, aid, maxDays) {
    const mdRaw = Number(maxDays);
    let md = Math.floor(mdRaw);
    if (!Number.isFinite(mdRaw) || Number.isNaN(md) || md < 1) md = 1;
    let n = Math.min(MONITOR_SNAP_DAY_MAX, md);
    let snap = snapAll(rows, aid, n);
    if (snap.pa || snap.pyes) return snap;

    // 若目标 n 不存在，回退到该活动可用的最大上线 n 日快照（避免显示全空）
    const avail = inferAvailableMaxSnapDay(rows, aid);
    if (avail != null && avail >= 1) {
      n = Math.min(n, avail);
      snap = snapAll(rows, aid, n);
      if (snap.pa || snap.pyes) return snap;
      // 再向下逐级回退，直到命中
      for (let d = n - 1; d >= 1; d--) {
        snap = snapAll(rows, aid, d);
        if (snap.pa || snap.pyes) return snap;
      }
    }
    return { pa: null, pyes: null, n };
  }

  function inferOnlineDaysFromRows(rows, aid) {
    const aidNorm = String(aid || '').trim();
    if (!aidNorm || !rows || !rows.length) return null;
    let best = null;
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (val(r, '活动标识') !== aidNorm) continue;
      const cat = String(val(r, '数据分类') || '').trim();
      if (!(cat === '当前累计' || cat.includes('当前累计'))) continue;
      const per = String(val(r, '数据周期') || '').trim();
      // 口径放宽：只要出现“上线 + 数字 + 日”即可（例如：上线8日内、上线 8 日、上线8日内（自然日））
      const m = per.match(/上线\s*(\d+)\s*日/);
      if (!m) continue;
      const d = parseInt(m[1], 10);
      if (!Number.isFinite(d) || d < 1) continue;
      if (best == null || d > best) best = d;
    }
    return best;
  }

  function inferOnlineDaysByTopic(rows, sumRow) {
    if (!rows || !rows.length || !sumRow) return null;
    const topic = String(val(sumRow, '专题名称') || '').trim();
    const actName = String(
      val(sumRow, '活动名称【修正】') || val(sumRow, '活动名称') || '',
    ).trim();
    const curKeys = parseActivityNameKeys(actName);
    const curPeriod = String(curKeys.periodKey || val(sumRow, '第x次祈愿') || '').trim();
    if (!topic && !actName) return null;
    let best = null;
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const cat = String(val(r, '数据分类') || '').trim();
      if (!(cat === '当前累计' || cat.includes('当前累计'))) continue;
      const per = String(val(r, '数据周期') || '').trim();
      const n = extractOnlineDaysFromPeriod(per);
      if (n == null) continue;
      const rTopic = String(val(r, '专题名称') || '').trim();
      const rAct = String(val(r, '活动名称【修正】') || val(r, '活动名称') || '').trim();
      const rKeys = parseActivityNameKeys(rAct);
      const rPeriod = String(rKeys.periodKey || val(r, '第x次祈愿') || '').trim();
      const sameTopic = topic && rTopic && rTopic === topic;
      const sameAct = actName && rAct && rAct === actName;
      if (!(sameTopic || sameAct)) continue;
      if (curPeriod && rPeriod && curPeriod !== rPeriod) continue;
      if (best == null || n > best) best = n;
    }
    return best;
  }

  function inferAvailableMaxSnapDay(rows, aid) {
    const aidNorm = String(aid || '').trim();
    if (!aidNorm || !rows || !rows.length) return null;
    let best = null;
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (val(r, '活动标识') !== aidNorm) continue;
      if (val(r, '数据分类') !== '当前累计') continue;
      const n = extractOnlineDaysFromPeriod(val(r, '数据周期'));
      if (n == null) continue;
      if (best == null || n > best) best = n;
    }
    return best;
  }

  function buildInferredOnlineDaysMap(rows) {
    const m = new Map();
    if (!rows || !rows.length) return m;
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (!r || typeof r !== 'object') continue;
      const cat = String(val(r, '数据分类') || '').trim();
      if (!(cat === '当前累计' || cat.includes('当前累计'))) continue;
      const aid = String(val(r, '活动标识') || '').trim();
      if (!aid) continue;
      const per = String(val(r, '数据周期') || '').trim();
      const mm = per.match(/上线\s*(\d+)\s*日/);
      if (!mm) continue;
      const d = parseInt(mm[1], 10);
      if (!Number.isFinite(d) || d < 1) continue;
      const prev = m.get(aid);
      if (prev == null || d > prev) m.set(aid, d);
    }
    return m;
  }

  function reconcileSummaryOnlineDays(rows) {
    // 按活动标识取「当前累计」行中最大上线天数
    // 来源1: 已上线天数/上线天数 字段
    // 来源2: 数据周期「上线x日内」中的 x
    const maxDaysMap = new Map();
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const cat = String(val(r, '数据分类') || '').trim();
      if (cat !== '当前累计' && !cat.includes('当前累计')) continue;
      const aid = String(val(r, '活动标识') || '').trim();
      if (!aid) continue;
      let d = toNum(valFuzzy(r, ['已上线天数', '上线天数']));
      if (d == null || !Number.isFinite(d) || d < 1) {
        d = extractOnlineDaysFromPeriod(val(r, '数据周期'));
      }
      if (d == null || d < 1) continue;
      const prev = maxDaysMap.get(aid);
      if (prev == null || d > prev) maxDaysMap.set(aid, d);
    }
    if (!maxDaysMap.size) return;
    // 回填汇总行
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (!isSummaryAllRow(r)) continue;
      const aid = String(val(r, '活动标识') || '').trim();
      if (!aid || !maxDaysMap.has(aid)) continue;
      const best = maxDaysMap.get(aid);
      const cur = toNum(val(r, '已上线天数'));
      if (cur == null || !Number.isFinite(cur) || best > cur) {
        r['已上线天数'] = String(Math.floor(best));
      }
    }
  }

  function buildOnlineDaysIndex(rows) {
    const byAidLaunch = new Map();
    const byNameLaunch = new Map();
    const byAid = new Map();
    const byName = new Map();
    if (!rows || !rows.length) return { byAidLaunch, byNameLaunch, byAid, byName };
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const cat = String(val(r, '数据分类') || '').trim();
      if (cat !== '当前累计' && !cat.includes('当前累计')) continue;
      if (!isAggregateTargetUserRow(r)) continue;
      let d = toNum(valFuzzy(r, ['上线天数', '已上线天数']));
      if (d == null || !Number.isFinite(d) || d < 1) {
        d = extractOnlineDaysFromPeriod(val(r, '数据周期'));
      }
      if (d == null || !Number.isFinite(d) || d < 1) continue;
      d = Math.floor(d);

      const aid = String(val(r, '活动标识') || '').trim();
      const name = String(
        valFuzzy(r, ['活动名称【修正】', '活动名称【修正】 ', '活动名称', '专题名称']) || '',
      ).trim();
      const launch = normalizeDateKey(
        valFuzzy(r, ['上线日期', '上线时间', '活动上线时间']),
      );

      if (aid && launch) {
        const k = `${aid}\x01${launch}`;
        const p = byAidLaunch.get(k);
        if (p == null || d > p) byAidLaunch.set(k, d);
      }
      if (name && launch) {
        const k = `${name}\x01${launch}`;
        const p = byNameLaunch.get(k);
        if (p == null || d > p) byNameLaunch.set(k, d);
      }
      if (aid) {
        const p = byAid.get(aid);
        if (p == null || d > p) byAid.set(aid, d);
      }
      if (name) {
        const p = byName.get(name);
        if (p == null || d > p) byName.set(name, d);
      }
    }
    return { byAidLaunch, byNameLaunch, byAid, byName };
  }

  function resolveOnlineDays(sumRow) {
    const fromSummaryRaw = toNum(val(sumRow, '已上线天数'));
    const fromSummary =
      fromSummaryRaw != null && Number.isFinite(fromSummaryRaw)
        ? Math.max(1, Math.floor(fromSummaryRaw))
        : null;
    const aid = String(val(sumRow, '活动标识') || '').trim();
    const name = String(
      valFuzzy(sumRow, ['活动名称【修正】', '活动名称【修正】 ', '活动名称', '专题名称']) || '',
    ).trim();
    const launch = normalizeDateKey(
      valFuzzy(sumRow, ['上线日期', '上线时间', '活动上线时间']),
    );
    let indexed = null;
    const idx = state.onlineDaysIndex;
    if (idx) {
      if (aid && launch) {
        const k = `${aid}\x01${launch}`;
        if (idx.byAidLaunch.has(k)) indexed = idx.byAidLaunch.get(k);
      }
      if (indexed == null && name && launch) {
        const k = `${name}\x01${launch}`;
        if (idx.byNameLaunch.has(k)) indexed = idx.byNameLaunch.get(k);
      }
      if (indexed == null && aid && idx.byAid.has(aid)) indexed = idx.byAid.get(aid);
      if (indexed == null && name && idx.byName.has(name)) indexed = idx.byName.get(name);
    }
    const elapsed = elapsedDaysFromLaunch(sumRow);
    let resolved = fromSummary;
    if (indexed != null && Number.isFinite(indexed)) {
      resolved = resolved != null ? Math.max(resolved, indexed) : indexed;
    }
    if (elapsed != null) {
      resolved = resolved != null ? Math.min(resolved, elapsed) : elapsed;
    }
    return resolved != null ? Math.max(1, resolved) : 1;
  }

  // ─── 30 日收入预估 + 同品类分位（与 生成_人鱼全期结论表.py 口径对齐）────────────────
  function buildSnapRevCache(rows) {
    const c = Object.create(null);
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (val(r, '数据分类') !== '当前累计' || !isAggregateTargetUserRow(r)) continue;
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
      if (!aid) continue;
      const prev = m.get(aid);
      if (shouldReplaceSummary(prev, r)) m.set(aid, r);
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

    /** 专题名(取x.x.x第1段) -> 期键(第2段+开始日期) -> 行 */
    const byTopic = new Map();
    for (let i = 0; i < summaries.length; i++) {
      const r = summaries[i];
      const actName = val(r, '活动名称【修正】') || val(r, '活动名称') || '';
      const parsed = parseTopicPeriodFromActivityName(actName);
      const name = parsed.topicKey || val(r, '专题名称');
      if (!name) continue;
      const periodKey2 = parsed.periodKey || String(val(r, '第x次祈愿') || '').trim() || '未识别期';
      const startKey = periodStartDateKey(r) || '未识别开始日期';
      const periodIdentity = `${periodKey2}\x01${startKey}`;
      if (!byTopic.has(name)) byTopic.set(name, new Map());
      const m = byTopic.get(name);
      const prev = m.get(periodIdentity);
      if (shouldReplaceSummary(prev, r)) m.set(periodIdentity, r);
    }

    const topics = [];
    const weekStart = startOfWeekMondayMs();
    const now = Date.now();
    const sevenAgo = now - 7 * 86400000;

    let activityDedupCount = 0;
    byTopic.forEach((aidMap, name) => {
      const arr = Array.from(aidMap.values());
      activityDedupCount += arr.length;

      // 期次重排：按「开始日期」从早到晚定义第1期、第2期...
      const asc = arr.slice().sort((a, b) => {
        const ta = parseLaunchDate(periodStartDateKey(a)) || 0;
        const tb = parseLaunchDate(periodStartDateKey(b)) || 0;
        if (ta !== tb) return ta - tb;
        const aa = val(a, '活动名称【修正】') || val(a, '活动名称') || '';
        const bb = val(b, '活动名称【修正】') || val(b, '活动名称') || '';
        const pa = parseTopicPeriodFromActivityName(aa).periodKey || '';
        const pb = parseTopicPeriodFromActivityName(bb).periodKey || '';
        return String(pa).localeCompare(String(pb));
      });
      for (let pi = 0; pi < asc.length; pi++) {
        asc[pi].__periodNo = pi + 1;
      }
      const arrSorted = asc.slice().sort((a, b) => periodNum(b) - periodNum(a));
      const latest = arrSorted[0];
      const launchMs = parseLaunchDate(val(latest, '上线日期'));
      const inThisWeek = launchMs != null && launchMs >= weekStart;
      const in7d = launchMs != null && launchMs >= sevenAgo;
      topics.push({
        name,
        periods: arrSorted,
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
    onlineDaysIndex: null,
    layerRows: [],
    layerIndex: new Map(),
    splitRows: [],
    accessRows: [],
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

  const INCOME_STRUCTURE_DENY_KEYS = new Set(['对应人群收入占比']);

  const INCOME_STRUCTURE_CHART_COLORS = [
    '#4f46e5',
    '#0d9488',
    '#7c3aed',
    '#d97706',
    '#db2777',
    '#0891b2',
    '#64748b',
  ];

  /** 收入结构：监测表中带「贡献收入占比」的列，按表头原文逐行展示（不合并为礼/券/其余） */
  function collectIncomeStructurePairs(pa) {
    if (!pa || typeof pa !== 'object') return [];
    const out = [];
    const keys = Object.keys(pa);
    for (let i = 0; i < keys.length; i++) {
      const k = String(keys[i] || '')
        .replace(/^\uFEFF/, '')
        .trim();
      if (!k || INCOME_STRUCTURE_DENY_KEYS.has(k)) continue;
      if (!/贡献收入占比$/.test(k)) continue;
      const raw = pa[keys[i]];
      if (raw == null || String(raw).trim() === '') continue;
      const r = rate01(raw);
      if (r == null || !Number.isFinite(r)) continue;
      out.push({ key: k, pct: r });
    }
    out.sort((a, b) => a.key.localeCompare(b.key, 'zh-Hans-CN'));
    return out;
  }

  function incomeStructureBlockHtml(pairs) {
    if (!pairs.length) {
      return (
        '<p class="review-mod-note">监测表未匹配到「…贡献收入占比」类字段，无法展示收入结构。</p>'
      );
    }
    let chartInner = '';
    for (let i = 0; i < pairs.length; i++) {
      const p = pairs[i];
      const c = INCOME_STRUCTURE_CHART_COLORS[i % INCOME_STRUCTURE_CHART_COLORS.length];
      chartInner += hbarHtml(
        p.key,
        Math.min(1, Math.max(0, p.pct)),
        fmtPct(p.pct),
        c,
      );
    }
    const rows = pairs.map(
      (p) =>
        '<tr><th scope="row" class="rv-income-structure-field">' +
        esc(p.key) +
        '</th><td class="rv-income-structure-pct">' +
        esc(fmtPct(p.pct)) +
        '</td></tr>',
    );
    return (
      '<div class="rv-income-structure-wrap">' +
      '<p class="rv-income-structure-chart-caption">横向条为各字段占比（满宽＝100%）；含合计类字段时可能与分项加总接近，以监测表为准。</p>' +
      '<div class="rv-income-structure-chart" role="group" aria-label="贡献收入占比条形图">' +
      chartInner +
      '</div>' +
      '<table class="rv-income-structure-table">' +
      '<thead><tr><th scope="col">监测表字段</th><th scope="col">占比</th></tr></thead>' +
      '<tbody>' +
      rows.join('') +
      '</tbody></table>' +
      '</div>'
    );
  }

  function maxIncomeContributionShare(pairs) {
    let m = null;
    for (let i = 0; i < pairs.length; i++) {
      const p = pairs[i].pct;
      if (p != null && Number.isFinite(p) && (m == null || p > m)) m = p;
    }
    return m;
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

  function oneLinerText(pa, nSnap, genreRaw, revForRank) {
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
    const catPr =
      revForRank != null &&
      Number.isFinite(revForRank) &&
      revForRank > 0
        ? percentileRankSameGenre(revForRank, genreRaw)
        : null;
    if (catPr != null) {
      const g0 = genreRaw != null ? String(genreRaw).trim() : '';
      if (g0 && g0 !== '—') {
        bits.push(
          '在所属「' +
            esc(g0) +
            '」品类中，同期收入约超 ' +
            catPr.toFixed(0) +
            '% 样本',
        );
      } else {
        bits.push('在所属品类中，同期收入约超 ' + catPr.toFixed(0) + '% 样本');
      }
    }
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
        items.push('参与付费率偏低，可结合礼包/券结构（②）与访问来源贡献（③）排查转化断点。');
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

  /**
   * 综合结论：根据参与付费率、触达、目标达成度、同品类分位等信号生成 1～2 段总述，置于要点列表之上。
   */
  function buildComprehensiveVerdictHtml(pa, pyes, synthCtx) {
    if (!pa) return '';
    const join = rate01(val(pa, '参与付费率'));
    const tgt = rate01(val(pa, '目标触达率'));
    const goalPct = parseGoalPercentCell(val(pa, '目标达成度'));
    const catPr = synthCtx.catPr;
    const linearEst = synthCtx.linearEst;
    const scriptProgressPct = synthCtx.scriptProgressPct;
    const progShow =
      synthCtx.usedScript30 && scriptProgressPct != null
        ? scriptProgressPct
        : linearEst.progressPct;

    let freeShare = null;
    const fd = toNum(val(pa, '免费抽卡用户数'));
    const du = toNum(val(pa, '抽卡用户数'));
    if (fd != null && du != null && du > 0) freeShare = fd / du;

    const tgtUserShare = pyes ? rate01(val(pyes, '对应人群收入占比')) : null;

    let neg = 0;
    let pos = 0;
    if (join != null) {
      if (join < 0.12) neg += 2;
      else if (join < 0.18) neg += 1;
      else if (join >= 0.18) pos += 1;
    }
    if (tgt != null) {
      if (tgt < 0.12) neg += 2;
      else if (tgt < 0.2) neg += 1;
    }
    if (goalPct != null) {
      if (goalPct < 75) neg += 2;
      else if (goalPct < 90) neg += 1;
      else if (goalPct >= 95) pos += 1;
    }
    if (catPr != null) {
      if (catPr < 35) neg += 2;
      else if (catPr < 50) neg += 1;
      if (catPr >= 72) pos += 2;
      else if (catPr >= 55) pos += 1;
    }
    if (freeShare != null && freeShare > 0.55 && join != null && join < 0.18) neg += 1;
    if (progShow != null && progShow >= 92) pos += 1;
    if (tgtUserShare != null && tgtUserShare < 0.3 && join != null && join < 0.15) neg += 1;

    const divergeRevJoin =
      catPr != null &&
      catPr >= 58 &&
      join != null &&
      join < 0.15;
    const divergeJoinRev =
      catPr != null &&
      catPr < 45 &&
      join != null &&
      join >= 0.17;

    let opening = '';
    if (divergeRevJoin || divergeJoinRev) {
      opening =
        '本期在同品类收入与参与付费率上信号不完全一致，综合判断宜拆分曝光/人群与付费动线分别归因，再定优先级。';
    } else if (neg >= 5) {
      opening =
        '本期多项核心指标同时走弱，整体承压，剩余周期需在增收、触达与转化上明确抓手并快速试错。';
    } else if (neg >= 3) {
      opening = '本期整体偏谨慎，关键短板已显现，建议结合下方分维度要点逐项验证。';
    } else if (pos >= 4 && neg <= 1) {
      opening =
        '本期在同品类对比与核心转化上的表现相对积极，宜巩固有效动作并持续对照目标达成度收官。';
    } else if (pos >= 2 && neg === 0) {
      opening = '本期监测快照整体相对健康，可在目标达成与分层表现上保持跟踪并做小幅优化。';
    } else {
      opening =
        '本期各维度信号交织，整体尚可但仍有优化空间，可对照下方要点与⑥策略建议安排优先级。';
    }

    let follow = '';
    if (join != null && join < 0.12) {
      follow = ' 其中，参与付费率偏低是当前最突出的矛盾点。';
    } else if (goalPct != null && goalPct < 75) {
      follow = ' 其中，收入目标达成度偏紧，需在剩余窗口内聚焦增收节奏。';
    } else if (tgt != null && tgt < 0.12) {
      follow = ' 其中，目标触达率偏低，宜优先检视宣发与触达效率。';
    } else if (catPr != null && catPr < 35) {
      follow = ' 其中，同品类收入分位偏低，玩法与定价竞争力建议对标近期标杆。';
    } else if (
      freeShare != null &&
      freeShare > 0.55 &&
      join != null &&
      join < 0.18
    ) {
      follow = ' 其中，免费抽占比偏高且付费转化一般，赠抽与付费入口宜联动调整。';
    } else if (neg <= 1 && catPr != null && catPr >= 68) {
      follow = ' 收入侧在同品类中处于偏强分位，建议沉淀可复用因素。';
    } else if (neg <= 1 && join != null && join >= 0.18 && tgt != null && tgt >= 0.18) {
      follow = ' 触达与付费转化勾稽尚可，可保持主线并微调素材与档位。';
    }

    const text = opening + follow;
    return (
      '<div class="review-synthesis-verdict">' +
      '<div class="review-synthesis-verdict__kicker">综合结论</div>' +
      '<p class="review-synthesis-verdict__body">' +
      esc(text) +
      '</p>' +
      '</div>'
    );
  }

  /**
   * 给业务的下一步策略：可执行向建议，与「④ 综合判断」互补；规则与快照指标挂钩。
   * @param {object|null} pa 当前累计·表内 n 日·全量快照行
   * @param {{
   *   join: number|null,
   *   tgt: number|null,
   *   freeShare: number|null,
   *   goalPct: number|null,
   *   catPr: number|null,
   *   tgtUserShare: number|null,
   *   maxContributionShare: number|null,
   *   repeatR: number|null,
   *   n: number,
   *   usedScript30: boolean,
   *   scriptProgressPct: number|null,
   *   linearProgressPct: number|null,
   * }} ctx
   */
  function buildBusinessNextStrategyHtml(pa, ctx) {
    if (!pa) {
      return (
        '<section class="period-next-strategy period-next-strategy--empty">' +
        '<h3 class="period-next-strategy__title"><span class="period-next-strategy__badge">⑦</span>给业务的下一步策略</h3>' +
        '<p class="review-mod-note">当前无快照数据，无法生成策略建议。</p>' +
        '</section>'
      );
    }
    const join = ctx.join;
    const tgt = ctx.tgt;
    const freeShare = ctx.freeShare;
    const goalPct = ctx.goalPct;
    const catPr = ctx.catPr;
    const tgtUserShare = ctx.tgtUserShare;
    const maxContributionShare = ctx.maxContributionShare;
    const repeatR = ctx.repeatR;
    const usedScript30 = ctx.usedScript30;
    const scriptProgressPct = ctx.scriptProgressPct;
    const linearProgressPct = ctx.linearProgressPct;

    const items = [];

    if (join != null && join < 0.12) {
      items.push(
        '优先组织一轮「付费动线」排查：梳理礼包/券档位、首抽引导与付费入口曝光，并对齐宣发落地页与活动内链路，小流量验证后再扩量。',
      );
    } else if (join != null && join < 0.18) {
      items.push(
        '围绕参与付费率做可控实验：尝试调整付费入口位置、券感知强度与免费→付费过渡文案，并按天看漏斗拐点。',
      );
    }

    if (tgt != null && tgt < 0.12) {
      items.push(
        '目标触达率偏低时，建议评估渠道扩容、提醒/推送节奏与素材迭代，并核对目标用户池口径是否与投放一致。',
      );
    } else if (tgt != null && tgt < 0.2) {
      items.push(
        '触达仍有抬升空间：可补充定向曝光或档期内的二次触达，并结合③访问来源贡献模块看渠道效率。',
      );
    }

    if (freeShare != null && freeShare > 0.55 && join != null && join < 0.18) {
      items.push(
        '免费抽占比较高且付费转化一般：可收紧赠抽节奏或增设付费试抽锚点，避免用户长期停留在免费层。',
      );
    }

    if (goalPct != null && goalPct < 75) {
      items.push(
        '收入目标达成度偏紧：建议集中资源在高付费意愿人群（礼包组合、限时加码），并明确剩余窗口内的日销与库存节奏。',
      );
    } else if (goalPct != null && goalPct >= 95) {
      items.push(
        '目标达成度较好：重点转为维稳与长尾收割——控制成本、保留收官物料与客服/社区预案。',
      );
    }

    if (catPr != null && catPr < 35) {
      items.push(
        '同品类分位偏低：对标近期同类活动的中奖池结构、定价与宣发打法，列出 2～3 项可复用改动做灰度。',
      );
    } else if (catPr != null && catPr >= 72) {
      items.push(
        '同品类表现靠前：把本期有效的池型、礼包结构与素材沉淀进 playbook，供后续同类专题快速复用。',
      );
    }

    if (tgtUserShare != null && tgtUserShare < 0.32) {
      items.push(
        '目标用户收入占比偏低：复盘人群定向与内容匹配，必要时收缩泛曝光、加大核心盘承接资源。',
      );
    } else if (tgtUserShare != null && tgtUserShare > 0.68) {
      items.push(
        '收入高度集中在目标用户：巩固核心盘的同时，关注非目标盘的体验与舆情，避免过度牺牲广度。',
      );
    }

    if (maxContributionShare != null && maxContributionShare > 0.82) {
      items.push(
        '收入结构单项占比较高：可测试各收入来源配比，降低单一来源波动风险。',
      );
    }

    if (repeatR != null && repeatR < 0.12 && join != null && join >= 0.15) {
      items.push(
        '复抽率偏弱：可设计阶梯奖励、每日任务或卡池更新节奏，提升已付费用户的持续抽取意愿。',
      );
    }

    const prog =
      usedScript30 && scriptProgressPct != null ? scriptProgressPct : linearProgressPct;
    if (prog != null && prog >= 88 && prog < 100 && goalPct != null && goalPct < 90) {
      items.push(
        '预估进度接近收官：锁定剩余活动日的排期与库存，避免券/礼包断档或峰值时段缺少预案。',
      );
    }

    if (!items.length) {
      items.push(
        '当前快照未触发强规则信号：建议按周会节奏跟踪 KPI，并为礼包、触达与分层各保留 1～2 个可控实验位，便于快速迭代。',
      );
    }

    return (
      '<section class="period-next-strategy" aria-label="给业务的下一步策略">' +
      '<h3 class="period-next-strategy__title"><span class="period-next-strategy__badge">⑦</span>给业务的下一步策略</h3>' +
      '<p class="period-next-strategy__hint">以下为基于本期监测数据的启发式建议，执行前请结合活动阶段、预算与合规要求评审。</p>' +
      '<ol class="period-next-strategy__list">' +
      items.map((t) => '<li>' + esc(t) + '</li>').join('') +
      '</ol>' +
      '</section>'
    );
  }

  /** 同一专题内，按活动标识在 periods（期次降序）中取紧邻的上一期汇总行 */
  function findPrevPeriodSummaryRow(sumRow) {
    const topicName = val(sumRow, '专题名称');
    const aid = val(sumRow, '活动标识');
    if (!topicName || !aid) return null;
    const topic = state.topics.find((x) => x.name === topicName);
    if (!topic || !topic.periods || topic.periods.length < 2) return null;
    const arr = topic.periods;
    for (let i = 0; i < arr.length; i++) {
      if (val(arr[i], '活动标识') !== aid) continue;
      const j = i + 1;
      if (j < arr.length) return arr[j];
      return null;
    }
    return null;
  }

  function parseGoalPercentCell(raw) {
    if (raw == null || raw === '') return null;
    const t = String(raw).trim().replace(/,/g, '');
    const m = t.match(/(-?[\d.]+)\s*%/);
    if (m) {
      const n = parseFloat(m[1]);
      return Number.isFinite(n) ? n : null;
    }
    const n = parseFloat(t);
    return Number.isFinite(n) ? n : null;
  }

  function isAllTargetUserKpiRow(r) {
    return isAggregateTargetUserRow(r);
  }

  /** 数据周期是否为「上线至今」类（排除「上线n日内」快照行） */
  function isLaunchToDateDataPeriod(dp) {
    const s = String(dp || '').trim();
    if (!s) return false;
    if (/^上线\d+日内$/.test(s)) return false;
    if (s === '上线至今' || s === '上线至今累计' || s === '至今累计') return true;
    if (s.indexOf('上线至今') >= 0) return true;
    return false;
  }

  /**
   * 累计触达用户数：优先全量人群行（是否目标用户=全部/全部用户，或目标用户类型=整体）且「数据周期」为上线至今类；
   * 其次读快照行上的「上线至今触达用户数」「累计触达uv」等列；再无则回退为表内「上线n日内」全量行触达（与漏斗一致）。
   */
  function findCumulativeReachTuvAllUsers(rows, aid, pa) {
    const aidNorm = String(aid || '').trim();
    if (pa) {
      const direct = toNum(
        valFuzzy(pa, [
          '上线至今触达用户数',
          '上线至今累计触达用户数',
          '累计触达用户数',
          '累计触达uv',
          '累计触达UV',
        ]),
      );
      if (direct != null && Number.isFinite(direct)) return direct;
    }
    if (rows && rows.length && aidNorm) {
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        if (val(r, '活动标识') !== aidNorm) continue;
        if (val(r, '数据分类') !== '当前累计') continue;
        if (!isAllTargetUserKpiRow(r)) continue;
        if (!isLaunchToDateDataPeriod(val(r, '数据周期'))) continue;
        const v = toNum(
          valFuzzy(r, [
            '累计触达uv',
            '累计触达UV',
            '触达用户数',
            '触达UV',
          ]),
        );
        if (v != null && Number.isFinite(v)) return v;
      }
    }
    if (pa)
      return toNum(
        valFuzzy(pa, ['累计触达uv', '累计触达UV', '触达用户数', '触达UV']),
      );
    return null;
  }

  /** 与本期 KPI 同口径：当前累计·上线 n 日内·全部 + 预估 30 日数值（用于环比） */
  function getKpiComparisonBase(sumRow) {
    const aid = val(sumRow, '活动标识');
    const daysInt = resolveOnlineDays(sumRow);
    const snap = snapRowN(state.rows, aid, daysInt);
    const pa = snap.pa;
    const n = snap.n;
    if (!pa) return null;
    const rev = toNum(val(pa, '总收入'));
    const join = rate01(val(pa, '参与付费率'));
    const paidUv = toNum(val(pa, '付费抽卡用户数'));
    const ppd = toNum(val(pa, '付费抽用户-人均付费抽数'));
    const arppu = toNum(val(pa, '触达付费ARPPU'));
    const goalRaw = val(pa, '目标达成度');
    const goalPct = parseGoalPercentCell(goalRaw);

    const genre = val(sumRow, '品类') || '—';
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
    let est30Num = null;
    if (format30dRevenueKpi(revModelBase, pred30v) != null && pred30v != null) {
      est30Num = Math.round(pred30v);
    } else if (linearEst.est != null) {
      est30Num = Math.round(linearEst.est);
    }

    const cumReachTuv = findCumulativeReachTuvAllUsers(state.rows, aid, pa);

    return {
      rev,
      join,
      paidUv,
      ppd,
      arppu,
      goalPct,
      est30Num,
      cumReachTuv,
    };
  }

  /** 相对上一期的增幅百分比，如（上一期-15%）（上一期+8%）；基期为 0 时不展示避免歧义 */
  function yoyPercentSpanHtml(prevVal, currVal) {
    if (prevVal == null || currVal == null) return '';
    if (!Number.isFinite(prevVal) || !Number.isFinite(currVal)) return '';
    if (prevVal === 0) return '';
    const pct = ((currVal - prevVal) / prevVal) * 100;
    const rounded = Math.round(pct);
    const sign = rounded > 0 ? '+' : '';
    let yoyMod = ' kpi-yoy--flat';
    if (rounded > 0) yoyMod = ' kpi-yoy--up';
    else if (rounded < 0) yoyMod = ' kpi-yoy--down';
    return '<span class="kpi-yoy' + yoyMod + '">（上一期' + sign + rounded + '%）</span>';
  }

  /** 顶栏环比同款配色，行内用于宣发文案 */
  function yoyPercentInlineHtml(prevVal, currVal) {
    if (prevVal == null || currVal == null) return '';
    if (!Number.isFinite(prevVal) || !Number.isFinite(currVal)) return '';
    if (prevVal === 0) return '';
    const pct = ((currVal - prevVal) / prevVal) * 100;
    const rounded = Math.round(pct);
    const sign = rounded > 0 ? '+' : '';
    let mod = ' promo-yoy--flat';
    if (rounded > 0) mod = ' promo-yoy--up';
    else if (rounded < 0) mod = ' promo-yoy--down';
    return '<span class="promo-yoy' + mod + '">（上一期' + sign + rounded + '%）</span>';
  }

  const ACCESS_METRIC_SPECS = [
    {
      key: 'visit',
      label: '访问贡献',
      shareKeys: ['访问贡献占比', '访问贡献', '访问UV贡献占比', '访问用户贡献占比', '访问占比'],
      absKeys: ['访问UV', '访问用户数', '访问人数', '访问量'],
      color: '#0891b2',
    },
    {
      key: 'draw',
      label: '抽卡贡献',
      shareKeys: ['抽卡贡献占比', '抽卡贡献', '抽卡用户贡献占比'],
      absKeys: ['抽卡用户数', '抽卡人数', '抽卡UV'],
      color: '#4f46e5',
    },
    {
      key: 'payDraw',
      label: '付费抽卡贡献',
      shareKeys: ['付费抽卡贡献占比', '付费抽卡贡献', '付费抽用户贡献占比'],
      absKeys: ['付费抽卡用户数', '付费抽卡人数', '付费抽用户数'],
      color: '#7c3aed',
    },
    {
      key: 'payAmt',
      label: '付费金额贡献',
      shareKeys: ['付费金额贡献占比', '付费金额贡献', '收入贡献占比', '收入贡献'],
      absKeys: ['付费金额', '付费收入', '收入金额'],
      color: '#d97706',
    },
  ];

  function buildProjectAccessPeriods(sumRow) {
    const project = String(
      val(sumRow, '活动名称【修正】') || val(sumRow, '活动名称') || val(sumRow, '专题名称') || '',
    ).trim();
    if (!project) return { project: '', current: null, previous: null, history: [], periods: [] };
    const currentLaunchKey = normalizeDateKey(val(sumRow, '上线日期'));
    const rows = state.accessRows || [];
    const whiteSet = new Set(ACCESS_SOURCE_WHITELIST);

    // 第一步：收集所有匹配project且在白名单内的行，附带日期和上线时间
    const matched = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const rp = String(
        valFuzzy(r, ['活动名称【修正】', '活动名称【修正】 ', '活动名称', '专题名称']) || '',
      ).trim();
      if (!rp || rp !== project) continue;
      const lk = normalizeDateKey(valFuzzy(r, ['上线时间', '上线日期', '活动上线时间']));
      if (!lk) continue;
      const src = normalizePrimarySourceName(
        String(valFuzzy(r, ['一级来源', '来源一级', '访问一级来源', '来源']) || ''),
      );
      if (!src || !whiteSet.has(src)) continue;
      const rawDate = valFuzzy(r, ['日期', '统计日期', '数据日期']);
      const dateKey = normalizeDateKey(rawDate) || lk;
      matched.push({ r, lk, src, dateKey, ts: parseLaunchDate(lk) });
    }

    // 第二步：按上线时间分组，每个上线时间内取最新日期的数据
    const byLaunch = new Map();
    for (let i = 0; i < matched.length; i++) {
      const x = matched[i];
      if (!byLaunch.has(x.lk)) byLaunch.set(x.lk, []);
      byLaunch.get(x.lk).push(x);
    }

    const periodMap = new Map();
    byLaunch.forEach(function (items, lk) {
      // 统计该上线时间下有哪些不同的 dateKey
      const dateSet = new Set();
      for (let i = 0; i < items.length; i++) dateSet.add(items[i].dateKey);
      let maxDate = '';
      for (let i = 0; i < items.length; i++) {
        if (items[i].dateKey > maxDate) maxDate = items[i].dateKey;
      }
      const latest = items.filter(function (x) { return x.dateKey === maxDate; });

      // 按 (一级来源, 来源后缀) 聚合同key候选，选取更贴近明细口径的一条（优先更小 visit）
      const bySuffix = new Map();
      function metricFromRow(row, keys) {
        return toNum(valFuzzy(row, keys));
      }
      function isBetterCandidate(next, prev) {
        if (!prev) return true;
        const nVisit = metricFromRow(next.r, ['访问用户数', '访问UV', '访问人数', '访问量']);
        const pVisit = metricFromRow(prev.r, ['访问用户数', '访问UV', '访问人数', '访问量']);
        if (nVisit != null && pVisit != null && Number.isFinite(nVisit) && Number.isFinite(pVisit) && nVisit !== pVisit) {
          return nVisit < pVisit;
        }
        const nDraw = metricFromRow(next.r, ['抽卡用户数', '抽卡人数', '抽卡UV']);
        const pDraw = metricFromRow(prev.r, ['抽卡用户数', '抽卡人数', '抽卡UV']);
        if (nDraw != null && pDraw != null && Number.isFinite(nDraw) && Number.isFinite(pDraw) && nDraw !== pDraw) {
          return nDraw < pDraw;
        }
        return false;
      }
      for (let i = 0; i < latest.length; i++) {
        var x = latest[i];
        var suffix = String(valFuzzy(x.r, ['来源后缀', '来源标识']) || '').trim();
        var dk = x.src + '\x01' + suffix;
        var prev = bySuffix.get(dk);
        if (!prev || isBetterCandidate(x, prev)) bySuffix.set(dk, x);
      }
      const deduped = Array.from(bySuffix.values());

      console.log('[日期筛选] lk=' + lk + ' | 总行=' + items.length +
        ' | 不同日期=' + Array.from(dateSet).sort().join(', ') +
        ' | 取maxDate=' + maxDate + ' | 筛选后=' + latest.length +
        ' | 后缀去重后=' + deduped.length);
      for (var _di = 0; _di < deduped.length; _di++) {
        var _dx = deduped[_di];
        var _dsuffix = String(valFuzzy(_dx.r, ['来源后缀', '来源标识']) || '');
        var _dsrc2 = String(valFuzzy(_dx.r, ['二级来源', '来源二级']) || '');
        var _dv = toNum(valFuzzy(_dx.r, ['访问用户数', '访问UV', '访问人数', '访问量']));
        console.log('  #' + _di + ' src=' + _dx.src + ' | suffix=' + _dsuffix +
          ' | 二级=' + _dsrc2 + ' | visit=' + _dv + ' | dateKey=' + _dx.dateKey);
      }

      const ts = deduped.length ? deduped[0].ts : 0;
      const p = { launchKey: lk, launchTs: ts || 0, sources: new Map() };
      for (var i = 0; i < deduped.length; i++) {
        var x = deduped[i];
        if (!p.sources.has(x.src)) {
          p.sources.set(x.src, { visit: 0, draw: 0, payDraw: 0, payAmt: 0 });
        }
        var bucket = p.sources.get(x.src);
        var visit = toNum(valFuzzy(x.r, ['访问用户数', '访问UV', '访问人数', '访问量']));
        var draw = toNum(valFuzzy(x.r, ['抽卡用户数', '抽卡人数', '抽卡UV']));
        var payDraw = toNum(valFuzzy(x.r, ['付费抽卡用户数', '付费抽卡人数', '付费抽用户数']));
        var payAmt = toNum(valFuzzy(x.r, ['付费金额', '付费收入', '收入金额', '累计付费金额', '累计付费收入']));
        if (visit != null && Number.isFinite(visit)) visit = Math.round(visit);
        if (draw != null && Number.isFinite(draw)) draw = Math.round(draw);
        if (payDraw != null && Number.isFinite(payDraw)) payDraw = Math.round(payDraw);
        if (visit != null && Number.isFinite(visit)) bucket.visit += visit;
        if (draw != null && Number.isFinite(draw)) bucket.draw += draw;
        if (payDraw != null && Number.isFinite(payDraw)) bucket.payDraw += payDraw;
        if (payAmt != null && Number.isFinite(payAmt)) bucket.payAmt += payAmt;
      }
      periodMap.set(lk, p);
    });

    // 调试日志
    console.group('[访问贡献数据匹配] project=' + project);
    console.log('匹配行数:', matched.length);
    periodMap.forEach(function (p, lk) {
      console.log('上线时间=' + lk + ':');
      p.sources.forEach(function (b, src) {
        console.log(
          '  source=' + src,
          '| visit=' + b.visit, '| draw=' + b.draw,
          '| payDraw=' + b.payDraw, '| payAmt=' + b.payAmt
        );
      });
    });
    console.groupEnd();

    const periods = Array.from(periodMap.values()).sort(function (a, b) { return a.launchTs - b.launchTs; });
    if (!periods.length) return { project, current: null, previous: null, history: [], periods: [] };
    let current = periods[periods.length - 1];
    if (currentLaunchKey) {
      const hit = periods.find(function (p) { return p.launchKey === currentLaunchKey; });
      if (hit) current = hit;
    }
    const curIdx = periods.findIndex(function (p) { return p.launchKey === current.launchKey; });
    const previous = curIdx > 0 ? periods[curIdx - 1] : null;
    const history = periods.filter(function (p) { return p.launchTs < current.launchTs; });
    return { project, current, previous, history, periods };
  }

  function avgShareFromHistory(historyPeriods, source, metricKey) {
    const arr = [];
    for (let i = 0; i < historyPeriods.length; i++) {
      const m = historyPeriods[i].sources.get(source);
      if (!m || !m[metricKey]) continue;
      const v = m[metricKey].share;
      if (v != null && Number.isFinite(v)) arr.push(v);
    }
    if (!arr.length) return null;
    return arr.reduce((a, b) => a + b, 0) / arr.length;
  }

  function formatDeltaPp(v) {
    if (v == null || !Number.isFinite(v)) return '—';
    const pp = v * 100;
    const sign = pp > 0 ? '+' : '';
    return `${sign}${pp.toFixed(1)}pp`;
  }

  function buildAccessSourceConclusion(current, previous, history) {
    const lines = [];
    const srcNames = Array.from(current.sources.keys());
    let best = null;
    for (let i = 0; i < srcNames.length; i++) {
      const src = srcNames[i];
      let score = 0;
      let hit = 0;
      for (let j = 0; j < ACCESS_METRIC_SPECS.length; j++) {
        const mk = ACCESS_METRIC_SPECS[j].key;
        const cur = current.sources.get(src)[mk].share;
        if (cur == null || !Number.isFinite(cur)) continue;
        const prev = previous && previous.sources.get(src) ? previous.sources.get(src)[mk].share : null;
        const hist = avgShareFromHistory(history, src, mk);
        if (prev != null && Number.isFinite(prev)) {
          score += (cur - prev) * 100;
          hit += 1;
        }
        if (hist != null && Number.isFinite(hist)) {
          score += (cur - hist) * 70;
          hit += 1;
        }
      }
      if (!hit) continue;
      if (!best || score > best.score) best = { source: src, score };
    }
    if (best && best.score > 2) {
      lines.push(`本期来源贡献提升最明显的是「${best.source}」，对访问到付费链路的多指标贡献相对更优。`);
    }

    for (let j = 0; j < ACCESS_METRIC_SPECS.length; j++) {
      const m = ACCESS_METRIC_SPECS[j];
      let top = null;
      for (let i = 0; i < srcNames.length; i++) {
        const src = srcNames[i];
        const cur = current.sources.get(src)[m.key].share;
        if (cur == null || !Number.isFinite(cur)) continue;
        if (!top || cur > top.v) top = { src, v: cur };
      }
      if (top && top.v >= 0.35) {
        lines.push(`${m.label}当前由「${top.src}」主导（约 ${fmtPct(top.v)}）。`);
      }
    }
    if (!lines.length) {
      lines.push('各一级来源贡献分布较为均衡，建议结合成本与转化效率做精细化预算分配。');
    }
    return (
      '<div class="promo-reach-summary">' +
      '<p class="promo-reach-summary__p"><span class="promo-reach-summary__tag">综合结论</span>' +
      esc(lines.slice(0, 2).join(' ')) +
      '</p></div>'
    );
  }

  function fmtDeltaAbs(curr, prev) {
    if (curr == null || !Number.isFinite(curr) || prev == null || !Number.isFinite(prev)) return '（—）';
    const d = curr - prev;
    const sign = d > 0 ? '+' : d < 0 ? '' : '';
    return '（' + sign + fmtInt(d) + '）';
  }

  function buildSourceCompactCardHtml(source, curBucket, prevBucket) {
    const metrics = [
      { label: '访问用户数', k: 'visit' },
      { label: '抽卡用户数', k: 'draw' },
      { label: '付费抽卡用户数', k: 'payDraw' },
      { label: '付费金额', k: 'payAmt' },
    ];
    let lines = '';
    for (let i = 0; i < metrics.length; i++) {
      const m = metrics[i];
      const cur = curBucket ? curBucket[m.k] : null;
      const prev = prevBucket ? prevBucket[m.k] : null;
      lines +=
        '<div class="promo-source-metric-line"><span>' + esc(m.label) +
        '</span><strong>' + esc(cur != null && Number.isFinite(cur) ? fmtInt(Math.round(cur)) : '—') +
        '</strong><em>' + esc(fmtDeltaAbs(cur, prev)) + '</em></div>';
    }
    return (
      '<section class="promo-source-card">' +
      '<h4 class="promo-source-card__title">' + esc(source) + '</h4>' +
      lines +
      '</section>'
    );
  }

  function buildAccessContributionSectionHtml(sumRow) {
    if (!state.accessRows || !state.accessRows.length) {
      return '<p class="review-mod-note">未读取到「访问贡献」文件夹内 CSV。</p>';
    }
    const grp = buildProjectAccessPeriods(sumRow);
    if (!grp.current) {
      return '<p class="review-mod-note">访问贡献未匹配到当前项目（按「活动名称【修正】」+「上线时间」）。</p>';
    }
    const srcNames = Array.from(grp.current.sources.keys());
    if (!srcNames.length) {
      return '<p class="review-mod-note">访问贡献文件中未找到有效的「一级来源」行。</p>';
    }

    const periodMeta =
      '<div class="promo-reach-summary">' +
      '<p class="promo-reach-summary__p"><span class="promo-reach-summary__tag">口径</span>' +
      '同一一级来源下所有二级来源的绝对值加总；当期值后括号展示相对上一期的绝对波动。</p>' +
      '<p class="promo-reach-summary__p">当前期上线时间 <strong>' +
      esc(grp.current.launchKey) +
      '</strong>，上一期 <strong>' +
      esc(grp.previous ? grp.previous.launchKey : '—') +
      '</strong>。</p></div>';

    const sourceCards = ACCESS_SOURCE_WHITELIST.map(function (source) {
      const curBucket = grp.current.sources.get(source) || null;
      const prevBucket = grp.previous && grp.previous.sources.get(source) ? grp.previous.sources.get(source) : null;
      return buildSourceCompactCardHtml(source, curBucket, prevBucket);
    }).join('');

    return (
      '<div class="promo-reach-default">' +
      periodMeta +
      '<div class="promo-source-grid">' +
      sourceCards +
      '</div>' +
      '</div>'
    );
  }

  function buildPeriodBoardArticle(sumRow) {
    const pNo = periodNum(sumRow);
    const aid = val(sumRow, '活动标识');
    const daysInt = resolveOnlineDays(sumRow);
    const snap = snapRowN(state.rows, aid, daysInt);
    const pa = snap.pa;
    const pyes = snap.pyes;
    const n = snap.n;
    const genre = val(sumRow, '品类') || '—';
    const theme = val(sumRow, '活动名称【修正】') || val(sumRow, '活动名称') || '—';
    const daysOnline = String(daysInt);

    const join = pa ? rate01(val(pa, '参与付费率')) : null;
    let freeShare = null;
    if (pa) {
      const fd = toNum(val(pa, '免费抽卡用户数'));
      const du = toNum(val(pa, '抽卡用户数'));
      if (fd != null && du != null && du > 0) freeShare = fd / du;
    }
    const repeatR = pa ? rate01(val(pa, '复抽率')) : null;

    const incomeStructurePairs = pa ? collectIncomeStructurePairs(pa) : [];
    const poolBlock = incomeStructureBlockHtml(incomeStructurePairs);
    const maxContributionShare = maxIncomeContributionShare(incomeStructurePairs);

    const tgt = pa ? rate01(val(pa, '目标触达率')) : null;
    const tuv = pa
      ? toNum(
          valFuzzy(pa, ['累计触达uv', '累计触达UV', '触达用户数', '触达UV']),
        )
      : null;
    const tarpu = pa ? toNum(val(pa, '触达ARPU')) : null;

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

    let est30Num = null;
    if (formattedScript) {
      est30Num = pred30v != null ? Math.round(pred30v) : null;
    } else if (linearEst.est != null) {
      est30Num = Math.round(linearEst.est);
    }

    const prevSumRow = findPrevPeriodSummaryRow(sumRow);
    const prevKpi = prevSumRow ? getKpiComparisonBase(prevSumRow) : null;
    const goalPctCurr = parseGoalPercentCell(goalCell);
    const yoyRev = prevKpi ? yoyPercentSpanHtml(prevKpi.rev, rev) : '';
    const yoyEst =
      prevKpi && est30Num != null && prevKpi.est30Num != null
        ? yoyPercentSpanHtml(prevKpi.est30Num, est30Num)
        : '';
    const yoyGoal =
      prevKpi && goalPctCurr != null && prevKpi.goalPct != null
        ? yoyPercentSpanHtml(prevKpi.goalPct, goalPctCurr)
        : '';
    const yoyJoin = prevKpi ? yoyPercentSpanHtml(prevKpi.join, join) : '';
    const yoyPaid = prevKpi ? yoyPercentSpanHtml(prevKpi.paidUv, paidUv) : '';
    const yoyPpd = prevKpi ? yoyPercentSpanHtml(prevKpi.ppd, ppd) : '';
    const yoyArppu = prevKpi ? yoyPercentSpanHtml(prevKpi.arppu, arppu) : '';
    const cumReachTuv = findCumulativeReachTuvAllUsers(state.rows, aid, pa);
    const yoyCumReach = prevKpi ? yoyPercentSpanHtml(prevKpi.cumReachTuv, cumReachTuv) : '';

    const catPr =
      rev != null && Number.isFinite(rev) && rev > 0
        ? percentileRankSameGenre(rev, genre)
        : null;

    const kpiBlock = pa
      ? `<div class="period-kpis">
          <div class="kpi-pill"><span class="kpi-l">${n}日累计收入</span><strong>${fmtInt(rev)}${yoyRev}</strong></div>
          <div class="kpi-pill kpi-pill-goal"><span class="kpi-l">预估30日收入</span><strong>${est30Strong}${yoyEst}</strong></div>
          <div class="kpi-pill kpi-pill-goal"><span class="kpi-l">收入目标达成度</span><strong>${goalHtml}${yoyGoal}</strong></div>
          <div class="kpi-pill"><span class="kpi-l">累计触达用户数</span><strong>${
            cumReachTuv != null && Number.isFinite(cumReachTuv) ? fmtInt(cumReachTuv) : '—'
          }${yoyCumReach}</strong></div>
          <div class="kpi-pill"><span class="kpi-l">参与付费率</span><strong>${join != null ? fmtPct(join) : '—'}${yoyJoin}</strong></div>
          <div class="kpi-pill"><span class="kpi-l">付费抽卡人数</span><strong>${fmtInt(paidUv)}${yoyPaid}</strong></div>
          <div class="kpi-pill"><span class="kpi-l">付费抽人均抽数</span><strong>${ppd != null ? ppd.toFixed(2) : '—'}${yoyPpd}</strong></div>
          <div class="kpi-pill"><span class="kpi-l">付费ARPPU</span><strong>${arppu != null ? arppu.toFixed(2) : '—'}${yoyArppu}</strong></div>
        </div>`
      : '<p class="review-mod-note">无当前累计快照，顶栏 KPI 略。</p>';

    const pps = pa ? toNum(val(pa, '付费单抽均价')) : null;
    const topShare = pa ? rate01(val(pa, '抽到最高等级用户占比')) : null;
    const tgtUserShare = pyes ? rate01(val(pyes, '对应人群收入占比')) : null;

    const poolOl = '';

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
      '<div class="mini-stats">' +
      '<div class="data-line">目标用户收入占比 <strong>' +
      (tgtUserShare != null ? fmtPct(tgtUserShare) : '—') +
      '</strong></div>' +
      (incomeStructurePairs.length
        ? incomeStructurePairs
            .map(
              (p) =>
                '<div class="data-line"><span class="mini-structure-field">' +
                esc(p.key) +
                '</span> <strong>' +
                esc(fmtPct(p.pct)) +
                '</strong></div>',
            )
            .join('')
        : '<div class="data-line">贡献收入占比（表字段）<strong>—</strong></div>') +
      '</div>' +
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

    const strategyCtx = {
      join,
      tgt,
      freeShare,
      goalPct: goalPctCurr,
      catPr,
      tgtUserShare,
      maxContributionShare,
      repeatR,
      n,
      usedScript30,
      scriptProgressPct,
      linearProgressPct: linearEst.progressPct,
    };
    const nextStrategyHtml = buildBusinessNextStrategyHtml(pa, strategyCtx);

    return (
      `<article class="period-board" data-period="${esc(String(pNo))}">` +
      '<header class="period-head">' +
      '<div class="period-head-text">' +
      `<span class="period-badge">第 ${esc(String(pNo))} 期</span>` +
      `<span class="period-theme">${esc(theme)}</span>` +
      `<span class="period-sub">${esc(genre)} · 已上线 ${esc(daysOnline)} 天 · 表内对比 ${esc(String(n))} 日</span>` +
      '</div>' +
      '<p class="period-one-liner"><span class="one-liner-label">一句话总结</span>' +
      oneLinerText(pa, n, genre, rev) +
      '</p>' +
      kpiBlock +
      '</header>' +
      '<div class="review-modules">' +
      buildUserReachFunnelsModuleHtml(sumRow) +
      '<div class="review-mod-grid">' +
      '<section class="review-mod">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">②</span> 抽池策略与收入结构</h3>' +
      poolBlock +
      poolOl +
      '</section>' +
      '</div>' +
      '<section class="review-mod review-mod--promo">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">③</span> 当前抽池访问来源及贡献</h3>' +
      buildAccessContributionSectionHtml(sumRow) +
      '</section>' +
      '<section class="review-mod review-mod--layer">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">④</span> 付费分层表现</h3>' +
      buildLayerModuleHtml(sumRow, state.layerIndex, n, pNo) +
      '</section>' +
      '<section class="review-mod review-mod--synthesis">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">⑤</span> 综合判断（跨维度）</h3>' +
      buildComprehensiveVerdictHtml(pa, pyes, {
        linearEst,
        usedScript30,
        scriptProgressPct,
        catPr,
      }) +
      buildSynthesisModuleHtml(pa, pyes, {
        linearEst,
        usedScript30,
        scriptProgressPct,
        catPr,
      }) +
      '</section>' +
      '</div>' +
      '<details class="period-metrics-fold">' +
      '<summary class="period-metrics-fold-sum">展开 / 收起 ⑥ 六格指标明细</summary>' +
      '' +
      miniGrid +
      '</details>' +
      nextStrategyHtml +
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
    if (normalized.includes('全部')) candidates.push('全部', '全部用户', '整体');
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
    const daysInt = resolveOnlineDays(sumRow);
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
        const dn = extractOnlineDaysFromPeriod(val(r, '数据周期'));
        if (dn == null || dn !== n) continue;
        snapRows.push(r);
      }
      buildUserReachFunnelsModuleHtml._cache.set(cKey, snapRows);
    }

    function targetUserLabelFromRow(r) {
      return String(
        valFuzzy(r, ['是否目标用户', '是否为目标用户', '目标用户类型']) || '',
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

    /** 「全部用户」漏斗：与监测表全量行一致（是否目标用户=全部/全部用户，或目标用户类型=整体） */
    function pickAllUsersFunnelRow() {
      const wants = ['全部', '全部用户', '整体'];
      for (let wi = 0; wi < wants.length; wi++) {
        const want = wants[wi];
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
      const reach = toNum(
        valFuzzy(row, [
          '累计触达uv',
          '累计触达UV',
          '触达用户数',
          '触达UV',
        ]),
      );
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
          '」；四卡对应监测表「是否目标用户 / 是否为目标用户 / 目标用户类型」下「全部·全部用户·整体 / 目标阅读 / 目标IP付费 / 非目标阅读」各行，与下图漏斗同快照。',
      ) +
      '</p>';

    const insightHtml = buildFunnelCrossSegmentInsightHtml(segmentData);

    return (
      `<section class="review-mod rv-funnelModule">` +
      `<h3 class="review-mod-title"><span class="review-mod-badge">①</span>用户触达</h3>` +
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

  function parseLooseCsvToRows(text, logLabel) {
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
        acc.push(normalizeRowKeys(raw));
      },
      complete(results) {
        if (results && results.errors && results.errors.length) {
          for (let i = 0; i < results.errors.length; i++) errs.push(results.errors[i]);
        }
      },
    });
    if (errs.length) {
      console.warn('[wish-review-dynamic] split', logLabel || 'CSV', errs.slice(0, 5));
    }
    return { rows: acc, scanned, kept: acc.length };
  }

  function ingestSplitCsvParts(parts, labelPrefix) {
    let merged = [];
    let scanned = 0;
    let kept = 0;
    if (!parts || !parts.length) return { merged, scanned, kept };
    for (let pi = 0; pi < parts.length; pi++) {
      const one = parseLooseCsvToRows(parts[pi].text, parts[pi].name || labelPrefix);
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
      state.splitRows = [];
      state.accessRows = [];
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
    let splitMerged = [];
    let accessMerged = [];

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
      if (isBundle && raw.split && raw.split.parts && raw.split.parts.length) {
        const ingS = ingestSplitCsvParts(raw.split.parts, '目标用户拆分');
        splitMerged = ingS.merged;
        csvScannedTotal += ingS.scanned;
        csvKeptBeforeDedupe += ingS.kept;
      }
      if (isBundle && raw.access && raw.access.parts && raw.access.parts.length) {
        const ingA = ingestSplitCsvParts(raw.access.parts, '访问贡献');
        accessMerged = ingA.merged;
        csvScannedTotal += ingA.scanned;
        csvKeptBeforeDedupe += ingA.kept;
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
    // 自动校准：若汇总行已上线天数落后于明细「上线n日内」最大口径，则回填为明细口径
    reconcileSummaryOnlineDays(state.rows);
    reconcileSummaryOnlineDays(state.benchRows);
    reconcileSummaryOnlineDays(state.mergedRows);
    state.onlineDaysIndex = buildOnlineDaysIndex(state.rows);
    state.snapCacheMain = buildSnapRevCache(state.rows);
    state.snapCacheMerged = buildSnapRevCache(state.mergedRows);
    state.layerRows = dedupeLayerRows(layerMerged);
    state.layerIndex = buildLayerRowIndex(state.layerRows);
    state.splitRows = dedupeTargetSplitRows(splitMerged);
    state.accessRows = dedupeAccessRows(accessMerged);
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
        splitRowCount: state.splitRows.length,
        accessRowCount: state.accessRows.length,
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
