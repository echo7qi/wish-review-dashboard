/* 祈愿单项目复盘 · 动态看板：解析整体数据监测 CSV，按专题展示、搜索、本周/近7日新上线 */

(function () {
  const $ = (id) => document.getElementById(id);

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
   * 本页看板实际用到的行远少于全表：大量明细（分日、分渠道等）可丢弃，避免数据逐年膨胀时拖垮浏览器。
   * 保留：① 汇总/汇总/全部（建专题列表）；② 当前累计·上线1–9日内·全部或「是」（详情 KPI）。
   */
  function isLaunchWindowWithin9DaysRow(r) {
    if (val(r, '数据分类') !== '当前累计') return false;
    const dr = val(r, '数据周期');
    if (!/^上线\s*([1-9])\s*日内$/.test(dr)) return false;
    const ut = val(r, '是否目标用户');
    return ut === '全部' || ut === '是';
  }

  function isRowUsedByDashboard(r) {
    return isSummaryAllRow(r) || isLaunchWindowWithin9DaysRow(r);
  }

  /** 流式解析：只把「看板用行」推入数组，避免 Papa 一次性生成百万行 objects */
  function parseMonitoringCsvToSlimRows(text, logLabel) {
    const acc = [];
    const errs = [];
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
        const row = normalizeRowKeys(raw);
        if (!isRowUsedByDashboard(row)) return;
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
    return acc;
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
    let md = parseInt(String(maxDays), 10);
    if (Number.isNaN(md)) md = 9;
    const n = Math.min(9, Math.max(1, md));
    return snapAll(rows, aid, n);
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
    layerRows: [],
    workRows: [],
    topics: [],
    fileName: '',
    selected: null,
    filter: '',
    /** 右侧 fish 版复盘：是否已展开该专题除最新期外的往期 */
    showPastPeriodsExpanded: false,
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

  /** 与 fish 顶栏「进度」一致：预估30日 = n 日累计 × 30/n，进度 = 累计/预估 */
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
        '日内·全部」快照；请核对导出列名或运行本地生成脚本。'
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
    bits.push(
      '顶栏「预估30日收入」按 n 日累计线性外推；脚本多模型预估请以本地生成 HTML 为准；同品类分位、与相邻期对比未在浏览器计算',
    );
    return bits.join('；') + '。';
  }

  function buildPeriodBoardArticle(sumRow) {
    const pNo = periodNum(sumRow);
    const aid = val(sumRow, '活动标识');
    const daysRaw = toNum(val(sumRow, '已上线天数'));
    const daysInt = daysRaw != null ? Math.floor(daysRaw) : 9;
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

    let payRows = '';
    if (join != null) payRows += hbarHtml('参与付费率', join, fmtPct(join), '#0d9488');
    if (freeShare != null) payRows += hbarHtml('纯免费抽占抽卡用户', freeShare, fmtPct(freeShare), '#ca8a04');
    if (repeatR != null) payRows += hbarHtml('复抽率', repeatR, fmtPct(repeatR), '#6366f1');
    if (!payRows) {
      payRows = '<p class="review-mod-note">本快照行缺少参与付费率/抽卡用户数等字段时无法绘制条形。</p>';
    }

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
    const goalHtml =
      goalCell !== '' ? esc(goalCell) : '—<span class="kpi-level">（未录入）</span>';

    const est30 = linearEst30Revenue(rev, n);
    const est30Strong =
      est30.est != null && est30.progressPct != null
        ? `${fmtInt(est30.est)} · 进度${est30.progressPct}%<span class="kpi-level">（线性外推）</span>`
        : '—<span class="kpi-level">（缺 n 日累计）</span>';

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

    const payOl =
      '<ol class="review-conclusions">' +
      '<li>条形与监测表「当前累计·上线' +
      esc(String(n)) +
      '日内·全部」一致；解读句式与静态 fish 页同源逻辑请在本地脚本中生成。</li>' +
      '<li>若参与付费率与纯免费抽占比同向异常，请结合赠抽与进付费抽引导复盘（与 fish 使用说明一致）。</li>' +
      '</ol>';
    const poolOl =
      '<ol class="review-conclusions">' +
      '<li>礼/券/其余结构由堆叠条展示；完整有序结论与阈值提示见本地生成 HTML。</li>' +
      '</ol>';
    const launchOl =
      '<ol class="review-conclusions">' +
      '<li>触达类指标与 fish ③ 块同源字段；宣发曝光与 pCTR 需作品明细表，浏览器未载入。</li>' +
      '</ol>';

    const miniGrid =
      '<div class="period-grid">' +
      '<section class="mini-card" aria-label="收入规模">' +
      '<h4 class="mini-card-title">收入规模</h4>' +
      '<p class="mini-con">表内对比 ' +
      esc(String(n)) +
      ' 日·当前累计</p>' +
      '<div class="mini-stats"><div class="data-line">' +
      esc(String(n)) +
      '日累计收入 <strong>' +
      fmtInt(rev) +
      '</strong></div></div>' +
      '<details class="mini-details"><summary>展开说明</summary><div class="detail-inner">n=min(9, 该期「已上线天数」取整)；与 Python 卡片对齐。</div></details>' +
      '</section>' +
      '<section class="mini-card" aria-label="触达效率">' +
      '<h4 class="mini-card-title">触达效率</h4>' +
      '<p class="mini-con">来自监测表同快照</p>' +
      '<div class="mini-stats"><div class="data-line">触达 UV <strong>' +
      fmtInt(tuv) +
      '</strong>｜触达 ARPU <strong>' +
      (tarpu != null ? tarpu.toFixed(3) : '—') +
      '</strong>｜目标触达率 <strong>' +
      (tgt != null ? fmtPct(tgt) : '—') +
      '</strong></div></div>' +
      '<details class="mini-details"><summary>展开说明</summary><div class="detail-inner">与 fish ⑥ 触达小卡同源字段。</div></details>' +
      '</section>' +
      '<section class="mini-card" aria-label="客单价">' +
      '<h4 class="mini-card-title">客单价</h4>' +
      '<p class="mini-con">付费与抽次结构</p>' +
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
      '<details class="mini-details"><summary>展开说明</summary><div class="detail-inner">顶配用户占比列名：抽到最高等级用户占比。</div></details>' +
      '</section>' +
      '<section class="mini-card" aria-label="结构">' +
      '<h4 class="mini-card-title">结构</h4>' +
      '<p class="mini-con">目标用户收入占比（「是」行）</p>' +
      '<div class="mini-stats"><div class="data-line">目标用户收入占比 <strong>' +
      (tgtUserShare != null ? fmtPct(tgtUserShare) : '—') +
      '</strong>｜礼包 <strong>' +
      (giftR != null ? fmtPct(giftR) : '—') +
      '</strong>｜祈愿券 <strong>' +
      (ticketR != null ? fmtPct(ticketR) : '—') +
      '</strong></div></div>' +
      '<details class="mini-details"><summary>展开说明</summary><div class="detail-inner">目标用户收入占比为「是否目标用户=是」行的对应人群收入占比。</div></details>' +
      '</section>' +
      '<section class="mini-card" aria-label="收入节奏">' +
      '<h4 class="mini-card-title">收入节奏</h4>' +
      '<p class="mini-con">同期累计 + 线性外推 30 日</p>' +
      '<div class="mini-stats"><div class="data-line">同期' +
      esc(String(n)) +
      '日累计收入 <strong>' +
      fmtInt(rev) +
      '</strong></div>' +
      (est30.est != null
        ? '<div class="data-line">线性预估 30 日累计 <strong>' +
          fmtInt(est30.est) +
          '</strong>｜进度 <strong>' +
          esc(String(est30.progressPct)) +
          '%</strong></div>'
        : '') +
      '</div>' +
      '<details class="mini-details"><summary>展开说明</summary><div class="detail-inner">预估 = 当前 n 日累计 ÷ n × 30；进度 = 当前累计 ÷ 预估。与 Python 脚本中的混合 k / OLS 等模型可能不同。</div></details>' +
      '</section>' +
      '<section class="mini-card" aria-label="同品类表现">' +
      '<h4 class="mini-card-title">同品类表现</h4>' +
      '<p class="mini-con">浏览器未加载对标池 CSV</p>' +
      '<div class="mini-stats"><div class="data-line">9日收入分位 <strong>—</strong>｜参与付费率 <strong>' +
      (join != null ? fmtPct(join) : '—') +
      '</strong></div></div>' +
      '<details class="mini-details"><summary>展开说明</summary><div class="detail-inner">与人鱼静态页一致的分位与话术需 <code>漫改耽美池</code> 等文件，请本地运行生成脚本。</div></details>' +
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
      '<p class="review-mod-note">阅读顺序：①②③④⑤；⑥ 为下方折叠区。版式对齐 fish-wish-review.html。</p>' +
      '<div class="review-mod-grid">' +
      '<section class="review-mod">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">①</span> 付费表现</h3>' +
      payRows +
      payOl +
      '</section>' +
      '<section class="review-mod">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">②</span> 抽池策略与收入结构</h3>' +
      poolBlock +
      poolOl +
      '</section>' +
      '<section class="review-mod">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">③</span> 宣发与触达</h3>' +
      launchRows +
      launchOl +
      '</section>' +
      '</div>' +
      '<section class="review-mod review-mod--layer">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">④</span> 付费分层表现</h3>' +
      '<p class="review-mod-note">未在浏览器加载《② 分层用户监测》全文（控内存）。分层表与条形要点与 fish ④ 一致时请本地运行 <code>生成_人鱼全期结论表.py --data-bundle</code>。</p>' +
      '</section>' +
      '<section class="review-mod review-mod--synthesis">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">⑤</span> 综合判断（跨维度）</h3>' +
      '<ol class="review-conclusions">' +
      '<li>此处为占位说明：静态 fish 页中 ⑤ 由脚本结合对标池分位、触达、结构等多句生成。若需相同文案与判断，请使用本地生成脚本输出 HTML。</li>' +
      '</ol>' +
      '</section>' +
      '</div>' +
      '<details class="period-metrics-fold">' +
      '<summary class="period-metrics-fold-sum">展开 / 收起 ⑥ 六格指标明细（字段与 fish 同源快照）</summary>' +
      '<p class="review-grid-hint">以下为简版 <strong>⑥</strong>，指标来自监测表当前累计行；完整算式脚注以本地生成页为准。</p>' +
      miniGrid +
      '</details>' +
      '</article>'
    );
  }

  function buildFishEmbedReportHtml(t) {
    const periods = t.periods || [];
    const latest = periods[0];
    if (!latest) {
      return '<p class="review-mod-note">该专题无汇总期次数据。</p>';
    }
    const nPast = periods.length - 1;
    const firstBoard = buildPeriodBoardArticle(latest);
    const pastBoards =
      state.showPastPeriodsExpanded && nPast > 0
        ? periods
            .slice(1)
            .map((sr) => buildPeriodBoardArticle(sr))
            .join('\n')
        : '';

    let pastBar = '';
    if (nPast > 0) {
      if (state.showPastPeriodsExpanded) {
        pastBar =
          '<div class="wishReviewPastPeriodsBar">' +
          '<button type="button" class="btn btn--ghost wishReviewPastPeriodsBtn" data-wish-review-past="collapse">收起往期（仅保留最新一期）</button>' +
          '</div>';
      } else {
        pastBar =
          '<div class="wishReviewPastPeriodsBar">' +
          '<p class="wishReviewPastPeriodsHint muted">默认仅展示<strong>最近上线</strong>的一期完整复盘（与列表「最新期」一致）。</p>' +
          '<button type="button" class="btn wishReviewPastPeriodsBtn" data-wish-review-past="expand">加载往期完整复盘（另 ' +
          esc(String(nPast)) +
          ' 期）</button>' +
          '</div>';
      }
    }

    return (
      '<div class="wishReviewFishRoot fish-report-embedded">' +
      '<p class="sub" style="margin:0 0 16px;line-height:1.55">' +
      '专题 <strong>' +
      esc(t.name) +
      '</strong> · <strong>版式与 fish-wish-review.html 一致</strong>。' +
      (nPast > 0
        ? ' 监测表内共 <strong>' +
          esc(String(periods.length)) +
          '</strong> 期；当前' +
          (state.showPastPeriodsExpanded ? '已展开全部期次' : '仅详解最新一期') +
          '。'
        : ' 本专题仅 1 期。') +
      ' <strong>顶栏「预估30日」为线性外推</strong>；多模型 MAPE 与⑤ 长结论、④ 分层、同品类分位等仍依赖本地：<code>生成_人鱼全期结论表.py --topic ' +
      esc(t.name) +
      "'</code>。</p>" +
      '<div class="fish-period-stack">' +
      firstBoard +
      '</div>' +
      pastBar +
      (pastBoards ? '<div class="fish-period-stack wishReviewPastStack">' + pastBoards + '</div>' : '') +
      '</div>'
    );
  }

  function renderDetail(topicName) {
    const host = $('wishReviewDetailInner');
    if (!host) return;

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
    // Always generate the review fully on-page (no local HTML iframe).
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
    state.showPastPeriodsExpanded = false;
    state.selected = name;
    renderList();
    renderDetail(name);
  }

  async function loadFromBinding() {
    const status = $('wishReviewDashStatus');
    const src = $('wishReviewDashSource');
    const readFn = window.wishReviewReadMonitorCsv;
    if (!readFn) {
      if (status) status.textContent = '脚本未就绪。';
      return;
    }
    if (status) {
      status.textContent = '正在解析整体数据监测（流式瘦身，仅保留看板用行）…';
    }
    const mainBlock = await readFn();
    if (!mainBlock.ok) {
      if (status) status.textContent = mainBlock.error || '读取失败';
      if (src) src.textContent = '';
      state.rows = [];
      state.benchRows = [];
      state.layerRows = [];
      state.workRows = [];
      state.topics = [];
      if (typeof window !== 'undefined') window.__WISH_REVIEW_BUNDLE_DATA__ = null;
      renderList();
      renderDetail(null);
      return;
    }
    const parts = mainBlock.parts && mainBlock.parts.length ? mainBlock.parts : null;
    let mergedRows = [];
    try {
      if (parts) {
        for (let pi = 0; pi < parts.length; pi++) {
          mergedRows = mergedRows.concat(
            parseMonitoringCsvToSlimRows(parts[pi].text, parts[pi].name),
          );
        }
      } else if (mainBlock.text) {
        mergedRows = parseMonitoringCsvToSlimRows(
          mainBlock.text,
          mainBlock.fileName || '监测表',
        );
      } else {
        if (status) {
          status.textContent = '读取结果缺少 CSV 内容，请刷新页面后重试。';
        }
        return;
      }
    } catch (e) {
      if (status) status.textContent = 'CSV 解析失败';
      return;
    }
    state.rows = dedupeMonitoringRows(mergedRows);
    state.benchRows = [];
    state.layerRows = [];
    state.workRows = [];
    if (typeof window !== 'undefined') {
      window.__WISH_REVIEW_BUNDLE_DATA__ = {
        mainRowCount: state.rows.length,
        benchRowCount: 0,
        layerRowCount: 0,
        workRowCount: 0,
        loadedAt: Date.now(),
        note: '仅保留汇总行+当前累计·上线1–9日内；流式解析降低内存',
      };
    }
    const built = buildTopicModels(state.rows);
    state.topics = built.topics;
    state._meta = built;
    state.fileName = mainBlock.fileName;
    state.fileNames =
      mainBlock.fileNames ||
      (parts ? parts.map((p) => p.name) : mainBlock.fileName ? [mainBlock.fileName] : []);
    const mtime = new Date(mainBlock.lastModified).toLocaleString('zh-CN', { hour12: false });
    const nFiles = parts ? parts.length : 1;
    if (status) {
      status.textContent =
        (nFiles > 1
          ? `已合并 ${nFiles} 个监测 CSV → ${state.rows.length} 行（跨文件去重后）`
          : `已加载 ${state.rows.length} 行`) +
        ` · ${built.topicCount} 个专题 · ${built.activityDedupCount} 个祈愿活动` +
        `（汇总·全部 原始 ${built.rawSummaryCount} 行，专题内按活动标识去重）` +
        ' · 已瘦身解析（原表再大也只保留看板用行）；对标池等未载入，全量请用本地脚本';
    }
    if (src) {
      const label =
        mainBlock.fileNames && mainBlock.fileNames.length
          ? mainBlock.fileNames.join(' + ')
          : mainBlock.fileName || (parts && parts.map((p) => p.name).join(' + ')) || '—';
      src.textContent = `${label} · ${mtime}`;
    }

    // Cache mtime for auto refresh.
    state.lastMainMonitorLastModified = mainBlock.lastModified || null;
    ensureAutoRefresh();

    state.showPastPeriodsExpanded = false;
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

    const detailCard = $('wishReviewDetail');
    if (detailCard) {
      detailCard.addEventListener('click', (ev) => {
        const b = ev.target.closest('.wishReviewPastPeriodsBtn');
        if (!b) return;
        const act = b.getAttribute('data-wish-review-past');
        if (act === 'expand') state.showPastPeriodsExpanded = true;
        else if (act === 'collapse') state.showPastPeriodsExpanded = false;
        renderDetail(state.selected);
      });
    }

    document.addEventListener('wishreview:datasource-updated', () => loadFromBinding());

    loadFromBinding();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
