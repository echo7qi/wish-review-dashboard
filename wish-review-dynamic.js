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
   * 解析监测 CSV：
   * - 为了展示“最新一期的完整复盘”，这里不再做看板用行的筛选瘦身；
   * - 仍然会 normalize 行 key 并在后续逻辑中只使用所需的汇总/快照字段。
   *
   * 注意：数据量很大时浏览器内存/解析时间会显著上升。
   */
  function parseMonitoringCsvToAllRows(text, logLabel) {
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
    // This dashboard always renders only the latest period (no past expansion).
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
    bits.push(
      '顶栏「预估30日收入」为 n 日累计线性外推；同品类分位与多模型预测需载入对标池等数据后另行扩展',
    );
    return bits.join('；') + '。';
  }

  /** ⑤ 综合判断：基于当前快照可计算的指标生成要点，不依赖外部 HTML。 */
  function buildSynthesisModuleHtml(pa, pyes, est30) {
    if (!pa) {
      return (
        '<ol class="review-conclusions">' +
        '<li>当前无「当前累计」快照行，无法生成综合判断；请检查监测表字段与活动标识。</li>' +
        '</ol>'
      );
    }
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
        items.push('参与付费率偏低，可结合礼包/券结构（②）与触达（③）排查转化断点。');
      } else if (join < 0.18) {
        items.push('参与付费率中等略偏低，关注免费抽占比与进付费抽引导。');
      } else {
        items.push('参与付费率在监测快照中处于相对正常区间，可结合收入目标达成度持续观察。');
      }
    }
    if (tgt != null) {
      items.push('目标触达率约 ' + (tgt * 100).toFixed(1) + '%，可对照宣发与渠道效率（③）。');
    }
    if (freeShare != null && freeShare > 0.55 && join != null && join < 0.18) {
      items.push('纯免费抽占比较高且付费转化一般时，建议复盘赠抽与付费入口设计。');
    }
    if (tgtUserShare != null) {
      items.push('目标用户收入占比约 ' + (tgtUserShare * 100).toFixed(1) + '%（「是否目标用户=是」行）。');
    }
    if (est30.progressPct != null && est30.progressPct >= 90) {
      items.push(
        '线性外推进度约 ' +
          est30.progressPct +
          '%，可结合「收入目标达成度」评估是否接近收尾节奏。',
      );
    }
    items.push('同品类分位、分层条形全文、作品明细曝光等依赖额外 CSV，本页当前未合并载入时不作对标结论。');
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
      '<li>条形数据与监测表「当前累计·上线' +
      esc(String(n)) +
      '日内·全部」快照行一致。</li>' +
      '<li>若参与付费率与纯免费抽占比同向偏弱，可结合赠抽与进付费抽引导复盘。</li>' +
      '</ol>';
    const poolOl =
      '<ol class="review-conclusions">' +
      '<li>礼包、祈愿券与其余收入占比由堆叠条展示；阈值与话术可结合业务口径在表外补充。</li>' +
      '</ol>';
    const launchOl =
      '<ol class="review-conclusions">' +
      '<li>触达类指标来自同一快照；宣发曝光与 pCTR 等需作品明细表字段，当前看板未合并载入。</li>' +
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
      '<details class="mini-details"><summary>展开说明</summary><div class="detail-inner">n=min(9, 该期「已上线天数」取整)，与复盘卡片表内对比口径一致。</div></details>' +
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
      '<details class="mini-details"><summary>展开说明</summary><div class="detail-inner">触达 UV、ARPU、目标触达率等同监测表该快照行。</div></details>' +
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
      '<details class="mini-details"><summary>展开说明</summary><div class="detail-inner">预估 = 当前 n 日累计 ÷ n × 30；进度 = 当前累计 ÷ 预估。多模型拟合需另行扩展。</div></details>' +
      '</section>' +
      '<section class="mini-card" aria-label="同品类表现">' +
      '<h4 class="mini-card-title">同品类表现</h4>' +
      '<p class="mini-con">浏览器未加载对标池 CSV</p>' +
      '<div class="mini-stats"><div class="data-line">9日收入分位 <strong>—</strong>｜参与付费率 <strong>' +
      (join != null ? fmtPct(join) : '—') +
      '</strong></div></div>' +
      '<details class="mini-details"><summary>展开说明</summary><div class="detail-inner">9 日收入分位等需合并对标池 CSV 后计算，当前未载入故显示为「—」。</div></details>' +
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
      '<p class="review-mod-note">阅读顺序：模块1（用户触达）→①②③④⑤；⑥ 为下方折叠区。</p>' +
      buildUserReachFunnelsModuleHtml(sumRow) +
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
      '<p class="review-mod-note">「分层用户监测」CSV 未合并载入本页（控内存与性能）。需要分层条形时可在后续版本扩展读取该子目录。</p>' +
      '</section>' +
      '<section class="review-mod review-mod--synthesis">' +
      '<h3 class="review-mod-title"><span class="review-mod-badge">⑤</span> 综合判断（跨维度）</h3>' +
      buildSynthesisModuleHtml(pa, pyes, est30) +
      '</section>' +
      '</div>' +
      '<details class="period-metrics-fold">' +
      '<summary class="period-metrics-fold-sum">展开 / 收起 ⑥ 六格指标明细</summary>' +
      '<p class="review-grid-hint">以下为 <strong>⑥</strong> 六格摘要，指标来自监测表「当前累计」快照行；预估为线性外推。</p>' +
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

  function buildUserReachFunnelsModuleHtml(sumRow) {
    const aid = val(sumRow, '活动标识');
    const daysRaw = toNum(val(sumRow, '已上线天数'));
    const daysInt = daysRaw != null ? Math.floor(daysRaw) : 9;
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

    const isTargetValues = Array.from(
      new Set(snapRows.map((r) => val(r, '是否目标用户')).filter(Boolean)),
    );

    const pickRowByCategory = (categoryLabel) => {
      const resolved = resolveIsTargetUserCategoryValue(isTargetValues, categoryLabel);
      if (!resolved) return null;
      for (let i = 0; i < snapRows.length; i++) {
        if (val(snapRows[i], '是否目标用户') === resolved) return snapRows[i];
      }
      return null;
    };

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

    const funnelCardsHtml = targets
      .map((t) => {
        const r = pickRowByCategory(t.label);
        const v = stepValuesFromRow(r);
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
          `<div class="rv-funnelCard__title">${esc(t.label)}</div>` +
          `<div class="rv-funnel">` +
          stepsInner +
          `</div>` +
          `</div>`
        );
      })
      .join('\n');

    return (
      `<section class="review-mod rv-funnelModule">` +
      `<h3 class="review-mod-title"><span class="review-mod-badge">1</span>用户触达</h3>` +
      `<div class="rv-funnel-grid">${funnelCardsHtml}</div>` +
      `<p class="review-mod-note">四个漏斗分别基于监测表「是否目标用户」列筛选：${esc(
        targets.map((x) => x.label).join(' / '),
      )}。</p>` +
      `</section>`
    );
  }

  function buildFishEmbedReportHtml(t) {
    const periods = t.periods || [];
    if (!periods.length) {
      return '<p class="review-mod-note">该专题无汇总期次数据。</p>';
    }
    const boards = periods.map((sumRow) => buildPeriodBoardArticle(sumRow)).join('');
    return (
      '<div class="wishReviewFishRoot fish-report-embedded">' +
      '<p class="sub" style="margin:0 0 16px;line-height:1.55">' +
      '专题 <strong>' +
      esc(t.name) +
      '</strong> · 共 <strong>' +
      esc(String(periods.length)) +
      '</strong> 期复盘均在下方由监测 CSV 在浏览器内生成（无需本地 HTML）。' +
      ' 顶栏「预估30日」为 <strong>线性外推</strong>；对标分位与分层明细等依赖额外 CSV 合并时方可扩展。</p>' +
      '<div class="fish-period-stack">' +
      boards +
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

  async function loadFromBinding() {
    const status = $('wishReviewDashStatus');
    const src = $('wishReviewDashSource');
    const readFn = window.wishReviewReadMonitorCsv;
    if (!readFn) {
      if (status) status.textContent = '脚本未就绪。';
      return;
    }
    if (status) {
      status.textContent = '正在解析整体数据监测（完整行解析，生成全部期次复盘）…';
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
            parseMonitoringCsvToAllRows(parts[pi].text, parts[pi].name),
          );
        }
      } else if (mainBlock.text) {
        mergedRows = parseMonitoringCsvToAllRows(
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
        note: '完整监测 CSV 解析；专题内全部期次复盘由页面动态生成。',
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
        ' · 已完成监测 CSV 解析（性能取决于数据量）；对标池/分层/作品明细等未合并载入';
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

    loadFromBinding();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
