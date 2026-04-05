const state = {
  data: null,
  filtered: [],
  selectedWishId: '',
  topicKeyword: '',
};

function fmtNumber(v, digits = 0) {
  if (v == null || Number.isNaN(v)) return '—';
  return Number(v).toLocaleString('zh-CN', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function fmtPct(v, digits = 1) {
  if (v == null || Number.isNaN(v)) return '—';
  return `${(v * 100).toFixed(digits)}%`;
}

function fmtDeltaPct(ratio) {
  if (ratio == null || Number.isNaN(ratio)) return '—';
  const pct = ratio * 100;
  const abs = Math.abs(pct).toFixed(0);
  if (pct > 0) return `+${abs}%`;
  if (pct < 0) return `-${abs}%`;
  return '0%';
}

function byId(id) {
  return document.getElementById(id);
}

function uniqueOptions(rows, key) {
  return [...new Set(rows.map((r) => String(r[key] || '').trim()).filter(Boolean))].sort();
}

function fillSelect(selectEl, options, keepValue) {
  const prev = keepValue || '';
  selectEl.innerHTML = '';
  const all = document.createElement('option');
  all.value = '';
  all.textContent = '全部';
  selectEl.appendChild(all);
  options.forEach((v) => {
    const opt = document.createElement('option');
    opt.value = v;
    opt.textContent = v;
    selectEl.appendChild(opt);
  });
  selectEl.value = options.includes(prev) ? prev : '';
}

function parseDate(dateStr) {
  if (!dateStr) return null;
  const ts = new Date(dateStr).getTime();
  return Number.isNaN(ts) ? null : ts;
}

function weekStartTs(date = new Date()) {
  const d = new Date(date);
  const day = d.getDay();
  const delta = day === 0 ? -6 : 1 - day;
  d.setDate(d.getDate() + delta);
  d.setHours(0, 0, 0, 0);
  return d.getTime();
}

function endOfTodayTs(date = new Date()) {
  const d = new Date(date);
  d.setHours(23, 59, 59, 999);
  return d.getTime();
}

function latestWishByTopic(rows, topicId) {
  const topicRows = rows
    .filter((r) => r.topic_id === topicId)
    .sort((a, b) => String(a.start_date).localeCompare(String(b.start_date)));
  return topicRows.length ? topicRows[topicRows.length - 1] : null;
}

function latestWishWithinToday(rows) {
  if (!rows.length) return null;
  const todayEnd = endOfTodayTs();
  const eligible = rows.filter((r) => {
    const ts = parseDate(r.start_date);
    return ts != null && ts <= todayEnd;
  });
  const pool = eligible.length ? eligible : rows;
  const sorted = [...pool].sort((a, b) => String(a.start_date).localeCompare(String(b.start_date)));
  return sorted.length ? sorted[sorted.length - 1] : null;
}

function buildTopicBuckets(rows) {
  const map = new Map();
  rows.forEach((r) => {
    if (!r.topic_id) return;
    if (!map.has(r.topic_id)) {
      map.set(r.topic_id, {
        topic_id: r.topic_id,
        topic_name: r.topic_name || '未命名专题',
        wishes: [],
      });
    }
    map.get(r.topic_id).wishes.push(r);
  });

  const buckets = [];
  map.forEach((item) => {
    const sorted = [...item.wishes].sort((a, b) => String(a.start_date).localeCompare(String(b.start_date)));
    const latest = sorted[sorted.length - 1];
    const wishCount = new Set(sorted.map((x) => x.wish_id)).size;
    buckets.push({
      topic_id: item.topic_id,
      topic_name: item.topic_name,
      latest,
      wishCount,
      latestTs: parseDate(latest?.start_date),
    });
  });
  return buckets.sort((a, b) => (b.latestTs || 0) - (a.latestTs || 0));
}

function renderTopicCardList(containerId, buckets, selectedTopicId, badgeText) {
  const el = byId(containerId);
  if (!buckets.length) {
    el.innerHTML = '<div class="empty">暂无专题</div>';
    return;
  }
  el.innerHTML = buckets
    .map((b) => {
      const active = selectedTopicId && selectedTopicId === b.topic_id ? 'topicCard--active' : '';
      const badge = badgeText ? `<span class="topicCard__badge">${badgeText}</span>` : '';
      return `
      <button type="button" class="topicCard ${active}" data-topic-id="${b.topic_id}">
        <div class="topicCard__title">${b.topic_name}${badge}</div>
        <div class="topicCard__meta">第 ${b.wishCount} 期 · ${b.latest?.start_date || '—'}</div>
      </button>
    `;
    })
    .join('');
}

function renderLeftTopics(rows, selected) {
  const keyword = state.topicKeyword.trim().toLowerCase();
  const all = buildTopicBuckets(rows).filter((b) =>
    !keyword ? true : String(b.topic_name || '').toLowerCase().includes(keyword),
  );
  const wkStart = weekStartTs();
  const wkEnd = endOfTodayTs();
  const week = all.filter((b) => b.latestTs != null && b.latestTs >= wkStart && b.latestTs <= wkEnd);
  const selectedTopicId = selected?.topic_id || '';
  renderTopicCardList('weekTopicList', week, selectedTopicId, '本周');
  renderTopicCardList('allTopicList', all, selectedTopicId, '');
}

function rankInCategory(rows, category, metric, targetWishId) {
  const pool = rows
    .filter((r) => r.category === category)
    .filter((r) => typeof r[metric] === 'number')
    .sort((a, b) => b[metric] - a[metric]);
  const idx = pool.findIndex((r) => r.wish_id === targetWishId);
  if (idx < 0 || pool.length === 0) {
    return { rank: null, total: pool.length, percentile: null };
  }
  const rank = idx + 1;
  const percentile = ((pool.length - rank) / Math.max(1, pool.length - 1)) * 100;
  return { rank, total: pool.length, percentile };
}

function previousWishInTopic(selected, rows) {
  if (!selected) return null;
  const topicRows = rows.filter((r) => r.topic_id === selected.topic_id && r.wish_id !== selected.wish_id);
  if (!topicRows.length) return null;

  if (selected.phase_no != null) {
    const candidates = topicRows
      .filter((r) => r.phase_no != null && r.phase_no < selected.phase_no)
      .sort((a, b) => {
        if ((b.phase_no || 0) !== (a.phase_no || 0)) return (b.phase_no || 0) - (a.phase_no || 0);
        return String(b.start_date).localeCompare(String(a.start_date));
      });
    if (candidates.length) return candidates[0];
  }

  const byDate = topicRows
    .filter((r) => parseDate(r.start_date) != null)
    .sort((a, b) => String(a.start_date).localeCompare(String(b.start_date)));
  const older = byDate.filter((r) => String(r.start_date) < String(selected.start_date));
  return older.length ? older[older.length - 1] : byDate[byDate.length - 1];
}

function sameDaySnapshot(wishId, day) {
  if (!state.data || !state.data.snapshots || !state.data.snapshots.wish_day_metrics) return null;
  const wishMap = state.data.snapshots.wish_day_metrics[wishId];
  if (!wishMap) return null;
  return wishMap[String(day)] || null;
}

function compareBaseValue(selected, rows, metricKey) {
  const prev = previousWishInTopic(selected, rows);
  if (!prev) return null;
  const day = selected.online_days;
  if (day != null) {
    const snap = sameDaySnapshot(prev.wish_id, day);
    if (snap && snap[metricKey] != null) return snap[metricKey];
  }
  return prev[metricKey];
}

function onlineDaysSinceStart(startDate) {
  const ts = parseDate(startDate);
  if (ts == null) return null;
  const today = new Date();
  today.setHours(23, 59, 59, 999);
  const delta = Math.floor((today.getTime() - ts) / 86400000) + 1;
  return Math.max(1, delta);
}

function topicPhaseIndex(selected, rows) {
  if (!selected) return null;
  const topicRows = rows
    .filter((r) => r.topic_id === selected.topic_id)
    .sort((a, b) => String(a.start_date).localeCompare(String(b.start_date)));
  const idx = topicRows.findIndex((r) => r.wish_id === selected.wish_id);
  return idx >= 0 ? idx + 1 : null;
}

function renderProjectOverview(selected, rows) {
  const box = byId('projectOverview');
  if (!selected) {
    box.innerHTML = '<div class="empty">请选择左侧专题查看复盘。</div>';
    return;
  }
  const phase = selected.phase_no ?? topicPhaseIndex(selected, rows);
  const onlineDays = selected.online_days ?? onlineDaysSinceStart(selected.start_date);
  const phaseLabel = phase != null ? `第 ${phase} 期` : '—';
  const categoryLabel = selected.category || '未分类';
  const onlineLabel = onlineDays != null ? `已上线 ${onlineDays} 天` : '已上线天数待补充';
  const compareLabel = onlineDays != null ? `表内对比 ${onlineDays} 日` : '表内对比周期待补充';
  const activityName = selected.activity_name_fixed || selected.wish_name || selected.topic_name || '未命名活动';

  box.innerHTML = `
    <div class="projectOverview__titleRow">
      <span class="projectOverview__phase">${phaseLabel}</span>
      <strong class="projectOverview__name">${activityName}</strong>
    </div>
    <div class="projectOverview__meta">${categoryLabel} · ${onlineLabel} · ${compareLabel}</div>
  `;
}

function oneLineSummaryText(selected, rows) {
  if (!selected) return '请选择左侧专题后查看复盘结论。';
  const revenueRank = rankInCategory(rows, selected.category, 'revenue', selected.wish_id);
  const payRateRank = rankInCategory(rows, selected.category, 'paid_participation_rate', selected.wish_id);
  const bits = [];
  bits.push(`访问付费率 ${fmtPct(selected.paid_participation_rate)}`);
  if (selected.paid_arppu != null) bits.push(`付费ARPPU ${fmtNumber(selected.paid_arppu, 2)}`);
  if (revenueRank.rank != null) bits.push(`同期当前累计收入位于品类前 ${fmtNumber(100 - revenueRank.percentile, 0)}%`);
  if (payRateRank.rank != null) bits.push(`访问付费率排名 ${payRateRank.rank}/${payRateRank.total}`);
  return bits.join('；');
}

function renderOneLineSummary(selected, rows) {
  const box = byId('projectOneLineSummary');
  const text = oneLineSummaryText(selected, rows);
  box.innerHTML = `
    <span class="projectOneLineSummary__tag">一句话总结</span>
    <span class="projectOneLineSummary__text">${text}</span>
  `;
}

function summarizeComparisons(selected, filtered) {
  if (!selected) return { text: '请选择一个祈愿批次以查看复盘。' };

  const topicRows = filtered
    .filter((r) => r.topic_id === selected.topic_id)
    .sort((a, b) => String(a.start_date).localeCompare(String(b.start_date)));
  const categoryRows = filtered.filter((r) => r.category === selected.category);
  const revenueRank = rankInCategory(filtered, selected.category, 'revenue', selected.wish_id);
  const payRateRank = rankInCategory(filtered, selected.category, 'paid_participation_rate', selected.wish_id);

  const parts = [];
  parts.push(
    `当前祈愿「${selected.wish_name}」当前累计收入 ${fmtNumber(selected.revenue)}，访问用户数 ${fmtNumber(selected.reach_users_cum)}，访问付费率 ${fmtPct(selected.paid_participation_rate)}。`,
  );

  if (topicRows.length > 1) {
    const prev = topicRows[topicRows.length - 2];
    const revenueDelta = (selected.revenue ?? 0) - (prev.revenue ?? 0);
    const reachDelta = (selected.reach_users_cum ?? 0) - (prev.reach_users_cum ?? 0);
    parts.push(
      `同专题历史对比（最近一期）当前累计收入变化 ${revenueDelta >= 0 ? '+' : ''}${fmtNumber(revenueDelta)}，访问用户变化 ${reachDelta >= 0 ? '+' : ''}${fmtNumber(reachDelta)}。`,
    );
  } else {
    parts.push('该专题当前仅 1 期数据，暂无法做专题历史环比。');
  }

  if (categoryRows.length > 1 && revenueRank.rank != null) {
    parts.push(
      `在「${selected.category}」品类中，当前累计收入排名 ${revenueRank.rank}/${revenueRank.total}（约超过 ${fmtNumber(revenueRank.percentile, 1)}% 样本），访问付费率排名 ${payRateRank.rank}/${payRateRank.total}。`,
    );
  } else {
    parts.push(`「${selected.category}」品类样本不足，建议补充更多历史祈愿后再做横向分位判断。`);
  }

  return { text: parts.join('\n') };
}

function renderKpis(selected) {
  const box = byId('kpiGrid');
  if (!selected) {
    box.innerHTML = '<div class="empty">暂无数据</div>';
    return;
  }

  const kpis = [
    { key: 'revenue', label: '当前累计收入', fmt: (v) => fmtNumber(v) },
    { key: 'revenue_30d_forecast', label: '30日收入预估', fmt: (v) => fmtNumber(v) },
    { key: 'reach_users_cum', label: '访问用户数', fmt: (v) => fmtNumber(v) },
    { key: 'paid_participation_rate', label: '访问付费率', fmt: (v) => fmtPct(v) },
    { key: 'paid_draw_users', label: '付费抽卡用户', fmt: (v) => fmtNumber(v) },
    { key: 'paid_avg_draws', label: '付费人均抽数', fmt: (v) => fmtNumber(v, 2) },
    { key: 'paid_arppu', label: '付费 ARPPU', fmt: (v) => fmtNumber(v, 2) },
    { key: 'paid_single_draw_price', label: '付费单抽均价', fmt: (v) => fmtNumber(v, 2) },
  ];

  box.innerHTML = kpis
    .map(
      ({ key, label, fmt }) => {
        const current = selected[key];
        const base = compareBaseValue(selected, state.filtered, key);
        const ratio =
          base != null && current != null && Number(base) !== 0 ? (Number(current) - Number(base)) / Number(base) : null;
        const deltaText = fmtDeltaPct(ratio);
        const deltaClass =
          ratio == null ? 'kpiDelta--na' : ratio > 0 ? 'kpiDelta--up' : ratio < 0 ? 'kpiDelta--down' : 'kpiDelta--flat';
        const delta = `<span class="kpiDelta ${deltaClass}">（${deltaText}）</span>`;
        return `
      <article class="kpiCard">
        <div class="kpiLabel">${label}</div>
        <div class="kpiValue">${fmt(current)}${delta}</div>
      </article>
    `;
      },
    )
    .join('');
}

function renderTopicHistory(selected, rows) {
  const box = byId('topicHistoryTable');
  if (!selected) {
    box.innerHTML = '<div class="empty">请选择祈愿批次</div>';
    return;
  }
  const topicRows = rows
    .filter((r) => r.topic_id === selected.topic_id)
    .sort((a, b) => String(a.start_date).localeCompare(String(b.start_date)));

  box.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>祈愿</th>
          <th>上线区间</th>
          <th>当前累计收入</th>
          <th>访问用户数</th>
          <th>访问付费率</th>
        </tr>
      </thead>
      <tbody>
        ${topicRows
          .map((r) => {
            const active = r.wish_id === selected.wish_id ? 'rowCurrent' : '';
            return `
              <tr class="${active}">
                <td>${r.wish_name}</td>
                <td>${r.start_date} ~ ${r.end_date}</td>
                <td>${fmtNumber(r.revenue)}</td>
                <td>${fmtNumber(r.reach_users_cum)}</td>
                <td>${fmtPct(r.paid_participation_rate)}</td>
              </tr>
            `;
          })
          .join('')}
      </tbody>
    </table>
  `;
}

function renderCategoryCompare(selected, rows) {
  const box = byId('categoryCompareTable');
  if (!selected) {
    box.innerHTML = '<div class="empty">请选择祈愿批次</div>';
    return;
  }
  const metricKeys = [
    ['revenue', '当前累计收入'],
    ['reach_users_cum', '访问用户数'],
    ['paid_participation_rate', '访问付费率'],
    ['paid_draw_users', '付费抽卡用户'],
    ['paid_avg_draws', '付费人均抽数'],
    ['paid_arppu', '付费 ARPPU'],
    ['paid_single_draw_price', '付费单抽均价'],
  ];
  const currentRows = rows.filter((r) => r.category === selected.category);

  box.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>指标</th>
          <th>当前值</th>
          <th>品类 P25</th>
          <th>品类 P50</th>
          <th>品类 P75</th>
          <th>品类排名</th>
        </tr>
      </thead>
      <tbody>
        ${metricKeys
          .map(([key, label]) => {
            const values = currentRows
              .map((r) => r[key])
              .filter((v) => typeof v === 'number')
              .sort((a, b) => a - b);
            const p25 = quantile(values, 0.25);
            const p50 = quantile(values, 0.5);
            const p75 = quantile(values, 0.75);
            const rank = rankInCategory(rows, selected.category, key, selected.wish_id);
            const isRate = key.includes('rate');
            const fmt =
              isRate || key === 'target_reach_rate'
                ? fmtPct
                : (n) => fmtNumber(n, key === 'paid_arppu' || key === 'paid_avg_draws' || key === 'paid_single_draw_price' ? 2 : 0);
            const current = selected[key];
            return `
              <tr>
                <td>${label}</td>
                <td>${fmt(current)}</td>
                <td>${fmt(p25)}</td>
                <td>${fmt(p50)}</td>
                <td>${fmt(p75)}</td>
                <td>${rank.rank != null ? `${rank.rank}/${rank.total}` : '—'}</td>
              </tr>
            `;
          })
          .join('')}
      </tbody>
    </table>
  `;
}

function quantile(sorted, p) {
  if (!sorted.length) return null;
  if (sorted.length === 1) return sorted[0];
  const idx = (sorted.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  const ratio = idx - lo;
  return sorted[lo] + (sorted[hi] - sorted[lo]) * ratio;
}

function renderReviewText(selected, rows) {
  const box = byId('reviewText');
  if (!selected) {
    box.textContent = '请选择祈愿批次后查看复盘结论。';
    return;
  }
  const summary = summarizeComparisons(selected, rows);
  const note = selected.review_notes ? `\n\n业务备注：${selected.review_notes}` : '';
  box.textContent = `${summary.text}${note}`;
}

function applyFilters() {
  state.filtered = state.data.wishes
    .filter((r) => parseDate(r.start_date) != null)
    .sort((a, b) => String(a.start_date).localeCompare(String(b.start_date)));

  const prevWish = state.selectedWishId;
  const hasPrev = state.filtered.some((r) => r.wish_id === prevWish);
  if (!state.filtered.length) {
    state.selectedWishId = '';
  } else if (hasPrev) {
    state.selectedWishId = prevWish;
  } else {
    const defaultWish = latestWishWithinToday(state.filtered) || state.filtered[state.filtered.length - 1];
    state.selectedWishId = defaultWish.wish_id;
  }
  renderAll();
}

function selectedWish() {
  if (!state.selectedWishId) return null;
  return state.filtered.find((r) => r.wish_id === state.selectedWishId) || null;
}

function renderAll() {
  const selected = selectedWish();
  renderLeftTopics(state.filtered, selected);
  renderProjectOverview(selected, state.filtered);
  renderOneLineSummary(selected, state.filtered);
  renderKpis(selected);
  renderTopicHistory(selected, state.filtered);
  renderCategoryCompare(selected, state.filtered);
  renderReviewText(selected, state.filtered);

  const status = byId('dataVersion');
  status.textContent = `数据版本 v${state.data.meta.version} ｜生成时间 ${state.data.meta.generated_at} ｜筛选后 ${state.filtered.length} 条祈愿`;
}

function bindEvents() {
  byId('topicSearch').addEventListener('input', (e) => {
    state.topicKeyword = String(e.target.value || '');
    renderAll();
  });
  ['weekTopicList', 'allTopicList'].forEach((id) => {
    byId(id).addEventListener('click', (e) => {
      const btn = e.target.closest('.topicCard');
      if (!btn) return;
      const topicId = btn.getAttribute('data-topic-id') || '';
      const latest = latestWishByTopic(state.filtered, topicId);
      if (!latest) return;
      state.selectedWishId = latest.wish_id;
      renderAll();
    });
  });
}

function initFilters(data) {
  return data;
}

async function bootstrap() {
  const loading = byId('loadingHint');
  try {
    const res = await fetch('./data/dashboard-data.json', { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.data = await res.json();
    initFilters(state.data);
    bindEvents();
    applyFilters();
    loading.hidden = true;
  } catch (e) {
    loading.hidden = false;
    loading.textContent = `数据加载失败：${e.message}。请先运行 npm run build-data 生成 data/dashboard-data.json。`;
  }
}

bootstrap();
