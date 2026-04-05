const state = {
  data: null,
  filtered: [],
  selectedWishId: '',
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
    `当前祈愿「${selected.wish_name}」收入 ${fmtNumber(selected.revenue)}，累计触达 ${fmtNumber(selected.reach_users_cum)}，参与付费率 ${fmtPct(selected.paid_participation_rate)}。`,
  );

  if (topicRows.length > 1) {
    const prev = topicRows[topicRows.length - 2];
    const revenueDelta = (selected.revenue ?? 0) - (prev.revenue ?? 0);
    const reachDelta = (selected.reach_users_cum ?? 0) - (prev.reach_users_cum ?? 0);
    parts.push(
      `同专题历史对比（最近一期）收入变化 ${revenueDelta >= 0 ? '+' : ''}${fmtNumber(revenueDelta)}，触达变化 ${reachDelta >= 0 ? '+' : ''}${fmtNumber(reachDelta)}。`,
    );
  } else {
    parts.push('该专题当前仅 1 期数据，暂无法做专题历史环比。');
  }

  if (categoryRows.length > 1 && revenueRank.rank != null) {
    parts.push(
      `在「${selected.category}」品类中，收入排名 ${revenueRank.rank}/${revenueRank.total}（约超过 ${fmtNumber(revenueRank.percentile, 1)}% 样本），参与付费率排名 ${payRateRank.rank}/${payRateRank.total}。`,
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
    ['收入', fmtNumber(selected.revenue)],
    ['累计触达用户', fmtNumber(selected.reach_users_cum)],
    ['参与付费率', fmtPct(selected.paid_participation_rate)],
    ['付费抽卡用户', fmtNumber(selected.paid_draw_users)],
    ['付费人均抽数', fmtNumber(selected.paid_avg_draws, 2)],
    ['付费 ARPPU', fmtNumber(selected.paid_arppu, 2)],
    ['目标用户触达率', fmtPct(selected.target_reach_rate)],
  ];

  box.innerHTML = kpis
    .map(
      ([label, value]) => `
      <article class="kpiCard">
        <div class="kpiLabel">${label}</div>
        <div class="kpiValue">${value}</div>
      </article>
    `,
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
          <th>收入</th>
          <th>累计触达</th>
          <th>参与付费率</th>
          <th>目标用户触达率</th>
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
                <td>${fmtPct(r.target_reach_rate)}</td>
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
    ['revenue', '收入'],
    ['reach_users_cum', '累计触达'],
    ['paid_participation_rate', '参与付费率'],
    ['paid_arppu', '付费 ARPPU'],
    ['target_reach_rate', '目标用户触达率'],
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
            const fmt = isRate ? fmtPct : (n) => fmtNumber(n, key === 'paid_arppu' ? 2 : 0);
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
  const project = byId('filterProject').value;
  const topic = byId('filterTopic').value;
  const category = byId('filterCategory').value;
  const from = parseDate(byId('filterFrom').value);
  const to = parseDate(byId('filterTo').value);

  state.filtered = state.data.wishes
    .filter((r) => !project || r.project_id === project)
    .filter((r) => !topic || r.topic_id === topic)
    .filter((r) => !category || r.category === category)
    .filter((r) => {
      const ts = parseDate(r.start_date);
      if (ts == null) return false;
      if (from != null && ts < from) return false;
      if (to != null && ts > to) return false;
      return true;
    })
    .sort((a, b) => String(a.start_date).localeCompare(String(b.start_date)));

  const wishSelect = byId('filterWish');
  const wishOptions = state.filtered.map((r) => ({ id: r.wish_id, label: `${r.start_date}｜${r.wish_name}` }));
  const prevWish = state.selectedWishId;
  wishSelect.innerHTML = '';
  wishOptions.forEach((w) => {
    const opt = document.createElement('option');
    opt.value = w.id;
    opt.textContent = w.label;
    wishSelect.appendChild(opt);
  });

  if (!wishOptions.length) {
    state.selectedWishId = '';
  } else if (wishOptions.some((w) => w.id === prevWish)) {
    state.selectedWishId = prevWish;
  } else {
    state.selectedWishId = wishOptions[wishOptions.length - 1].id;
  }
  wishSelect.value = state.selectedWishId;
  renderAll();
}

function selectedWish() {
  if (!state.selectedWishId) return null;
  return state.filtered.find((r) => r.wish_id === state.selectedWishId) || null;
}

function renderAll() {
  const selected = selectedWish();
  renderKpis(selected);
  renderTopicHistory(selected, state.filtered);
  renderCategoryCompare(selected, state.filtered);
  renderReviewText(selected, state.filtered);

  const status = byId('dataVersion');
  status.textContent = `数据版本 v${state.data.meta.version} ｜生成时间 ${state.data.meta.generated_at} ｜筛选后 ${state.filtered.length} 条`;
}

function bindEvents() {
  ['filterProject', 'filterTopic', 'filterCategory', 'filterFrom', 'filterTo'].forEach((id) => {
    byId(id).addEventListener('change', applyFilters);
  });
  byId('filterWish').addEventListener('change', (e) => {
    state.selectedWishId = e.target.value;
    renderAll();
  });
}

function initFilters(data) {
  fillSelect(byId('filterProject'), uniqueOptions(data.wishes, 'project_id'), '');
  fillSelect(byId('filterTopic'), uniqueOptions(data.wishes, 'topic_id'), '');
  fillSelect(byId('filterCategory'), uniqueOptions(data.wishes, 'category'), '');
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
