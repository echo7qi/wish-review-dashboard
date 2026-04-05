import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import XLSX from 'xlsx';

const REQUIRED_FIELDS = [
  'project_id',
  'project_name',
  'topic_id',
  'topic_name',
  'category',
  'wish_id',
  'wish_name',
  'start_date',
  'end_date',
  'target_users',
  'reached_target_users',
  'reach_users_cum',
  'paid_users',
  'paid_draw_users',
  'paid_draw_count',
  'paid_revenue',
  'revenue',
];

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const cur = argv[i];
    if (!cur.startsWith('--')) continue;
    const key = cur.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args[key] = true;
      continue;
    }
    args[key] = next;
    i += 1;
  }
  return args;
}

function toNumber(v) {
  if (v == null || String(v).trim() === '') return null;
  const n = Number(String(v).replace(/,/g, ''));
  return Number.isFinite(n) ? n : null;
}

function safeRate(numerator, denominator) {
  if (numerator == null || denominator == null || denominator <= 0) return null;
  return numerator / denominator;
}

function toDateStr(v) {
  if (v == null || String(v).trim() === '') return '';
  if (typeof v === 'number') {
    const d = XLSX.SSF.parse_date_code(v);
    if (!d) return '';
    return `${String(d.y).padStart(4, '0')}-${String(d.m).padStart(2, '0')}-${String(d.d).padStart(2, '0')}`;
  }
  const raw = String(v).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toISOString().slice(0, 10);
}

function normalizeKeys(row) {
  const out = {};
  Object.keys(row || {}).forEach((key) => {
    out[String(key || '').trim().replace(/^\uFEFF/, '')] = row[key];
  });
  return out;
}

function toCanonicalRecord(row, sourceFile) {
  const r = normalizeKeys(row);
  const paidParticipationRateInput = toNumber(r.paid_participation_rate);
  const paidAvgDrawsInput = toNumber(r.paid_avg_draws);
  const paidArppuInput = toNumber(r.paid_arppu);
  const paidSingleDrawPriceInput = toNumber(r.paid_single_draw_price);
  const canonical = {
    project_id: String(r.project_id || '').trim(),
    project_name: String(r.project_name || '').trim(),
    topic_id: String(r.topic_id || '').trim(),
    topic_name: String(r.topic_name || '').trim(),
    category: String(r.category || '').trim(),
    wish_id: String(r.wish_id || '').trim(),
    wish_name: String(r.wish_name || '').trim(),
    start_date: toDateStr(r.start_date),
    end_date: toDateStr(r.end_date),
    target_users: toNumber(r.target_users),
    reached_target_users: toNumber(r.reached_target_users),
    reach_users_cum: toNumber(r.reach_users_cum),
    paid_users: toNumber(r.paid_users),
    paid_draw_users: toNumber(r.paid_draw_users),
    paid_draw_count: toNumber(r.paid_draw_count),
    paid_revenue: toNumber(r.paid_revenue),
    revenue: toNumber(r.revenue),
    paid_single_draw_price: paidSingleDrawPriceInput,
    phase_no: toNumber(r.phase_no),
    online_days: toNumber(r.online_days),
    activity_name_fixed: String(r.activity_name_fixed || '').trim(),
    review_notes: String(r.review_notes || '').trim(),
    source_file: sourceFile,
  };

  if (canonical.revenue == null && canonical.paid_revenue != null) {
    canonical.revenue = canonical.paid_revenue;
  }

  canonical.paid_participation_rate = safeRate(canonical.paid_users, canonical.reach_users_cum);
  canonical.paid_avg_draws = safeRate(canonical.paid_draw_count, canonical.paid_draw_users);
  canonical.paid_arppu = safeRate(canonical.revenue, canonical.paid_draw_users);
  canonical.target_reach_rate = safeRate(canonical.reached_target_users, canonical.target_users);
  if (paidParticipationRateInput != null) canonical.paid_participation_rate = paidParticipationRateInput;
  if (paidAvgDrawsInput != null) canonical.paid_avg_draws = paidAvgDrawsInput;
  if (paidArppuInput != null) canonical.paid_arppu = paidArppuInput;

  return canonical;
}

function collectInputFiles(inputPath) {
  const full = path.resolve(inputPath);
  if (!fs.existsSync(full)) {
    throw new Error(`输入路径不存在: ${full}`);
  }
  const stat = fs.statSync(full);
  if (stat.isFile()) return [full];
  if (!stat.isDirectory()) throw new Error(`输入路径不是文件或目录: ${full}`);
  return fs
    .readdirSync(full)
    .filter((name) => /\.(csv|xlsx|xls)$/i.test(name))
    .map((name) => path.join(full, name))
    .sort();
}

function isWorkbookFile(name) {
  return /\.(csv|xlsx|xls)$/i.test(String(name || ''));
}

function loadRowsFromFile(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  let wb;
  if (ext === '.csv') {
    const text = fs.readFileSync(filePath, 'utf-8');
    wb = XLSX.read(text, { type: 'string', cellDates: false, raw: false });
  } else {
    wb = XLSX.readFile(filePath, { cellDates: false, raw: false });
  }
  const firstSheetName = wb.SheetNames[0];
  if (!firstSheetName) return [];
  const ws = wb.Sheets[firstSheetName];
  return XLSX.utils.sheet_to_json(ws, { defval: '' });
}

function firstExisting(obj, keys) {
  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      const v = obj[key];
      if (v != null && String(v).trim() !== '') return v;
    }
  }
  return null;
}

function parseOnlineWindowDays(periodText) {
  const raw = String(periodText || '').trim();
  const m = raw.match(/上线\s*(\d+)\s*日内/);
  if (!m) return null;
  const d = Number(m[1]);
  return Number.isFinite(d) ? d : null;
}

function isAllTargetUser(row) {
  const v = String(firstExisting(row, ['是否目标用户', '是否为目标用户', '目标用户类型']) || '').trim();
  return v === '' || v === '全部' || v === '全部用户' || v === '整体';
}

function collectFilesInSubdir(sourceRoot, subdirName) {
  const full = path.resolve(sourceRoot, subdirName);
  if (!fs.existsSync(full)) {
    throw new Error(`缺少子目录: ${full}`);
  }
  const files = fs
    .readdirSync(full)
    .filter((name) => isWorkbookFile(name))
    .map((name) => path.join(full, name))
    .sort();
  if (!files.length) {
    throw new Error(`子目录无可用文件: ${full}`);
  }
  return files;
}

function loadMergedRows(filePaths) {
  const rows = [];
  filePaths.forEach((f) => {
    const curRows = loadRowsFromFile(f).map((r) => normalizeKeys(r));
    curRows.forEach((r) => rows.push(r));
  });
  return rows;
}

function buildRecordsFromWishBoardFolders(sourceRoot, opts = {}) {
  const projectId = opts.projectId || 'wish_project';
  const projectName = opts.projectName || '祈愿项目';

  const categoryFiles = collectFilesInSubdir(sourceRoot, '品类数据');
  const summaryFiles = collectFilesInSubdir(sourceRoot, '汇总数据');
  const categoryRows = loadMergedRows(categoryFiles);
  const summaryRows = loadMergedRows(summaryFiles);
  const snapshotByWishDay = new Map();

  const categoryBestByWish = new Map();
  categoryRows.forEach((row, idx) => {
    const wishId = String(firstExisting(row, ['活动标识', 'id']) || '').trim();
    if (!wishId) return;
    const cls = String(firstExisting(row, ['数据分类']) || '').trim();
    const period = String(firstExisting(row, ['数据周期']) || '').trim();
    const days = parseOnlineWindowDays(period);
    if (cls !== '当前累计' || days == null) return;
    if (!isAllTargetUser(row)) return;

    const purchaseDate = toDateStr(firstExisting(row, ['购买日期', '日期'])) || '';
    const sortKey = `${purchaseDate}|${String(idx).padStart(8, '0')}`;
    const snapKey = `${wishId}\x01${days}`;
    const prevSnap = snapshotByWishDay.get(snapKey);
    if (!prevSnap || sortKey >= prevSnap.sortKey) {
      snapshotByWishDay.set(snapKey, {
        sortKey,
        wish_id: wishId,
        day: days,
        revenue: toNumber(firstExisting(row, ['总收入'])),
        reach_users_cum: toNumber(firstExisting(row, ['触达用户数'])),
        paid_participation_rate: toNumber(firstExisting(row, ['触达付费率', '参与付费率'])),
        paid_draw_users: toNumber(firstExisting(row, ['付费抽卡用户数'])),
        paid_avg_draws: toNumber(firstExisting(row, ['付费抽用户-人均付费抽数', '人均付费抽卡数'])),
        paid_single_draw_price: toNumber(firstExisting(row, ['付费单抽均价'])),
        paid_arppu: null,
        target_reach_rate: toNumber(firstExisting(row, ['目标触达率'])),
      });
    }

    const prev = categoryBestByWish.get(wishId);
    if (!prev || sortKey > prev.__sortKey || (sortKey === prev.__sortKey && days >= prev.__days)) {
      categoryBestByWish.set(wishId, { ...row, __days: days, __sortKey: sortKey });
    }
  });

  const summaryGroup = new Map();
  summaryRows.forEach((row) => {
    const wishId = String(firstExisting(row, ['id', '活动标识']) || '').trim();
    if (!wishId) return;
    const type = String(firstExisting(row, ['类型']) || '').trim();
    if (type && type !== '祈愿池') return;
    const dt = toDateStr(firstExisting(row, ['日期']));
    const withDate = { ...row, __date: dt };
    if (!summaryGroup.has(wishId)) summaryGroup.set(wishId, []);
    summaryGroup.get(wishId).push(withDate);
  });

  const records = [];
  const skipped = [];
  const allWishIds = new Set([...categoryBestByWish.keys(), ...summaryGroup.keys()]);
  allWishIds.forEach((wishId) => {
    const c = categoryBestByWish.get(wishId) || {};
    const sRows = summaryGroup.get(wishId) || [];
    const sortedRows = [...sRows].sort((a, b) => String(a.__date).localeCompare(String(b.__date)));
    const latest = sortedRows.length ? sortedRows[sortedRows.length - 1] : {};
    const revenueCum = sortedRows.reduce((sum, r) => sum + (toNumber(firstExisting(r, ['总收入'])) || 0), 0);

    const topicId = String(firstExisting(c, ['专题id', 'topic_id']) || '').trim();
    const topicName = String(firstExisting(c, ['专题名称']) || '').trim();
    const wishName = String(firstExisting(c, ['活动名称【修正】', '抽池名', '名称']) || firstExisting(latest, ['名称']) || '').trim();
    const category = String(firstExisting(c, ['品类']) || '').trim();
    const startDate = toDateStr(firstExisting(c, ['上线日期']) || firstExisting(latest, ['开始日期']));
    const endDate = toDateStr(firstExisting(latest, ['日期']) || firstExisting(c, ['购买日期']) || startDate);

    // 访问用户数口径：优先使用「品类数据」中 当前累计 且购买日期最新 的「触达用户数」
    const reachUsers = toNumber(firstExisting(c, ['触达用户数'])) ?? toNumber(firstExisting(latest, ['访问用户数'])) ?? 0;
    const paidDrawUsers = toNumber(firstExisting(c, ['付费抽卡用户数'])) ?? 0;
    const paidUsers = paidDrawUsers;
    const paidAvgDraws = toNumber(firstExisting(c, ['付费抽用户-人均付费抽数', '人均付费抽卡数']));
    const paidDrawCount = paidAvgDraws != null && paidDrawUsers != null ? paidAvgDraws * paidDrawUsers : 0;
    const paidSingleDrawPrice = toNumber(firstExisting(c, ['付费单抽均价']));
    const paidArppu = revenueCum != null && paidDrawUsers > 0 ? revenueCum / paidDrawUsers : null;
    const paidRevenue = paidArppu != null && paidUsers != null ? paidArppu * paidUsers : 0;

    if (!topicName && !wishName) {
      skipped.push({ wishId, reason: 'missing topic_name and wish_name' });
      return;
    }

    const record = toCanonicalRecord(
      {
        project_id: projectId,
        project_name: projectName,
        topic_id: topicId || wishId,
        topic_name: topicName || wishName || wishId,
        category: category || '未分类',
        wish_id: wishId,
        wish_name: wishName || wishId,
        start_date: startDate,
        end_date: endDate,
        target_users: toNumber(firstExisting(c, ['目标用户数'])) ?? reachUsers,
        reached_target_users: toNumber(firstExisting(c, ['触达用户数'])) ?? reachUsers,
        reach_users_cum: reachUsers,
        paid_users: paidUsers,
        paid_draw_users: paidDrawUsers,
        paid_draw_count: paidDrawCount,
        paid_revenue: paidRevenue,
        revenue: revenueCum,
        paid_participation_rate: toNumber(firstExisting(c, ['触达付费率', '参与付费率'])),
        paid_avg_draws: paidAvgDraws,
        paid_arppu: paidArppu,
        paid_single_draw_price: paidSingleDrawPrice,
        phase_no: toNumber(firstExisting(c, ['第x次祈愿'])),
        online_days: toNumber(firstExisting(c, ['已上线天数'])),
        activity_name_fixed: String(firstExisting(c, ['活动名称【修正】']) || ''),
      },
      `品类数据+汇总数据@${path.basename(sourceRoot)}`,
    );
    records.push(record);
  });

  const wishDayMetrics = {};
  snapshotByWishDay.forEach((snap) => {
    const snapArppu =
      snap.revenue != null && snap.paid_draw_users != null && Number(snap.paid_draw_users) > 0
        ? Number(snap.revenue) / Number(snap.paid_draw_users)
        : null;
    if (!wishDayMetrics[snap.wish_id]) wishDayMetrics[snap.wish_id] = {};
    wishDayMetrics[snap.wish_id][String(snap.day)] = {
      revenue: snap.revenue,
      reach_users_cum: snap.reach_users_cum,
      paid_participation_rate: snap.paid_participation_rate,
      paid_draw_users: snap.paid_draw_users,
      paid_avg_draws: snap.paid_avg_draws,
      paid_single_draw_price: snap.paid_single_draw_price,
      paid_arppu: snapArppu,
      target_reach_rate: snap.target_reach_rate,
    };
  });

  return {
    records,
    sourceFiles: [...categoryFiles, ...summaryFiles],
    skipped,
    snapshots: {
      wish_day_metrics: wishDayMetrics,
    },
  };
}

function percentile(sorted, p) {
  if (!sorted.length) return null;
  if (sorted.length === 1) return sorted[0];
  const idx = (sorted.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  const ratio = idx - lo;
  return sorted[lo] + (sorted[hi] - sorted[lo]) * ratio;
}

function median(nums) {
  if (!nums.length) return null;
  const sorted = [...nums].sort((a, b) => a - b);
  return percentile(sorted, 0.5);
}

function applyRevenue30Forecast(records, wishDayMetrics) {
  const wishCategory = {};
  records.forEach((r) => {
    wishCategory[r.wish_id] = r.category || '未分类';
  });

  const kByCatDay = {};
  const kByDay = {};

  Object.keys(wishDayMetrics || {}).forEach((wishId) => {
    const dayMap = wishDayMetrics[wishId] || {};
    const rev30 = toNumber(dayMap['30'] && dayMap['30'].revenue);
    if (rev30 == null || rev30 <= 0) return;
    const cat = wishCategory[wishId] || '未分类';

    Object.keys(dayMap).forEach((dayKey) => {
      const day = Number(dayKey);
      if (!Number.isFinite(day) || day < 1 || day > 30) return;
      const revD = toNumber(dayMap[dayKey] && dayMap[dayKey].revenue);
      if (revD == null || revD <= 0) return;
      const k = rev30 / revD;
      if (!Number.isFinite(k) || k <= 0) return;

      if (!kByCatDay[cat]) kByCatDay[cat] = {};
      if (!kByCatDay[cat][day]) kByCatDay[cat][day] = [];
      kByCatDay[cat][day].push(k);

      if (!kByDay[day]) kByDay[day] = [];
      kByDay[day].push(k);
    });
  });

  const medByCatDay = {};
  Object.keys(kByCatDay).forEach((cat) => {
    medByCatDay[cat] = {};
    Object.keys(kByCatDay[cat]).forEach((d) => {
      medByCatDay[cat][d] = median(kByCatDay[cat][d]);
    });
  });
  const medByDay = {};
  Object.keys(kByDay).forEach((d) => {
    medByDay[d] = median(kByDay[d]);
  });

  records.forEach((r) => {
    const rev = toNumber(r.revenue);
    const dRaw = toNumber(r.online_days);
    if (rev == null) {
      r.revenue_30d_forecast = null;
      return;
    }
    if (dRaw == null) {
      r.revenue_30d_forecast = rev;
      return;
    }
    const d = Math.max(1, Math.min(30, Math.floor(dRaw)));
    if (d >= 30) {
      r.revenue_30d_forecast = rev;
      return;
    }
    const cat = r.category || '未分类';
    const k = (medByCatDay[cat] && (medByCatDay[cat][d] || medByCatDay[cat][String(d)])) || medByDay[d] || medByDay[String(d)] || 1;
    const pred = rev * k;
    r.revenue_30d_forecast = pred < rev ? rev : pred;
  });
}

function buildCategoryStats(records) {
  const byCategory = new Map();
  records.forEach((r) => {
    if (!byCategory.has(r.category)) byCategory.set(r.category, []);
    byCategory.get(r.category).push(r);
  });

  const metrics = [
    'revenue',
    'revenue_30d_forecast',
    'reach_users_cum',
    'paid_participation_rate',
    'paid_arppu',
    'target_reach_rate',
  ];
  const output = {};
  byCategory.forEach((rows, category) => {
    const one = { sample_size: rows.length, metrics: {} };
    metrics.forEach((metric) => {
      const nums = rows
        .map((r) => r[metric])
        .filter((v) => typeof v === 'number' && Number.isFinite(v))
        .sort((a, b) => a - b);
      one.metrics[metric] = {
        p25: percentile(nums, 0.25),
        p50: percentile(nums, 0.5),
        p75: percentile(nums, 0.75),
        min: nums.length ? nums[0] : null,
        max: nums.length ? nums[nums.length - 1] : null,
      };
    });
    output[category] = one;
  });
  return output;
}

function validateRecords(records) {
  const errors = [];
  const warnings = [];
  const wishIdSet = new Set();

  records.forEach((r, idx) => {
    const label = `row#${idx + 1}(${r.wish_id || 'missing_wish_id'})`;
    REQUIRED_FIELDS.forEach((f) => {
      const val = r[f];
      const missing = val == null || (typeof val === 'string' && val.trim() === '');
      if (missing) errors.push(`${label} 缺失字段 ${f}`);
    });
    if (r.wish_id) {
      if (wishIdSet.has(r.wish_id)) errors.push(`${label} wish_id 重复: ${r.wish_id}`);
      wishIdSet.add(r.wish_id);
    }
    const st = new Date(r.start_date).getTime();
    const ed = new Date(r.end_date).getTime();
    if (!Number.isNaN(st) && !Number.isNaN(ed) && st > ed) {
      errors.push(`${label} start_date > end_date`);
    }
    if (r.reach_users_cum != null && r.reach_users_cum < 0) warnings.push(`${label} reach_users_cum 为负值`);
    if (r.revenue != null && r.revenue < 0) warnings.push(`${label} revenue 为负值`);
  });

  return { errors, warnings };
}

function buildOutput(records, sourceFiles, extras = {}) {
  const sorted = [...records].sort((a, b) => String(a.start_date).localeCompare(String(b.start_date)));
  const projects = [...new Set(sorted.map((r) => r.project_id))];
  const topics = [...new Set(sorted.map((r) => r.topic_id))];
  const categories = [...new Set(sorted.map((r) => r.category))];

  const topicHistory = {};
  sorted.forEach((r) => {
    if (!topicHistory[r.topic_id]) topicHistory[r.topic_id] = [];
    topicHistory[r.topic_id].push(r.wish_id);
  });

  return {
    meta: {
      version: 1,
      generated_at: new Date().toISOString(),
      source_files: sourceFiles.map((f) => path.basename(f)),
      row_count: sorted.length,
    },
    dictionary: {
      required_fields: REQUIRED_FIELDS,
      computed_fields: [
        'paid_participation_rate',
        'paid_avg_draws',
        'paid_arppu',
        'revenue_30d_forecast',
        'target_reach_rate',
      ],
    },
    dimensions: { projects, topics, categories },
    wishes: sorted,
    aggregates: {
      topic_history: topicHistory,
      category_stats: buildCategoryStats(sorted),
    },
    snapshots: extras.snapshots || {},
  };
}

function ensureParentDir(filePath) {
  const parent = path.dirname(filePath);
  fs.mkdirSync(parent, { recursive: true });
}

function main() {
  const args = parseArgs(process.argv);
  const input = args.input || './data/sample-wish-review.csv';
  const sourceRoot = args['source-root'] || '';
  const output = path.resolve(args.output || './data/dashboard-data.json');
  const allowWarnings = Boolean(args['allow-warnings']);
  const projectId = args['project-id'] || '';
  const projectName = args['project-name'] || '';

  let inputFiles = [];
  let records = [];
  let extras = {};
  if (sourceRoot) {
    const built = buildRecordsFromWishBoardFolders(sourceRoot, { projectId, projectName });
    inputFiles = built.sourceFiles;
    records = built.records;
    extras = { snapshots: built.snapshots || {} };
    if (built.skipped && built.skipped.length) {
      console.warn(`已跳过 ${built.skipped.length} 条缺少基础维度的数据行`);
    }
  } else {
    inputFiles = collectInputFiles(input);
    if (!inputFiles.length) throw new Error('输入目录下没有 CSV/XLSX/XLS 文件');
    inputFiles.forEach((filePath) => {
      const rows = loadRowsFromFile(filePath);
      rows.forEach((row) => {
        const record = toCanonicalRecord(row, path.basename(filePath));
        records.push(record);
      });
    });
  }

  applyRevenue30Forecast(records, (extras.snapshots && extras.snapshots.wish_day_metrics) || {});

  const validation = validateRecords(records);
  if (validation.errors.length) {
    throw new Error(`数据校验失败:\n- ${validation.errors.join('\n- ')}`);
  }
  if (validation.warnings.length && !allowWarnings) {
    console.warn(`数据校验警告:\n- ${validation.warnings.join('\n- ')}`);
  }

  const outputJson = buildOutput(records, inputFiles, extras);
  outputJson.validation = validation;

  ensureParentDir(output);
  fs.writeFileSync(output, JSON.stringify(outputJson, null, 2), 'utf-8');
  console.log(`构建完成: ${output}`);
  console.log(`输入文件: ${inputFiles.length} 个, 记录数: ${records.length}`);
}

main();
