# 祈愿复盘看板指标字典

## 1) 数据粒度
- 一行代表一个祈愿批次（`wish_id` 唯一）。
- 时间口径默认使用自然日，时区统一为 `Asia/Shanghai`。

## 2) 维度字段

| 字段 | 含义 | 类型 | 必填 | 示例 |
|---|---|---|---|---|
| `project_id` | 项目 ID | string | 是 | `proj_001` |
| `project_name` | 项目名称 | string | 是 | `言情项目A` |
| `topic_id` | 专题 ID | string | 是 | `topic_spring` |
| `topic_name` | 专题名称 | string | 是 | `春日祈愿专题` |
| `category` | 所属品类 | string | 是 | `古风` |
| `wish_id` | 祈愿批次 ID（唯一） | string | 是 | `wish_2026_001` |
| `wish_name` | 祈愿名称 | string | 是 | `春樱初启` |
| `start_date` | 上线日期 | `YYYY-MM-DD` | 是 | `2026-03-01` |
| `end_date` | 下线日期 | `YYYY-MM-DD` | 是 | `2026-03-10` |
| `review_notes` | 复盘备注 | string | 否 | `渠道A素材贡献高` |

## 3) 原子指标字段

| 字段 | 含义 | 类型 | 必填 | 备注 |
|---|---|---|---|---|
| `revenue` | 收入 | number | 是 | 单位与业务账表一致（如元） |
| `target_users` | 目标用户池规模 | number | 是 | 非负数 |
| `reached_target_users` | 已触达目标用户数 | number | 是 | 非负数 |
| `reach_users_cum` | 累计触达用户 | number | 是 | 非负数 |
| `paid_users` | 参与付费用户 | number | 是 | 非负数 |
| `paid_draw_users` | 付费抽卡用户 | number | 是 | 非负数 |
| `paid_draw_count` | 付费抽卡总次数 | number | 是 | 非负数 |
| `paid_revenue` | 付费收入 | number | 是 | 用于 `paid_arppu` 计算 |

## 4) 派生指标公式

| 字段 | 公式 | 含义 |
|---|---|---|
| `paid_participation_rate` | `paid_users / reach_users_cum` | 参与付费率 |
| `paid_avg_draws` | `paid_draw_count / paid_draw_users` | 付费人均抽数 |
| `paid_arppu` | `paid_revenue / paid_users` | 付费 ARPPU |
| `target_reach_rate` | `reached_target_users / target_users` | 目标用户触达率 |

> 分母为 0 或缺失时，派生指标记为 `null`（看板显示 `—`）。

## 5) 缺失值与质量规则
- 维度字段缺失：视为错误，构建失败。
- `wish_id` 重复：视为错误，构建失败。
- `start_date > end_date`：视为错误，构建失败。
- 原子指标为负值：视为警告（默认不阻断构建）。

## 6) 对比口径
- 纵向对比（专题历史）：同 `topic_id` 下按 `start_date` 排序对比。
- 横向对比（品类）：同 `category` 下进行样本分位（P25/P50/P75）和排名对比。
