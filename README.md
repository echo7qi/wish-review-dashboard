# 祈愿单项目复盘看板

本项目是一个静态线上看板：你在本地更新数据，构建后发布；其他人通过链接查看完整复盘并做筛选。

## 功能范围

- 关键指标展示：收入、累计触达用户、参与付费率、付费抽卡用户、付费人均抽数、付费 ARPPU、目标用户触达率。
- 纵向对比：当前专题下历史祈愿表现。
- 横向对比：当前祈愿在所属品类中的分位和排名。
- 数据筛选：项目 / 专题 / 品类 / 时间范围 / 当前祈愿。

## 数据准备

1. 参考 `data/metrics-dictionary.md` 确认口径。
2. 用 `data/wish-review-template.csv` 作为模板填数。
3. 把最新文件放到 `local-data/`（支持 `csv/xlsx/xls`，可多文件）。

## 本地构建

```bash
npm install
npm run build-data:local
```

构建后会产出：`data/dashboard-data.json`

如果只想快速看示例数据：

```bash
npm run build-data
```

如果你的数据按 `下载/祈愿看板/品类数据 + 汇总数据` 存放，可直接用：

```bash
npm run build-data:wishboard
```

## 本地预览

```bash
npm run preview
```

然后访问提示的本地地址。

## 发布

```bash
./deploy.sh "chore: update wish review dashboard"
```

推送到 `main` 后自动发布到 GitHub Pages。
