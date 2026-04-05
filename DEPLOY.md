# 祈愿复盘看板 · 发布说明

## 1) 本地更新数据（你自己执行）

1. 将最新 CSV/Excel 放入 `./local-data/`（文件格式参见 `data/wish-review-template.csv`）。
2. 运行数据构建命令：

```bash
npm run build-data:local
```

> 首次使用需要先 `npm install`。

如果你使用的是 `下载/祈愿看板/品类数据 + 汇总数据` 目录结构，直接运行：

```bash
npm run build-data:wishboard
```

这一步会生成 `data/dashboard-data.json`，线上页面读取该文件展示完整复盘与筛选。

## 2) 线上发布（他人访问同一链接）

仓库已启用 GitHub Pages 自动发布：见 [`.github/workflows/deploy-pages.yml`](.github/workflows/deploy-pages.yml)。

```bash
./deploy.sh "chore: 更新祈愿复盘数据与看板"
```

推送 `main` 后约 1-2 分钟生效。

## 3) 分享方式

- 直接分享固定链接：<https://echo7qi.github.io/wish-review-dashboard/>
- 业务同学可在页面中进行筛选与查看完整复盘，不需要上传或更新数据。
