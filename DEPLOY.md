# 祈愿复盘独立站 · 部署

## 默认流程（推荐）

1. 仓库已启用 **GitHub Actions** 发布：见 [`.github/workflows/deploy-pages.yml`](.github/workflows/deploy-pages.yml)。
2. **推送 `main` 即上线**（约 1–2 分钟）。
3. 本地一键推送（与 Cursor 规则一致）：

```bash
./deploy.sh "说明本次修改"
```

GitHub 仓库 **Settings → Pages**：**Source** 请选择 **GitHub Actions**（不要再用 “Deploy from a branch” 与 Actions 混用）。

## 线上地址

https://echo7qi.github.io/wish-review-dashboard/
