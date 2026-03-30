#!/bin/bash
# 祈愿复盘独立站 - 推送到 main 触发 GitHub Actions → Pages
# 用法：./deploy.sh [提交说明]
cd "$(dirname "$0")"
msg="${1:-更新祈愿复盘独立站}"
git add -A
if git diff --cached --quiet; then
  echo "无变更，跳过部署。"
  exit 0
fi
git commit -m "$msg"
git push origin main
echo "已推送到 https://echo7qi.github.io/wish-review-dashboard/ ，约 1–2 分钟后生效。"
