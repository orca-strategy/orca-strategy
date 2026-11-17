#!/bin/bash
echo "=========================================="
echo "   虎鲸策略 (Orca Strategy) API 安全配置"
echo "=========================================="
read -p "1. 请粘贴币安 API Key: " API_KEY
read -s -p "2. 请粘贴币安 API Secret (输入不可见): " API_SECRET
echo ""

if [ -z "$API_KEY" ] || [ -z "$API_SECRET" ]; then
    echo "❌ 错误：API Key 或 Secret 不能为空，请重新运行！"
    exit 1
fi

mkdir -p /etc/systemd/system/crypto-bot.service.d
cat <<EOF > /etc/systemd/system/crypto-bot.service.d/env.conf
[Service]
Environment="BINANCE_API_KEY=${API_KEY}"
Environment="BINANCE_API_SECRET=${API_SECRET}"
EOF

chmod 600 /etc/systemd/system/crypto-bot.service.d/env.conf
systemctl daemon-reload
systemctl enable crypto-bot
systemctl restart crypto-bot

echo ""
echo "✅ 配置成功！机器人已在后台启动。"
echo "👉 运行状态检查：systemctl status crypto-bot"
echo "👉 查看实时日志：journalctl -u crypto-bot -f"
