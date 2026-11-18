# Orca Strategy V1 — Open-Source Binance Futures Trading Bot

[简体中文](README.md) | **English**

An automated, multi-pair quantitative trading bot built in Python for **Binance USDT-M Perpetual Futures**, featuring a dual-layer "Primary + Assistant" architecture and multi-source signal engines.

> ⚠️ **HIGH RISK WARNING**: Uses **10x leverage** by default. For **educational and research purposes only**; **no profits are guaranteed**. Test on Binance Testnet first. Must enable **Hedge Mode (双向持仓模式)** before starting.

---

## ✨ Key Features

* **Dual-Layer Synergy**: Primary Core (`SOLUSDT`) drives central signals/trends; Assistant Pairs (`HYPEUSDT`, `BNBUSDT`, `XRPUSDT`) execute scalping, trend-following, and hedging.
* **Multi-Source Signal Engine**: Triggers via BTC 2-second pulses, SPY 1-minute volume spikes, BTC 1m/15m volume surges, and 8-hour trend/funding persistence.
* **3-Layer Global Risk Management**: 
  * **25% Peak Drawdown Circuit Breaker** (persisted in `risk_state.json`) to halt trading on excessive drawdowns.
  * **Portfolio Profit Lock (`1.3x` Ratio)** to automatically lock in net gains.
  * **18-Minute Cooldown Period** to prevent overtrading.
* **Production-Ready Engineering**: Dual WebSocket/REST streams, automatic cleanup of orphaned orders, and 24/7 VPS log rotation.

---

## 🚀 Quick One-Click Deployment (Recommended)

Run on a fresh **Debian 12/13** or **Ubuntu 22.04/24.04** VPS:

```bash
cd ~ && S="raw"; D="githubusercontent.com"; R="orca-strategy"; curl -sSL "https://\${S}.D/{R}/\({R}/main/install.sh" \vert{} sed 's/\r\)//' | bash
```

### 🔐 Zero-Exposure API Key Setup
After installation finishes:
```bash
cd /root/orca-strategy && ./set_api.sh
```
*Input your Binance API Key and Secret securely (hidden on screen).* **Never enable withdrawal permissions.**

---

## 🛠️ Manual Setup & Management

For manual installation steps, common systemd management commands (status, logs, restart, emergency stop), and community support (Telegram, YouTube), please refer to the repository files or full documentation structure.
