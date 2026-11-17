# =============================================================================
# 虎鲸策略 V1 (Orca Strategy V1)
# =============================================================================
# 开源软件 - 欢迎自由使用、修改、分发
#
# 模块总览：
#   1. 日志与磁盘清理模块     —— 磁盘空间保护、滚动日志、按交易对打标签
#   2. 全局配置模块           —— API 密钥、交易对配置、主动/助理交易对
#   3. 市场情绪分析器         —— 资金费率 + 成交量比率综合情绪打分
#   4. 相关性分析器           —— 多交易对收益率相关性矩阵
#   5. 动态风险管理器         —— 峰值回撤跟踪与熔断
#   6. 全局 BTC 数据管理器    —— 统一维护 BTC K线/趋势/2秒脉冲检测
#   7. 全局 SPY 数据管理器    —— SPY 1m/3m K线与信号
#   8. SymbolLogger          —— 带 symbol 标识的日志器
#   9. TradingBot            —— 单交易对交易机器人（主动/助理共用）
#  10. 全局函数与线程         —— 数据更新、监控、守护重启、主循环
# =============================================================================

import time
import logging
import logging.handlers
from binance.client import Client
from binance.enums import ORDER_TYPE_MARKET, SIDE_BUY, SIDE_SELL, ORDER_TYPE_LIMIT
from binance import ThreadedWebsocketManager
import threading
import os
import math
import subprocess
import signal
import sys
import datetime


# =============================================================================
# 模块 1：日志与磁盘清理
# =============================================================================

def cleanup_disk_space():
    """启动时清理过大的日志文件，防止磁盘占满"""
    try:
        log_files = [
            'multi_bot.log', 'multi_bot.log.1', 'multi_bot.log.2',
            'multi_bot.log.3', 'multi_bot.log.4', 'multi_bot.log.5'
        ]
        for log_file in log_files:
            if os.path.exists(log_file):
                size = os.path.getsize(log_file)
                if size > 10 * 1024 * 1024:
                    print("清理大日志文件")
                    with open(log_file, 'w') as f:
                        f.write("日志文件已清理\n")

        if os.path.exists('multi_bot.log'):
            with open('multi_bot.log', 'r') as f:
                lines = f.readlines()
            if len(lines) > 300:
                with open('multi_bot.log', 'w') as f:
                    f.writelines(lines[-300:])
                print(f"日志已截断，保留最近300行（原{len(lines)}行）")
        return True
    except Exception as e:
        print(f"磁盘清理失败: {e}")
        return True


cleanup_disk_space()


class SafeRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """磁盘满或编码异常时不抛出的安全滚动文件处理器"""

    def __init__(self, filename, maxBytes=0, backupCount=0, encoding='utf-8'):
        super().__init__(filename, maxBytes, backupCount, encoding=encoding)

    def emit(self, record):
        try:
            super().emit(record)
        except UnicodeEncodeError:
            record.msg = record.msg.encode('ascii', errors='replace').decode('ascii')
            super().emit(record)
        except OSError as e:
            if e.errno == 28:
                print(f"[磁盘满] {record.getMessage()}")
            else:
                raise


log_file = 'multi_bot.log'


class SymbolFormatter(logging.Formatter):
    """若日志记录没有 symbol 字段，则自动补 'GLOBAL'"""

    def format(self, record):
        if not hasattr(record, 'symbol'):
            record.symbol = 'GLOBAL'
        return super().format(record)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(symbol)s] - %(message)s',
    handlers=[
        SafeRotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=1),
        logging.StreamHandler()
    ]
)

formatter = SymbolFormatter('%(asctime)s - %(levelname)s - [%(symbol)s] - %(message)s')
for handler in logging.getLogger().handlers:
    handler.setFormatter(formatter)


def periodic_log_cleanup():
    """后台定期（每10小时）截断日志到最近300行"""
    while True:
        try:
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    lines = f.readlines()
                if len(lines) > 300:
                    with open(log_file, 'w') as f:
                        f.writelines(lines[-300:])
                    logging.info(f"日志清理完成，保留最近300行（原{len(lines)}行）")
                for i in range(1, 10):
                    backup_file = f"{log_file}.{i}"
                    if os.path.exists(backup_file):
                        os.remove(backup_file)
        except Exception as e:
            print(f"定期日志清理失败: {e}")
        time.sleep(36000)


log_cleanup_thread = threading.Thread(target=periodic_log_cleanup, daemon=True)
log_cleanup_thread.start()


# =============================================================================
# 模块 2：全局配置
# =============================================================================

API_KEY = os.getenv(
    'BINANCE_API_KEY',
    '登录你的币安账号申请'
)
API_SECRET = os.getenv(
    'BINANCE_API_SECRET',
    '登录你的币安账号申请'
)

# 交易对配置：
#   注意：当前策略的止损/止盈/仓位/最小数量/轮询间隔在具体方法内部硬编码，
#   以下配置项中仅 'leverage' 被 TradingBot 实际读取；
#   其余字段作为配置意图保留，后续可逐步接入各策略方法。
TRADING_CONFIG = {
    'HYPEUSDT': {
        'leverage': 10,
        'stop_loss_pct': 0.078,
        'take_profit_pct': 0.03,
        'position_percentage': 0.02,
        'min_position_qty': 0.001,
        'price_check_interval': 2
    },
    'XRPUSDT': {
        'leverage': 10,
        'stop_loss_pct': 0.078,
        'take_profit_pct': 0.03,
        'position_percentage': 0.02,
        'min_position_qty': 0.001,
        'price_check_interval': 2
    },
    'BNBUSDT': {
        'leverage': 10,
        'stop_loss_pct': 0.078,
        'take_profit_pct': 0.03,
        'position_percentage': 0.02,
        'min_position_qty': 0.001,
        'price_check_interval': 2
    },
    'SOLUSDT': {
        'leverage': 10,
        'stop_loss_pct': 0.065,
        'take_profit_pct': 0.03,
        'position_percentage': 0.02,
        'min_position_qty': 0.001,
        'price_check_interval': 2
    }
}

TRADING_SYMBOLS = ['SOLUSDT']
ASSISTANT_SYMBOLS = ['HYPEUSDT', 'BNBUSDT', 'XRPUSDT']
BTC_SYMBOL = 'BTCUSDT'
SPY_SYMBOL = 'SPYUSDT'

client = None
twm = None
trading_bots = []


# =============================================================================
# 模块 3：市场情绪分析器
# =============================================================================

class MarketSentimentAnalyzer:
    """资金费率 + 成交量比率综合情绪打分器"""

    def __init__(self):
        self.sentiment_cache = {}
        self.update_interval = 60

    def get_funding_rate(self, symbol):
        """获取最近一期资金费率"""
        try:
            funding_info = client.futures_funding_rate(symbol=symbol, limit=1)
            if funding_info:
                return float(funding_info[0]['fundingRate'])
        except Exception as e:
            logging.error(f"[资金费率获取失败] {symbol}: {e}")
        return 0

    def get_volume_ratio(self, symbol):
        """1H 周期：当前成交量 / 前11根平均成交量"""
        try:
            klines = client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_1HOUR,
                limit=24
            )
            if len(klines) >= 12:
                volumes = [float(k[5]) for k in klines]
                current_volume = volumes[-1]
                avg_volume = sum(volumes[-12:-1]) / 11 if len(volumes) > 11 else current_volume
                return current_volume / avg_volume if avg_volume > 0 else 1.0
        except Exception as e:
            logging.error(f"[成交量比率获取失败] {symbol}: {e}")
        return 1.0

    def analyze_sentiment(self, symbol):
        """综合情绪得分 [-1, 1]"""
        now = time.time()
        if symbol in self.sentiment_cache and now - self.sentiment_cache[symbol]['timestamp'] < self.update_interval:
            return self.sentiment_cache[symbol]['sentiment']

        try:
            funding_rate = self.get_funding_rate(symbol)
            volume_ratio = self.get_volume_ratio(symbol)

            sentiment_score = 0.0

            funding_weight = 0.4
            if funding_rate > 0.001:
                sentiment_score += funding_weight
            elif funding_rate > 0.0005:
                sentiment_score += funding_weight * 0.5
            elif funding_rate < -0.001:
                sentiment_score -= funding_weight
            elif funding_rate < -0.0005:
                sentiment_score -= funding_weight * 0.5

            volume_weight = 0.3
            if volume_ratio > 2.0:
                sentiment_score += volume_weight * 0.5
            elif volume_ratio < 0.5:
                sentiment_score -= volume_weight * 0.3

            sentiment_score = max(-1.0, min(1.0, sentiment_score))

            self.sentiment_cache[symbol] = {
                'timestamp': now,
                'sentiment': sentiment_score,
                'funding_rate': funding_rate,
                'volume_ratio': volume_ratio
            }
            return sentiment_score
        except Exception as e:
            logging.error(f"[情绪分析失败] {symbol}: {e}")
            return 0.0

    def get_sentiment_level(self, symbol):
        """情绪等级标签"""
        score = self.analyze_sentiment(symbol)
        if score > 0.7:
            return "EXTREME_BULLISH"
        elif score > 0.3:
            return "BULLISH"
        elif score > 0.1:
            return "SLIGHTLY_BULLISH"
        elif score < -0.7:
            return "EXTREME_BEARISH"
        elif score < -0.3:
            return "BEARISH"
        elif score < -0.1:
            return "SLIGHTLY_BEARISH"
        else:
            return "NEUTRAL"


# =============================================================================
# 模块 4：相关性分析器
# =============================================================================

class CorrelationAnalyzer:
    """基于收益率序列计算多交易对相关性矩阵"""

    def __init__(self):
        self.price_history = {}
        self.correlation_matrix = {}
        self.max_history = 100
        self.update_interval = 300
        self.last_update = 0

    def update_price(self, symbol, price):
        if symbol not in self.price_history:
            self.price_history[symbol] = []
        self.price_history[symbol].append(price)
        if len(self.price_history[symbol]) > self.max_history:
            self.price_history[symbol].pop(0)

    def calculate_correlation(self, symbol1, symbol2):
        if symbol1 not in self.price_history or symbol2 not in self.price_history:
            return 0.0

        prices1 = self.price_history[symbol1]
        prices2 = self.price_history[symbol2]
        min_len = min(len(prices1), len(prices2))
        if min_len < 30:
            return 0.0

        recent_prices1 = prices1[-min_len:]
        recent_prices2 = prices2[-min_len:]

        returns1, returns2 = [], []
        for i in range(1, min_len):
            ret1 = (recent_prices1[i] - recent_prices1[i - 1]) / recent_prices1[i - 1]
            ret2 = (recent_prices2[i] - recent_prices2[i - 1]) / recent_prices2[i - 1]
            returns1.append(ret1)
            returns2.append(ret2)

        if len(returns1) < 20:
            return 0.0

        try:
            n = len(returns1)
            sum_x = sum(returns1)
            sum_y = sum(returns2)
            sum_xy = sum(returns1[i] * returns2[i] for i in range(n))
            sum_x2 = sum(x * x for x in returns1)
            sum_y2 = sum(y * y for y in returns2)
            numerator = n * sum_xy - sum_x * sum_y
            denominator = ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)) ** 0.5
            if denominator == 0:
                return 0.0
            correlation = numerator / denominator
            return correlation if not math.isnan(correlation) else 0.0
        except Exception:
            return 0.0

    def update_correlation_matrix(self):
        now = time.time()
        if now - self.last_update < self.update_interval:
            return
        symbols = list(self.price_history.keys())
        if len(symbols) < 2:
            return
        for i in range(len(symbols)):
            for j in range(i + 1, len(symbols)):
                s1, s2 = symbols[i], symbols[j]
                correlation = self.calculate_correlation(s1, s2)
                self.correlation_matrix.setdefault(s1, {})[s2] = correlation
                self.correlation_matrix.setdefault(s2, {})[s1] = correlation
        self.last_update = now


# =============================================================================
# 模块 5：动态风险管理器
# =============================================================================

class DynamicRiskManager:
    """峰值回撤跟踪，回撤 > 25% 触发停止"""

    def __init__(self):
        self.max_drawdown = 0.0
        self.current_drawdown = 0.0
        self.peak_balance = 0.0

    def update_drawdown(self, current_balance):
        if current_balance > self.peak_balance:
            self.peak_balance = current_balance
            self.current_drawdown = 0.0
        else:
            self.current_drawdown = (
                (self.peak_balance - current_balance) / self.peak_balance
                if self.peak_balance > 0 else 0.0
            )
            self.max_drawdown = max(self.max_drawdown, self.current_drawdown)

    def should_stop_trading(self):
        return self.max_drawdown > 0.25


# =============================================================================
# 模块 6：全局 BTC 数据管理器（单例）
# =============================================================================

class GlobalBTCManager:
    """统一维护 BTC K线、趋势、2秒脉冲检测与 check_que 触发锁"""

    _instance = None
    btc_3m_prices = []
    last_update_time = 0
    update_interval = 60
    data_lock = threading.Lock()

    btc_tick_history = []
    btc_price_lock = threading.Lock()
    MAX_HISTORY_SIZE = 200

    btc_trend_mode = 'DUAL'
    btc_trend_start_time = 0
    btc_trend_lock = threading.Lock()
    btc_trend_calc_state = {
        "last": "DUAL",
        "pending": None,
        "confirm": 0,
        "last_change_time": 0,
        "min_hold_time": 300
    }
    last_trend_update = 0
    trend_update_interval = 60

    last_que_trigger_key = None
    que_lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GlobalBTCManager, cls).__new__(cls)
        return cls._instance

    def update_btc_data(self):
        """每60秒刷新 BTC 3m K线收盘价"""
        now = time.time()
        if now - self.last_update_time < self.update_interval:
            return
        try:
            with self.data_lock:
                klines_3m = client.futures_klines(
                    symbol=BTC_SYMBOL,
                    interval=Client.KLINE_INTERVAL_3MINUTE,
                    limit=120
                )
                self.btc_3m_prices = [float(x[4]) for x in klines_3m]
                self.last_update_time = now
        except Exception as e:
            logging.error(f"[全局BTC数据] 更新失败: {e}")

    def _calc_ema(self, data, window):
        if not data:
            return []
        k = 2.0 / (window + 1)
        ema_values = [data[0]]
        for i in range(1, len(data)):
            ema_values.append(data[i] * k + ema_values[-1] * (1 - k))
        return ema_values

    def _calc_macd(self, prices, fast=12, slow=26, signal=9):
        if len(prices) < slow + signal:
            return None, None
        ema_fast = self._calc_ema(prices, fast)
        ema_slow = self._calc_ema(prices, slow)
        if len(ema_fast) != len(ema_slow):
            min_len = min(len(ema_fast), len(ema_slow))
            ema_fast = ema_fast[-min_len:]
            ema_slow = ema_slow[-min_len:]
        macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
        signal_line = self._calc_ema(macd_line, signal) if macd_line else []
        if not signal_line:
            return None, None
        return macd_line[-1], signal_line[-1]

    def _calc_rsi(self, prices, period=9):
        if len(prices) <= period:
            return None
        deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        for i in range(period, len(deltas)):
            gain = gains[i] if i < len(gains) else 0
            loss = losses[i] if i < len(losses) else 0
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period
            if avg_loss == 0:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
        return rsi

    def update_btc_trend(self):
        """独立计算 BTC 趋势，单一来源写入供 check_que 使用"""
        now = time.time()
        if now - self.last_trend_update < self.trend_update_interval:
            return
        self.last_trend_update = now

        with self.data_lock:
            prices = list(self.btc_3m_prices)
        if len(prices) < 90:
            return

        st = self.btc_trend_calc_state
        if now - st["last_change_time"] < st["min_hold_time"]:
            return

        close = prices[-1]
        ma7 = sum(prices[-7:]) / 7
        ma30 = sum(prices[-30:]) / 30
        ma90 = sum(prices[-90:]) / 90
        macd_val, signal_val = self._calc_macd(prices)
        if macd_val is None or signal_val is None:
            return

        recent = prices[-10:]
        amplitude = (max(recent) - min(recent)) / min(recent) if min(recent) > 0 else 0
        rsi = self._calc_rsi(prices)
        is_consolidation = (amplitude < 0.01 and rsi is not None and 40 <= rsi <= 60)

        if is_consolidation:
            strong_long = (ma7 > ma30 > ma90 and macd_val > signal_val
                           and close > ma30 * 1.01 and macd_val > 0.01)
            strong_short = (ma7 < ma30 < ma90 and macd_val < signal_val
                            and close < ma30 * 0.99 and macd_val < -0.01)
        else:
            strong_long = (ma7 > ma30 > ma90 and macd_val > signal_val
                           and close > ma30 and macd_val > 0)
            strong_short = (ma7 < ma30 < ma90 and macd_val < signal_val
                            and close < ma30 and macd_val < 0)

        raw_mode = "LONG_ONLY" if strong_long else ("SHORT_ONLY" if strong_short else "DUAL")

        if is_consolidation and st["last"] != "DUAL":
            if st["last"] == "LONG_ONLY" and not (ma7 > ma30 and macd_val > signal_val):
                raw_mode = "DUAL"
            elif st["last"] == "SHORT_ONLY" and not (ma7 < ma30 and macd_val < signal_val):
                raw_mode = "DUAL"

        if raw_mode == st["last"]:
            st["pending"], st["confirm"] = None, 0
            new_mode = st["last"]
        else:
            N = 3 if is_consolidation else 2
            if raw_mode == st["pending"]:
                st["confirm"] += 1
            else:
                st["pending"], st["confirm"] = raw_mode, 1

            if raw_mode != "DUAL" and st["confirm"] >= N:
                st["last"] = raw_mode
                st["pending"], st["confirm"] = None, 0
                st["last_change_time"] = now
                new_mode = raw_mode
            else:
                new_mode = st["last"]

        with self.btc_trend_lock:
            if new_mode != self.btc_trend_mode:
                old = self.btc_trend_mode
                self.btc_trend_mode = new_mode
                self.btc_trend_start_time = now
                logging.info(f"[BTC趋势] {old} → {new_mode}，计时重置")
            elif self.btc_trend_start_time == 0:
                self.btc_trend_start_time = now

    def get_trend_state(self):
        with self.btc_trend_lock:
            return self.btc_trend_mode, self.btc_trend_start_time

    def try_acquire_que_trigger(self, trigger_key):
        """同一 trigger_key 只返回一次 True"""
        with self.que_lock:
            if self.last_que_trigger_key == trigger_key:
                return False
            self.last_que_trigger_key = trigger_key
            return True

    def release_que_trigger(self):
        with self.que_lock:
            self.last_que_trigger_key = None

    def update_btc_price(self, price, timestamp):
        """WebSocket tick 更新（仅保留最近10秒）"""
        with self.btc_price_lock:
            if self.btc_tick_history and self.btc_tick_history[-1][0] == timestamp:
                return
            self.btc_tick_history.append((timestamp, price))
            self.btc_tick_history = [
                (t, p) for t, p in self.btc_tick_history if timestamp - t <= 10
            ]
            if len(self.btc_tick_history) > self.MAX_HISTORY_SIZE:
                self.btc_tick_history = self.btc_tick_history[-self.MAX_HISTORY_SIZE:]

    def check_2s_spike(self):
        """检测2秒内 BTC 涨跌幅 ≥ 0.18%"""
        with self.btc_price_lock:
            now = time.time()
            recent_ticks = [(t, p) for t, p in self.btc_tick_history if now - t <= 10]
            if len(recent_ticks) < 2:
                return None
            two_sec_ticks = [(t, p) for t, p in recent_ticks if now - t <= 2]
            if len(two_sec_ticks) < 2:
                return None

            prices = [p for _, p in two_sec_ticks]
            max_price = max(prices)
            min_price = min(prices)
            base_price = two_sec_ticks[0][1]
            if base_price <= 0:
                return None

            diff_pct = (max_price - min_price) / base_price
            if diff_pct >= 0.0018:
                first_price = two_sec_ticks[0][1]
                last_price = two_sec_ticks[-1][1]
                return 'SPIKE_UP' if last_price > first_price else 'SPIKE_DOWN'
            return None

    def get_btc_3m_prices(self):
        with self.data_lock:
            return self.btc_3m_prices.copy() if self.btc_3m_prices else []


# =============================================================================
# 模块 7：全局 SPY 数据管理器（单例）
# =============================================================================

class GlobalSPYManager:
    """SPY 1m 信号 + 3m 趋势数据源"""

    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GlobalSPYManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not GlobalSPYManager._initialized:
            self.spy_1m_prices = []
            self.spy_1m_volumes = []
            self.spy_avg_volume = None
            self.last_kline_update = 0
            self.kline_update_interval = 60
            self.data_lock = threading.Lock()

            self.spy_3m_closes = []
            self.last_3m_update = 0
            self.kline_3m_update_interval = 60

            GlobalSPYManager._initialized = True

    def update_spy_1m_data(self):
        now = time.time()
        if now - self.last_kline_update < self.kline_update_interval:
            return
        try:
            klines = client.futures_klines(
                symbol=SPY_SYMBOL,
                interval=Client.KLINE_INTERVAL_1MINUTE,
                limit=25
            )
            if not klines:
                return
            with self.data_lock:
                self.spy_1m_prices = [float(k[4]) for k in klines]
                self.spy_1m_volumes = [float(k[5]) for k in klines]
                if len(self.spy_1m_volumes) >= 20:
                    self.spy_avg_volume = sum(self.spy_1m_volumes[-20:]) / 20
                else:
                    self.spy_avg_volume = (
                        sum(self.spy_1m_volumes) / len(self.spy_1m_volumes)
                        if self.spy_1m_volumes else None
                    )
                self.last_kline_update = now
        except Exception as e:
            logging.error(f"[SPY K线更新] 失败: {e}")

    def check_spy_1m_signal(self):
        """1m 涨跌幅 ≥ 0.06% 且成交量 ≥ 3 倍均值 → 强信号"""
        self.update_spy_1m_data()
        with self.data_lock:
            if len(self.spy_1m_prices) < 2 or self.spy_avg_volume is None or self.spy_avg_volume <= 0:
                logging.debug("[SPY信号] 数据不足，跳过")
                return None
            price_change_pct = (
                (self.spy_1m_prices[-1] - self.spy_1m_prices[-2]) / self.spy_1m_prices[-2]
            )
            current_volume = self.spy_1m_volumes[-1] if self.spy_1m_volumes else 0
            volume_ratio = current_volume / self.spy_avg_volume if self.spy_avg_volume else 0
            volume_surge = volume_ratio >= 3.0

            if price_change_pct >= 0.0006 and volume_surge:
                return 'STRONG_UP'
            elif price_change_pct <= -0.0006 and volume_surge:
                return 'STRONG_DOWN'
            return None

    def update_spy_3m_data(self):
        now = time.time()
        if now - self.last_3m_update < self.kline_3m_update_interval:
            return
        try:
            klines = client.futures_klines(
                symbol=SPY_SYMBOL,
                interval=Client.KLINE_INTERVAL_3MINUTE,
                limit=150
            )
            if not klines:
                return
            with self.data_lock:
                self.spy_3m_closes = [float(k[4]) for k in klines]
                self.last_3m_update = now
        except Exception as e:
            logging.error(f"[SPY 3m K线更新] 失败: {e}")

    def get_spy_3m_closes(self):
        with self.data_lock:
            return self.spy_3m_closes.copy() if self.spy_3m_closes else []


global_btc_manager = GlobalBTCManager()
global_spy_manager = GlobalSPYManager()

sentiment_analyzer = MarketSentimentAnalyzer()
correlation_analyzer = CorrelationAnalyzer()
global_risk_manager = DynamicRiskManager()


# =============================================================================
# 模块 8：SymbolLogger
# =============================================================================

class SymbolLogger:
    """给日志记录加 symbol 标识"""

    def __init__(self, symbol):
        self.symbol = symbol

    def info(self, msg):
        logging.info(msg, extra={'symbol': self.symbol})

    def error(self, msg):
        logging.error(msg, extra={'symbol': self.symbol})

    def warning(self, msg):
        logging.warning(msg, extra={'symbol': self.symbol})

    def debug(self, msg):
        logging.debug(msg, extra={'symbol': self.symbol})


# =============================================================================
# 模块 9：TradingBot
# =============================================================================

class TradingBot:
    """单交易对交易机器人：主动（SOL）与助理（HYPE/BNB/XRP）共用"""

    def __init__(self, symbol, btc_symbol='BTCUSDT'):
        self.symbol = symbol
        self.btc_symbol = btc_symbol
        self.config = TRADING_CONFIG.get(symbol, {
            'leverage': 10,
            'stop_loss_pct': 0.06,
            'take_profit_pct': 0.04,
            'position_percentage': 0.02,
            'min_position_qty': 0.005,
            'price_check_interval': 2
        })
        self.logger = SymbolLogger(symbol)
        self.leverage = self.config['leverage']

        self.current_position = {
            "LONG": {
                "entry_price": None, "quantity": 0, "margin": 0,
                "stop_price": None, "take_profit_price": None,
                "stop_updated": False, "open_time": None
            },
            "SHORT": {
                "entry_price": None, "quantity": 0, "margin": 0,
                "stop_price": None, "take_profit_price": None,
                "stop_updated": False, "open_time": None
            }
        }

        self.last_clear_time = {"LONG": None, "SHORT": None}
        self.ban_period = 18 * 60

        self.market_data = {"price": None}
        self.trend_mode = 'DUAL'

        self.local_risk_manager = DynamicRiskManager()

        self.active_trading = False
        self._last_spy_signal_time = 0
        self._last_fen_check = 0
        self._delayed_timers = {}
        self._close_timer = {}
        self._close_pending = {'LONG': False, 'SHORT': False}
        self._last_jia_check = 0
        self._spy_trend_mode = None
        self._last_jia_trigger = 0
        self._jia_ban_period = 30 * 60
        self._last_que_check = 0

    def initialize(self):
        try:
            client.futures_change_leverage(symbol=self.symbol, leverage=self.leverage)
            self.logger.info(f"初始化成功，杠杆设置为{self.leverage}")
            if self.symbol == 'SOLUSDT':
                self.active_trading = True
            return True
        except Exception as e:
            self.logger.error(f"初始化失败: {e}")
            return False

    def symbol_ws_callback(self, msg):
        """交易对 ticker WebSocket 回调"""
        try:
            if 'e' in msg and msg['e'] == 'error':
                self.logger.warning(f"WebSocket错误消息: {msg}")
                return
            if 'c' in msg:
                price = float(msg['c'])
                self.market_data['price'] = price
                correlation_analyzer.update_price(self.symbol, price)
            else:
                self.logger.warning(f"WebSocket消息缺少价格字段: {msg}")
        except Exception as e:
            self.logger.error(f"WebSocket异常: {e}, 消息: {msg}")

    # ===== 技术指标 =====

    def calculate_rsi(self, prices, period=9):
        if len(prices) <= period:
            return None
        deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        for i in range(period, len(deltas)):
            gain = gains[i] if i < len(gains) else 0
            loss = losses[i] if i < len(losses) else 0
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period
            if avg_loss == 0:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
        return rsi

    def get_ma(self, prices, n):
        if len(prices) < n:
            return None
        return sum(prices[-n:]) / n

    def get_ma_signal(self, prices, short_window=10, long_window=30, min_ma_diff_pct=0.0005):
        if len(prices) < max(long_window + 1, 50):
            return 'NONE'

        ma_short_prev = self.get_ma(prices[:-1], short_window)
        ma_long_prev = self.get_ma(prices[:-1], long_window)
        ma_short_now = self.get_ma(prices, short_window)
        ma_long_now = self.get_ma(prices, long_window)

        if None in [ma_short_prev, ma_long_prev, ma_short_now, ma_long_now]:
            return 'NONE'

        ma_cross_up = ma_short_prev < ma_long_prev and ma_short_now > ma_long_now
        ma_cross_down = ma_short_prev > ma_long_prev and ma_short_now < ma_long_now
        ma_distance_ok = abs(ma_short_now - ma_long_now) / ma_long_now >= min_ma_diff_pct

        macd, signal, _ = self.calculate_macd(prices)
        if macd is None or signal is None:
            return 'NONE'

        if ma_cross_up and ma_distance_ok and macd > signal:
            return 'UP'
        elif ma_cross_down and ma_distance_ok and macd < signal:
            return 'DOWN'
        elif macd > signal:
            return 'UP'
        elif macd < signal:
            return 'DOWN'
        return 'NONE'

    def calculate_macd(self, prices, fast=12, slow=26, signal=9):
        if len(prices) < slow + signal:
            return None, None, None

        def ema(data, window):
            if not data:
                return []
            k = 2.0 / (window + 1)
            ema_values = [data[0]]
            for i in range(1, len(data)):
                ema_values.append(data[i] * k + ema_values[-1] * (1 - k))
            return ema_values

        ema_fast = ema(prices, fast)
        ema_slow = ema(prices, slow)
        if len(ema_fast) != len(ema_slow):
            min_len = min(len(ema_fast), len(ema_slow))
            ema_fast = ema_fast[-min_len:]
            ema_slow = ema_slow[-min_len:]

        macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
        signal_line = ema(macd_line, signal) if macd_line else []
        hist = [m - s for m, s in zip(macd_line[-len(signal_line):], signal_line)] if signal_line else []

        return (
            macd_line[-1] if macd_line else None,
            signal_line[-1] if signal_line else None,
            hist[-1] if hist else None
        )

    def calculate_rsi_safe(self, prices, period=9, min_data_length=10):
        if len(prices) < min_data_length:
            return None
        return self.calculate_rsi(prices, period)

    # ===== 趋势判断 =====

    def get_trend_mode(self):
        """BTC 趋势模式（LONG_ONLY / SHORT_ONLY / DUAL），每 bot 独立延迟确认"""
        if not hasattr(self, "_trend_state"):
            self._trend_state = {
                "last": "DUAL", "pending": None, "confirm": 0,
                "last_change_time": 0, "min_hold_time": 300
            }
        st = self._trend_state
        now = time.time()
        if now - st["last_change_time"] < st["min_hold_time"]:
            return st["last"]

        btc_3m_prices = global_btc_manager.get_btc_3m_prices()
        if not btc_3m_prices or len(btc_3m_prices) < 50:
            return st["last"]

        close = btc_3m_prices[-1]
        ma7 = self.get_ma(btc_3m_prices, 7)
        ma30 = self.get_ma(btc_3m_prices, 30)
        ma90 = self.get_ma(btc_3m_prices, 90)
        macd, signal, _ = self.calculate_macd(btc_3m_prices)
        if any(x is None for x in (ma7, ma30, ma90, macd, signal)):
            return st["last"]

        recent_prices = btc_3m_prices[-10:]
        amplitude = (max(recent_prices) - min(recent_prices)) / min(recent_prices) if min(recent_prices) > 0 else 0
        rsi = self.calculate_rsi_safe(btc_3m_prices)
        is_consolidation = (amplitude < 0.01 and rsi is not None and 40 <= rsi <= 60)

        if is_consolidation:
            strong_long = (ma7 > ma30 > ma90 and macd > signal and close > ma30 * 1.01 and macd > 0.01)
            strong_short = (ma7 < ma30 < ma90 and macd < signal and close < ma30 * 0.99 and macd < -0.01)
        else:
            strong_long = (ma7 > ma30 > ma90 and macd > signal and close > ma30 and macd > 0)
            strong_short = (ma7 < ma30 < ma90 and macd < signal and close < ma30 and macd < 0)
        raw_mode = "LONG_ONLY" if strong_long else ("SHORT_ONLY" if strong_short else "DUAL")

        if is_consolidation and st["last"] != "DUAL":
            if st["last"] == "LONG_ONLY" and not (ma7 > ma30 and macd > signal):
                raw_mode = "DUAL"
            elif st["last"] == "SHORT_ONLY" and not (ma7 < ma30 and macd < signal):
                raw_mode = "DUAL"

        if raw_mode == st["last"]:
            st["pending"], st["confirm"] = None, 0
            return st["last"]

        N = 3 if is_consolidation else 2
        if raw_mode == st["pending"]:
            st["confirm"] += 1
        else:
            st["pending"], st["confirm"] = raw_mode, 1

        if raw_mode != "DUAL" and st["confirm"] >= N:
            st["last"], st["pending"], st["confirm"] = raw_mode, None, 0
            st["last_change_time"] = now
            return st["last"]
        return st["last"]

    def get_spy_trend_mode(self):
        """SPY 趋势模式（LONG_SPY / SHORT_SPY / DUAL）"""
        if not hasattr(self, "_spy_trend_state"):
            self._spy_trend_state = {
                "last": "DUAL", "pending": None, "confirm": 0,
                "last_change_time": 0, "min_hold_time": 300
            }
        st = self._spy_trend_state
        now = time.time()
        if now - st["last_change_time"] < st["min_hold_time"]:
            return st["last"]

        spy_3m = global_spy_manager.get_spy_3m_closes()
        if not spy_3m or len(spy_3m) < 50:
            return st["last"]

        close = spy_3m[-1]
        ma7 = self.get_ma(spy_3m, 7)
        ma30 = self.get_ma(spy_3m, 30)
        ma90 = self.get_ma(spy_3m, 90)
        macd, signal, _ = self.calculate_macd(spy_3m)
        if any(x is None for x in (ma7, ma30, ma90, macd, signal)):
            return st["last"]

        recent = spy_3m[-10:]
        amplitude = (max(recent) - min(recent)) / min(recent) if min(recent) > 0 else 0
        rsi = self.calculate_rsi_safe(spy_3m)
        is_consolidation = (amplitude < 0.01 and rsi is not None and 40 <= rsi <= 60)

        if is_consolidation:
            strong_long = (ma7 > ma30 > ma90 and macd > signal and close > ma30 * 1.01 and macd > 0.01)
            strong_short = (ma7 < ma30 < ma90 and macd < signal and close < ma30 * 0.99 and macd < -0.01)
        else:
            strong_long = (ma7 > ma30 > ma90 and macd > signal and close > ma30 and macd > 0)
            strong_short = (ma7 < ma30 < ma90 and macd < signal and close < ma30 and macd < 0)
        raw_mode = "LONG_SPY" if strong_long else ("SHORT_SPY" if strong_short else "DUAL")

        if is_consolidation and st["last"] != "DUAL":
            if st["last"] == "LONG_SPY" and not (ma7 > ma30 and macd > signal):
                raw_mode = "DUAL"
            elif st["last"] == "SHORT_SPY" and not (ma7 < ma30 and macd < signal):
                raw_mode = "DUAL"

        if raw_mode == st["last"]:
            st["pending"], st["confirm"] = None, 0
            return st["last"]

        N = 3 if is_consolidation else 2
        if raw_mode == st["pending"]:
            st["confirm"] += 1
        else:
            st["pending"], st["confirm"] = raw_mode, 1

        if raw_mode != "DUAL" and st["confirm"] >= N:
            st["last"], st["pending"], st["confirm"] = raw_mode, None, 0
            st["last_change_time"] = now
            return st["last"]
        return st["last"]

    # ===== 增强策略主入口 =====

    def enhanced_trading_strategy(self):
        """每2秒被主循环调用一次：同步仓位、趋势检查、主动交易专属策略"""
        try:
            self.sync_position()
            self.update_market_cache()

            previous_trend_mode = self.trend_mode
            self.trend_mode = self.get_trend_mode()
            if self.trend_mode != previous_trend_mode:
                self.logger.info(f"[趋势切换] 从 {previous_trend_mode} 切换到 {self.trend_mode}")
                if self.trend_mode == 'LONG_ONLY':
                    self.close_position_by_ban('SHORT')
                elif self.trend_mode == 'SHORT_ONLY':
                    self.close_position_by_ban('LONG')

            previous_spy_trend = getattr(self, '_spy_trend_mode', 'DUAL')
            self._spy_trend_mode = self.get_spy_trend_mode()
            if self._spy_trend_mode != previous_spy_trend:
                self.logger.info(f"[SPY趋势切换] 从 {previous_spy_trend} 切换到 {self._spy_trend_mode}")
                if self._spy_trend_mode == 'LONG_SPY':
                    self.close_position_by_ban('SHORT')
                elif self._spy_trend_mode == 'SHORT_SPY':
                    self.close_position_by_ban('LONG')

            for side in ['LONG', 'SHORT']:
                self.check_stop_conditions(side)

            if self.active_trading:
                spike_type = global_btc_manager.check_2s_spike()
                if spike_type == 'SPIKE_UP':
                    self.handle_btc_spike_up_for_all(trading_bots)
                elif spike_type == 'SPIKE_DOWN':
                    self.handle_btc_spike_down_for_all(trading_bots)

                spy_signal = global_spy_manager.check_spy_1m_signal()
                if spy_signal in ('STRONG_UP', 'STRONG_DOWN'):
                    now = time.time()
                    if now - self._last_spy_signal_time < 60:
                        self.logger.debug(
                            f"[SPY信号冷却] 距上次处理 {now - self._last_spy_signal_time:.1f} 秒，跳过"
                        )
                    else:
                        self._last_spy_signal_time = now
                        if spy_signal == 'STRONG_UP':
                            self.handle_spy_up_for_all(trading_bots)
                        elif spy_signal == 'STRONG_DOWN':
                            self.handle_spy_down_for_all(trading_bots)

                self.check_fen(trading_bots)
                self.check_qiang(trading_bots)
                self.check_jia(trading_bots)
                self.check_he(trading_bots)
                self.check_xau(trading_bots)
                self.check_que()
                self.update_performance()
            else:
                # 助理 bot 仍参与 check_que（让 XRP bot 能驱动加仓）
                self.check_que()
                self.update_performance()

        except Exception as e:
            self.logger.error(f"[增强策略异常] {e}")

    def update_performance(self):
        try:
            total_value = 0.0
            for side in ['LONG', 'SHORT']:
                pos = self.current_position[side]
                if pos['quantity'] > 0 and pos['entry_price']:
                    cp = self.market_data['price'] or self.get_current_price()
                    if cp:
                        value = pos['quantity'] * cp if side == 'LONG' else pos['quantity'] * pos['entry_price']
                        total_value += value
            self.local_risk_manager.update_drawdown(total_value)
        except Exception as e:
            self.logger.debug(f"[绩效更新失败] {e}")

    # ===== 各策略分支 =====

    def handle_btc_spike_up_for_all(self, all_bots):
        """BTC 2秒暴涨：所有交易对平空仓，HYPE 开多仓0.04"""
        btc_3m_prices = global_btc_manager.get_btc_3m_prices()
        btc_trend = self.get_ma_signal(btc_3m_prices)
        if btc_trend != 'DOWN':
            for bot in all_bots:
                bot.close_position_by_ban('SHORT')
                if bot.symbol == 'HYPEUSDT':
                    bot.execute_open_order('LONG', 0.04)
                    self.logger.info("BTC 2秒暴涨：所有交易对平空仓，仅对HYPE开多仓")

    def handle_btc_spike_down_for_all(self, all_bots):
        """BTC 2秒暴跌：所有交易对平多仓，HYPE 开空仓0.04"""
        btc_3m_prices = global_btc_manager.get_btc_3m_prices()
        btc_trend = self.get_ma_signal(btc_3m_prices)
        if btc_trend != 'UP':
            for bot in all_bots:
                bot.close_position_by_ban('LONG')
                if bot.symbol == 'HYPEUSDT':
                    bot.execute_open_order('SHORT', 0.04)
                    self.logger.info("BTC 2秒暴跌：所有交易对平多仓，仅对HYPE开空仓")

    def handle_spy_up_for_all(self, all_bots):
        """SPY强势上涨：所有交易对平空仓，SOL 开多仓0.02"""
        for bot in all_bots:
            bot.close_position_by_ban('SHORT')
            if bot.symbol == 'SOLUSDT':
                bot.execute_open_order('LONG', 0.02)
                self.logger.info("SPY强势上涨：对所有交易对-平空仓-SOL开多")

    def handle_spy_down_for_all(self, all_bots):
        """SPY强势下跌：所有交易对平多仓，SOL 开空仓0.02"""
        for bot in all_bots:
            bot.close_position_by_ban('LONG')
            if bot.symbol == 'SOLUSDT':
                bot.execute_open_order('SHORT', 0.02)
                self.logger.info("SPY强势下跌：对所有交易对-平多仓-SOL开空")

    def check_fen(self, all_bots):
        """BTC 1m 分型：形态+放量3~8倍开同向；>8倍极端放量延时反向"""
        now = time.time()
        if now - self._last_fen_check < 60:
            return
        try:
            klines_1m = client.futures_klines(
                symbol=self.btc_symbol,
                interval=Client.KLINE_INTERVAL_1MINUTE,
                limit=60
            )
            if len(klines_1m) < 30:
                self.logger.debug("[check_fen] BTC 1分钟K线不足30根，跳过")
                return

            closes = [float(k[4]) for k in klines_1m]
            volumes = [float(k[5]) for k in klines_1m]

            current_volume = volumes[-1]
            avg_volume = sum(volumes[:-1]) / (len(volumes) - 1) if len(volumes) > 1 else 0
            if avg_volume <= 0:
                return
            volume_ratio = current_volume / avg_volume

            close = closes[-1]
            ma7 = sum(closes[-7:]) / 7
            ma30 = sum(closes[-30:]) / 30
            macd, signal, hist = self.calculate_macd(closes)
            if macd is None or signal is None or hist is None:
                return

            up_condition = (close > ma7 > ma30 and macd > signal and hist > 0
                            and volume_ratio > 3.0 and close >= max(closes[-10:]))
            down_condition = (close < ma7 < ma30 and macd < signal and hist < 0
                              and volume_ratio > 3.0 and close <= min(closes[-10:]))

            active_bots = [bot for bot in all_bots if bot.active_trading]
            if not active_bots:
                return

            if up_condition:
                self._last_fen_check = now
                if volume_ratio > 8.0:
                    self.close('LONG')
                    self.logger.info(f"[up极端放量] BTC放量{volume_ratio:.1f}倍（>8倍），延时开空")
                else:
                    self.logger.info(f"[check_fen] BTC上涨形态触发，放量{volume_ratio:.1f}倍（3~8倍），开多")
                    self.close_position_by_ban('SHORT')
                    if self.active_trading:
                        self.execute_open_order('LONG', 0.02, delay=1)
            elif down_condition:
                self._last_fen_check = now
                if volume_ratio > 8.0:
                    self.close('SHORT')
                    self.logger.info(f"[down极端放量] BTC放量{volume_ratio:.1f}倍（>8倍），延时开多")
                else:
                    self.logger.info(f"[check_fen] BTC下跌形态触发，放量{volume_ratio:.1f}倍（3~8倍），开空")
                    self.close_position_by_ban('LONG')
                    if self.active_trading:
                        self.execute_open_order('SHORT', 0.02, delay=1)
            else:
                self.logger.debug(f"[check_fen] 未触发，形态不满足或放量{volume_ratio:.1f}倍不足3倍")
        except Exception as e:
            self.logger.error(f"[check_fen] 执行异常: {e}")

    def get_btc_30d_breakout(self):
        """基于 BTC 日线判断 30 日新高/新低（用已收盘前30根）"""
        try:
            klines = client.futures_klines(
                symbol=BTC_SYMBOL,
                interval=Client.KLINE_INTERVAL_1DAY,
                limit=31
            )
            if not klines or len(klines) < 31:
                self.logger.debug("[BTC 30日突破] 日线数据不足31根，返回NONE")
                return 'NONE'

            past_30 = klines[-31:-1]
            high_30 = max(float(k[2]) for k in past_30)
            low_30 = min(float(k[3]) for k in past_30)

            current_price = None
            try:
                ticker = client.futures_symbol_ticker(symbol=BTC_SYMBOL)
                current_price = float(ticker['price'])
            except Exception as e:
                self.logger.warning(f"[BTC 30日突破] 获取BTC实时价格失败: {e}")
            if not current_price or current_price <= 0:
                current_price = float(klines[-1][4])

            if current_price > high_30:
                self.logger.debug(f"[BTC 30日突破] 新高 current={current_price:.2f} > 30dHigh={high_30:.2f}")
                return 'NEW_HIGH'
            elif current_price < low_30:
                self.logger.debug(f"[BTC 30日突破] 新低 current={current_price:.2f} < 30dLow={low_30:.2f}")
                return 'NEW_LOW'
            else:
                self.logger.debug(
                    f"[BTC 30日突破] 未突破 current={current_price:.2f} "
                    f"(30dHigh={high_30:.2f}, 30dLow={low_30:.2f})"
                )
                return 'NONE'
        except Exception as e:
            self.logger.error(f"[BTC 30日突破] 计算失败: {e}")
            return 'NONE'

    def check_qiang(self, all_bots):
        """BTC 15m 强势突破 + 8倍放量紧急开仓；XRP 需 BTC 30日新高/新低过滤"""
        now = time.time()
        if now - getattr(self, '_last_qiang_time', 0) < 60:
            return
        self._last_qiang_time = now

        try:
            klines_15m = client.futures_klines(
                symbol=BTC_SYMBOL,
                interval=Client.KLINE_INTERVAL_15MINUTE,
                limit=50
            )
            if len(klines_15m) < 30:
                self.logger.debug("[check_qiang] BTC 15分钟K线不足30根，跳过")
                return

            closes_15 = [float(k[4]) for k in klines_15m]
            close = closes_15[-1]
            ma7_15 = sum(closes_15[-7:]) / 7
            ma30_15 = sum(closes_15[-30:]) / 30
            macd, signal, _ = self.calculate_macd(closes_15)
            if None in (ma7_15, ma30_15, macd, signal):
                return

            klines_3m = client.futures_klines(
                symbol=BTC_SYMBOL,
                interval=Client.KLINE_INTERVAL_3MINUTE,
                limit=30
            )
            if len(klines_3m) < 21:
                self.logger.debug("[check_qiang] BTC 3分钟K线不足21根，跳过")
                return

            volumes_3m = [float(k[5]) for k in klines_3m]
            current_vol = volumes_3m[-1]
            avg_vol = sum(volumes_3m[-21:-1]) / 20
            if avg_vol <= 0:
                return
            vol_ratio = current_vol / avg_vol

            up_condition = (close > ma7_15 > ma30_15 and macd > signal
                            and close >= max(closes_15[-10:]) and vol_ratio >= 8)
            down_condition = (close < ma7_15 < ma30_15 and macd < signal
                              and close <= min(closes_15[-10:]) and vol_ratio >= 8)

            btc_30d = self.get_btc_30d_breakout()
            allow_long = (btc_30d == 'NEW_HIGH')
            allow_short = (btc_30d == 'NEW_LOW')

            if up_condition:
                self.logger.info(
                    f"[check_qiang] 强势上涨触发（15分钟突破，放量{vol_ratio:.1f}倍），"
                    f"BTC 30日状态={btc_30d}"
                )
                if not allow_long:
                    self.logger.info("[check_qiang] BTC未创30日新高，XRP不开多；执行平空")
                for bot in all_bots:
                    bot.close('SHORT')
                    if bot.symbol == 'BNBUSDT':
                        bot.execute_open_order('LONG', 0.02)
                    if not allow_long:
                        continue
                    if bot.symbol == 'XRPUSDT':
                        bot.execute_open_order('LONG', 0.04)
            elif down_condition:
                self.logger.info(
                    f"[check_qiang] 强势下跌触发（15分钟跌破，放量{vol_ratio:.1f}倍），"
                    f"BTC 30日状态={btc_30d}"
                )
                if not allow_short:
                    self.logger.info("[check_qiang] BTC未创30日新低，XRP不开空；执行平多")
                for bot in all_bots:
                    bot.close('LONG')
                    if bot.symbol == 'BNBUSDT':
                        bot.execute_open_order('SHORT', 0.02)
                    if not allow_short:
                        continue
                    if bot.symbol == 'XRPUSDT':
                        bot.execute_open_order('SHORT', 0.04)
            else:
                self.logger.debug(f"[check_qiang] 未触发：vol_ratio={vol_ratio:.2f}, close={close:.2f}")
        except Exception as e:
            self.logger.error(f"[check_qiang] 执行异常: {e}")

    def check_jia(self, all_bots):
        """SOL 双向持仓，一边盈一边亏，盈方≥2%且仓位更小 → BNB 同向开仓0.02"""
        if self.symbol != 'SOLUSDT':
            return
        now = time.time()
        if now - self._last_jia_trigger < self._jia_ban_period:
            return
        if now - getattr(self, '_last_jia_check', 0) < 60:
            return
        self._last_jia_check = now

        try:
            self.sync_position()
            long_pos = self.current_position['LONG']
            short_pos = self.current_position['SHORT']
            if long_pos['quantity'] <= 0 or short_pos['quantity'] <= 0:
                return
            if not long_pos['entry_price'] or not short_pos['entry_price']:
                return

            cp = self.get_current_price()
            if not cp or cp <= 0:
                return

            long_profit = (cp - long_pos['entry_price']) / long_pos['entry_price']
            short_profit = (short_pos['entry_price'] - cp) / short_pos['entry_price']

            side = None
            if long_profit >= 0.02 and short_profit < 0:
                side = 'LONG'
            elif short_profit >= 0.02 and long_profit < 0:
                side = 'SHORT'
            if side is None:
                return

            if side == 'LONG' and long_pos['quantity'] >= short_pos['quantity']:
                self.logger.debug(
                    f"[check_jia] SOL LONG盈利 {long_profit:.2%}，但LONG仓位({long_pos['quantity']}) "
                    f">= SHORT亏损仓位({short_pos['quantity']})，跳过BNB加仓"
                )
                return
            if side == 'SHORT' and short_pos['quantity'] >= long_pos['quantity']:
                self.logger.debug(
                    f"[check_jia] SOL SHORT盈利 {short_profit:.2%}，但SHORT仓位({short_pos['quantity']}) "
                    f">= LONG亏损仓位({long_pos['quantity']})，跳过BNB加仓"
                )
                return

            bnb_bot = next((bot for bot in all_bots if bot.symbol == 'BNBUSDT'), None)
            if bnb_bot is None:
                self.logger.warning("[check_jia] 未找到 BNBUSDT 机器人，无法盈利加仓")
                return

            profit_value = long_profit if side == 'LONG' else short_profit
            self.logger.info(
                f"[check_jia] SOL双向持仓，{side}盈利 {profit_value:.2%}，"
                f"触发BNB同向开仓0.02"
            )
            bnb_bot.execute_open_order(side, 0.02)
            self._last_jia_trigger = now
            self.logger.info(f"[check_jia] 已触发，进入冷却 {self._jia_ban_period} 秒")
        except Exception as e:
            self.logger.error(f"[check_jia] 执行异常: {e}")

    def check_he(self, all_bots):
        """≥4仓（多空并存）且合计盈利 > 合计亏损×1.3 → 全部平仓"""
        now = time.time()
        if now - getattr(self, '_last_he_check', 0) < 60:
            return
        self._last_he_check = now

        try:
            total_profit = 0.0
            total_loss = 0.0
            position_count = 0
            has_long = False
            has_short = False
            details = []

            for bot in all_bots:
                try:
                    bot.sync_position()
                except Exception as e:
                    self.logger.error(f"[check_he] {bot.symbol} 同步仓位失败: {e}")
                    continue

                cp = bot.get_current_price()
                if not cp or cp <= 0:
                    continue

                for side in ['LONG', 'SHORT']:
                    pos = bot.current_position[side]
                    if pos['quantity'] <= 0 or not pos['entry_price']:
                        continue
                    position_count += 1

                    if side == 'LONG':
                        has_long = True
                        pnl = (cp - pos['entry_price']) * pos['quantity']
                    else:
                        has_short = True
                        pnl = (pos['entry_price'] - cp) * pos['quantity']

                    if pnl > 0:
                        total_profit += pnl
                        details.append(f"{bot.symbol}/{side}: +{pnl:.4f}")
                    elif pnl < 0:
                        total_loss += abs(pnl)
                        details.append(f"{bot.symbol}/{side}: -{abs(pnl):.4f}")

            if position_count < 4:
                return
            if not (has_long and has_short):
                return
            if total_profit <= 0:
                return

            if total_loss <= 0:
                ratio = float('inf')
                should_close = True
            else:
                ratio = total_profit / total_loss
                should_close = ratio > 1.3

            if not should_close:
                return

            self.logger.info(
                f"[check_he] 触发全部平仓！盈利合计={total_profit:.4f} USDT，"
                f"亏损合计={total_loss:.4f} USDT，比值={ratio:.2f} > 1.3，"
                f"持仓数={position_count} | 明细: {details}"
            )

            for bot in all_bots:
                try:
                    bot.close_all('LONG', reason='check_he全部平仓-合计盈利>合计亏损1.3倍')
                except Exception as e:
                    self.logger.error(f"[check_he] {bot.symbol} 平LONG失败: {e}")
                try:
                    bot.close_all('SHORT', reason='check_he全部平仓-合计盈利>合计亏损1.3倍')
                except Exception as e:
                    self.logger.error(f"[check_he] {bot.symbol} 平SHORT失败: {e}")
        except Exception as e:
            self.logger.error(f"[check_he] 执行异常: {e}")

    def check_xau(self, all_bots):
        """SPY 趋势切换时对 BNB 开仓0.02，并平反向"""
        target_symbols = ['BNBUSDT']
        if not hasattr(self, '_last_xau_spy_trend'):
            self._last_xau_spy_trend = None

        current_spy_trend = getattr(self, '_spy_trend_mode', 'DUAL')
        if current_spy_trend == self._last_xau_spy_trend:
            return
        self._last_xau_spy_trend = current_spy_trend

        if current_spy_trend == 'LONG_SPY':
            side, action_desc, opposite_side = 'LONG', '开多', 'SHORT'
        elif current_spy_trend == 'SHORT_SPY':
            side, action_desc, opposite_side = 'SHORT', '开空', 'LONG'
        else:
            return

        self.logger.info(
            f"[check_xau] SPY趋势切换为 {current_spy_trend}，"
            f"对 {target_symbols} {action_desc}0.02，并平反向 {opposite_side}"
        )

        for bot in all_bots:
            if bot.symbol in target_symbols:
                try:
                    bot.close(opposite_side, reason=f"SPY趋势切换为 {current_spy_trend}，平反向 {opposite_side}")
                    self.logger.info(f"[check_xau] {bot.symbol} 平反向 {opposite_side} 已触发")
                    bot.execute_open_order(side, 0.02)
                    self.logger.info(f"[check_xau] {bot.symbol} {action_desc}0.02 已触发")
                except Exception as e:
                    self.logger.error(f"[check_xau] {bot.symbol} 操作失败: {e}")

    def check_que(self):
        """BTC 趋势持续 ≥8h 且 8h 价格同向确认 → XRP 加仓0.02（每趋势实例一次）"""
        if self.symbol != 'XRPUSDT':
            return

        now = time.time()
        if now - self._last_que_check < 60:
            return
        self._last_que_check = now

        btc_trend_mode, btc_trend_start_time = global_btc_manager.get_trend_state()
        if btc_trend_mode == 'DUAL' or btc_trend_start_time <= 0:
            return

        duration = now - btc_trend_start_time
        if duration < 8 * 3600:
            return

        try:
            klines = client.futures_klines(
                symbol=BTC_SYMBOL,
                interval=Client.KLINE_INTERVAL_1HOUR,
                limit=9
            )
            if not klines or len(klines) < 9:
                return
            price_8h_ago = float(klines[0][4])
            current_price = float(klines[-1][4])
        except Exception as e:
            self.logger.error(f"[check_que] 获取BTC 1h K线失败: {e}")
            return

        if price_8h_ago <= 0 or current_price <= 0:
            return

        if btc_trend_mode == 'LONG_ONLY' and current_price > price_8h_ago * 1.005:
            side = 'LONG'
        elif btc_trend_mode == 'SHORT_ONLY' and current_price < price_8h_ago * 0.995:
            side = 'SHORT'
        else:
            self.logger.debug(
                f"[check_que] 趋势={btc_trend_mode} 持续={duration / 3600:.2f}h，"
                f"价格未同向确认（8h前={price_8h_ago:.2f}，当前={current_price:.2f}），跳过"
            )
            return

        bal = self.get_balance()
        if bal <= 0:
            return
        pos_margin = self.current_position[side]['margin']
        if pos_margin > bal * 0.02:
            self.logger.info(
                f"[check_que] XRP {side} 当前 margin={pos_margin:.2f} 已接近/超过 2% 上限 "
                f"({bal * 0.02:.2f})，跳过本轮"
            )
            return

        trigger_key = f"{btc_trend_mode}_{int(btc_trend_start_time)}"
        if not global_btc_manager.try_acquire_que_trigger(trigger_key):
            return

        self.logger.info(
            f"[check_que] BTC {btc_trend_mode} 已持续 {duration / 3600:.2f}h（≥8h），"
            f"8h前={price_8h_ago:.2f} → 当前={current_price:.2f}，XRP 加 {side} 仓 0.02"
        )
        try:
            self.execute_open_order(side, 0.02)
            self.logger.info(f"[check_que] XRP {side} 加仓 0.02 已触发")
        except Exception as e:
            self.logger.error(f"[check_que] XRP {side} 加仓失败: {e}")
            global_btc_manager.release_que_trigger()

    # ===== 平仓/对冲 =====

    def close_position_by_ban(self, side):
        """根据盈亏比执行对冲或减仓"""
        self.sync_position()
        bal = self.get_balance()
        if bal <= 0:
            self.logger.warning(f"[平仓失败] 余额无效: {bal}")
            return

        pos = self.current_position[side]
        if pos['quantity'] <= 0:
            return

        cp = self.market_data['price']
        entry = pos['entry_price']
        if not cp or not entry:
            return

        profit_ratio = (cp - entry) / entry if side == 'LONG' else (entry - cp) / entry

        if -0.3 < profit_ratio < -0.002:
            long_margin = self.current_position['LONG']['margin']
            short_margin = self.current_position['SHORT']['margin']
            if long_margin > bal * 0.04 and side == 'LONG':
                self.execute_open_order('SHORT', 0.03)
                self.logger.info("对冲-反向加空仓0.03")
                self.sync_position()
            if long_margin < bal * 0.04 and side == 'LONG':
                self.execute_open_order('SHORT', 0.02)
                self.logger.info("对冲-反向加空仓0.02")
                self.sync_position()
            if short_margin > bal * 0.04 and side == 'SHORT':
                self.execute_open_order('LONG', 0.03)
                self.logger.info("对冲-反向加多仓0.03")
                self.sync_position()
            if short_margin < bal * 0.04 and side == 'SHORT':
                self.execute_open_order('LONG', 0.02)
                self.logger.info("对冲-反向加多仓0.02")
                self.sync_position()
            return

        elif profit_ratio >= 0.01:
            long_qty = self.current_position['LONG']['quantity']
            short_qty = self.current_position['SHORT']['quantity']
            if not (long_qty > 0 and short_qty > 0):
                self.logger.debug(
                    f"[部分止盈跳过] {side} 单向持仓"
                    f"（LONG={long_qty}, SHORT={short_qty}）"
                )
                return

            try:
                reduce_ratio = 0.2
                close_qty = self._round_qty(pos['quantity'] * reduce_ratio)
                if close_qty <= 0:
                    self.logger.warning(
                        f"[盈利减仓20%] {side} 数量过小（{pos['quantity'] * reduce_ratio}），跳过"
                    )
                    return

                if side == 'LONG':
                    limit_price = self._round_price(cp + 0.001)
                    order_side = SIDE_SELL
                else:
                    limit_price = self._round_price(cp - 0.001)
                    order_side = SIDE_BUY

                try:
                    open_orders = client.futures_get_open_orders(symbol=self.symbol)
                    for order in open_orders:
                        if ('_reduce_' in order.get('clientOrderId', '')
                                and order.get('positionSide') == side):
                            self.logger.debug(f"[盈利减仓20%] {side} 方向已存在减仓挂单，跳过")
                            return
                except Exception as e:
                    self.logger.warning(f"[盈利减仓20%] 查询挂单失败: {e}")

                order = client.futures_create_order(
                    symbol=self.symbol,
                    side=order_side,
                    type=ORDER_TYPE_LIMIT,
                    timeInForce='GTC',
                    quantity=close_qty,
                    price=limit_price,
                    positionSide=side,
                    newClientOrderId=f"{self.symbol}_reduce_{side}_{int(time.time())}"
                )
                self.logger.info(
                    f"[盈利减仓20%] {side} 双向持仓限价挂单成功 数量={close_qty}，"
                    f"挂单价={limit_price:.4f}（现价{cp:.4f}，偏离0.001），"
                    f"盈利比例={profit_ratio:.4f}，订单ID={order['orderId']}"
                )
                time.sleep(1)
                self.sync_position()
            except Exception as e:
                self.logger.error(f"[盈利减仓20%失败 {side}]: {e}")
            return

    # ===== 工具方法 =====

    def _round_qty(self, qty):
        try:
            symbol_info = next(
                s for s in client.futures_exchange_info()['symbols']
                if s['symbol'] == self.symbol
            )
            step_size = float(next(
                f['stepSize'] for f in symbol_info['filters']
                if f['filterType'] == 'LOT_SIZE'
            ))
            min_qty = float(next(
                f['minQty'] for f in symbol_info['filters']
                if f['filterType'] == 'LOT_SIZE'
            ))
            qty = round(round(qty / step_size) * step_size, 8)
            return 0 if qty < min_qty else qty
        except Exception:
            return round(qty, 4)

    def _round_price(self, price):
        try:
            symbol_info = next(
                s for s in client.futures_exchange_info()['symbols']
                if s['symbol'] == self.symbol
            )
            tick_size = float(next(
                f['tickSize'] for f in symbol_info['filters']
                if f['filterType'] == 'PRICE_FILTER'
            ))
            return round(round(price / tick_size) * tick_size, 8)
        except Exception:
            return round(price, 2)

    def can_open_side(self, side):
        now = time.time()
        last_clear_time = self.last_clear_time.get(side)
        if last_clear_time is not None and now < last_clear_time:
            return False
        return True

    def get_current_price(self):
        if self.market_data['price'] is not None and self.market_data['price'] > 0:
            return self.market_data['price']
        try:
            price = float(client.futures_symbol_ticker(symbol=self.symbol)['price'])
            if price <= 0:
                self.logger.error(f"获取到无效价格: {price}")
                return None
            self.market_data['price'] = price
            return price
        except Exception as e:
            self.logger.error(f"获取价格失败: {e}")
            return None

    def update_market_cache(self):
        try:
            current_price = self.get_current_price()
            if current_price:
                self.market_data['price'] = current_price
        except Exception as e:
            self.logger.error(f"[行情缓存] 获取价格失败: {e}")
            self.market_data['price'] = None

    # ===== 仓位同步 =====

    def sync_position(self):
        """从交易所拉取持仓；处理仓位变化、加仓重置、归零反向挂单"""
        try:
            try:
                current_price = float(client.futures_symbol_ticker(symbol=self.symbol)['price'])
                if current_price > 0:
                    self.market_data['price'] = current_price
                    self.logger.debug(f"[价格同步] 更新市场价格: {current_price}")
                else:
                    self.logger.warning(f"[价格同步] 获取到无效价格: {current_price}，使用缓存价格")
            except Exception as price_error:
                self.logger.warning(
                    f"[价格同步] 获取实时价格失败: {price_error}，"
                    f"使用缓存价格: {self.market_data.get('price', '无')}"
                )

            positions = client.futures_position_information(symbol=self.symbol)

            new_position = {
                "LONG": {
                    "entry_price": None, "quantity": 0, "margin": 0,
                    "stop_price": None, "take_profit_price": None,
                    "stop_updated": False, "open_time": None
                },
                "SHORT": {
                    "entry_price": None, "quantity": 0, "margin": 0,
                    "stop_price": None, "take_profit_price": None,
                    "stop_updated": False, "open_time": None
                }
            }

            for pos in positions:
                amt = float(pos.get('positionAmt', 0))
                entry_price = float(pos.get('entryPrice', 0))
                margin = abs(amt) * entry_price / self.leverage if self.leverage > 0 else 0
                if abs(amt) <= 0:
                    continue

                side = 'LONG' if amt > 0 else 'SHORT'
                if entry_price == 0:
                    self.logger.warning(
                        f"[sync_position警告] {self.symbol} {side} 仓位存在但 entry_price=0，尝试使用上次记录的价格"
                    )
                    entry_price = self.current_position[side].get('entry_price', 0)
                    if entry_price == 0:
                        self.logger.error("[sync_position错误] 无法获取有效的入场价格，跳过该仓位")
                        continue

                new_position[side].update({
                    'entry_price': entry_price,
                    'quantity': abs(amt),
                    'margin': round(margin, 4)
                })

            for side in ['LONG', 'SHORT']:
                old_qty = self.current_position[side]['quantity']
                new_qty = new_position[side]['quantity']

                if old_qty > 0 and new_qty > 0 and new_qty < old_qty:
                    reduction_ratio = (old_qty - new_qty) / old_qty
                    if reduction_ratio >= 0.6:
                        self.logger.info("[减仓超60%]")

                if abs(old_qty - new_qty) > 0:
                    self.logger.info(f"[仓位变化] {side}仓位从 {old_qty} 变为 {new_qty}")

                if new_qty > 0:
                    if old_qty <= 0:
                        new_position[side]['open_time'] = time.time()
                        self.logger.info(
                            f"[新开仓位] {side} 开仓时间记录: "
                            f"{time.strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                    elif new_qty > old_qty:
                        new_position[side]['open_time'] = time.time()
                        increase_ratio = (new_qty - old_qty) / old_qty if old_qty > 0 else 0
                        if increase_ratio > 0.1:
                            new_position[side]['stop_updated'] = False
                            self.logger.info(
                                f"[加仓重置动态止损] {side} 加仓比例={increase_ratio:.2%}，允许再次设置动态止损"
                            )
                        self.logger.info(
                            f"[加仓重置时间] {side} 加仓比例={increase_ratio:.2%}，持仓时间重新计算"
                        )
                    else:
                        new_position[side]['open_time'] = self.current_position[side].get('open_time')
                        if new_qty < old_qty:
                            reduction_pct = (old_qty - new_qty) / old_qty if old_qty > 0 else 0
                            self.logger.debug(
                                f"[减仓保持时间] {side} 减仓{reduction_pct:.2%}，持仓时间保持不变"
                            )

                    if (self.current_position[side].get('stop_price') is not None
                            and self.current_position[side]['stop_price'] > 0):
                        new_position[side]['stop_price'] = self.current_position[side].get('stop_price')
                    else:
                        new_position[side]['stop_price'] = None

                    if (self.current_position[side].get('take_profit_price') is not None
                            and self.current_position[side]['take_profit_price'] > 0):
                        new_position[side]['take_profit_price'] = self.current_position[side].get('take_profit_price')
                    else:
                        new_position[side]['take_profit_price'] = None

                    if new_qty <= old_qty:
                        new_position[side]['stop_updated'] = self.current_position[side].get('stop_updated', False)
                else:
                    new_position[side]['open_time'] = None
                    new_position[side]['stop_price'] = None
                    new_position[side]['take_profit_price'] = None
                    new_position[side]['stop_updated'] = False

                if new_qty <= 0 and old_qty > 0:
                    self.logger.info(f"[仓位清零] {side}仓位归零，重置时间并撤销止损单")
                    self.last_clear_time[side] = time.time() + self.ban_period
                    opposite_side = 'LONG' if side == 'SHORT' else 'SHORT'
                    self.execute_open_order(opposite_side, 0.02, delay=180)
                    self.logger.info(
                        f"[仓位清零] {side}仓位归零，反向开仓{opposite_side}（延迟180秒）"
                    )

            self.current_position = new_position

            for side in ['LONG', 'SHORT']:
                pos = self.current_position[side]
                if pos['quantity'] > 0 and pos['entry_price']:
                    cp = self.get_current_price()
                    if cp and cp > 0:
                        loss_ratio = ((pos['entry_price'] - cp) / pos['entry_price']
                                      if side == 'LONG'
                                      else (cp - pos['entry_price']) / pos['entry_price'])
                        if loss_ratio >= 0.003:
                            self.logger.debug(f"[亏损监控] {side}仓亏损比例: {loss_ratio:.4f}")

            for side in ['LONG', 'SHORT']:
                if self.current_position[side]['open_time']:
                    hold_time = time.time() - self.current_position[side]['open_time']
                    if hold_time > 0:
                        self.logger.debug(f"[持仓时间] {side} 已持仓: {hold_time / 3600:.2f}小时")

            self.logger.debug(
                f"[仓位同步完成] LONG: {self.current_position['LONG']['quantity']}, "
                f"SHORT: {self.current_position['SHORT']['quantity']}"
            )
        except Exception as e:
            self.logger.error(f"持仓同步异常: {e}")
            self.logger.warning("同步异常，保持原有仓位数据")

    def get_balance(self):
        """USDT 可用余额（20秒缓存）"""
        now = time.time()
        if hasattr(self, '_bal_time') and now - self._bal_time < 20:
            return getattr(self, '_bal_value', 0)

        for attempt in range(3):
            try:
                balance = client.futures_account_balance()
                usdt_items = [item for item in balance if item.get("asset") == "USDT"]
                usdt_balance = usdt_items[0].get("availableBalance") if usdt_items else 0
                result = round(float(usdt_balance), 4) if usdt_balance else 0
                self._bal_time = now
                self._bal_value = result
                return result
            except Exception as e:
                if "Timeout" in str(e) or "-1007" in str(e):
                    if attempt < 2:
                        time.sleep(3)
                        continue
                self.logger.error(f"获取余额失败: {e}")
                return getattr(self, '_bal_value', 0)

    # ===== 开仓 =====

    def execute_open_order(self, side, percentage, delay=0):
        """市价开仓；margin 上限 3% 余额；delay 秒后执行"""
        if not self.can_open_side(side):
            return

        if delay > 0:
            if side in self._delayed_timers:
                self._delayed_timers[side].cancel()
                del self._delayed_timers[side]

            def delayed():
                try:
                    self.execute_open_order(side, percentage, delay=0)
                except Exception as e:
                    self.logger.error(f"[延时开仓执行失败] {e}")
                finally:
                    if side in self._delayed_timers:
                        del self._delayed_timers[side]

            timer = threading.Timer(delay, delayed)
            timer.daemon = True
            self._delayed_timers[side] = timer
            timer.start()
            return

        bal = self.get_balance()
        if bal <= 0:
            self.logger.warning(f"余额无效: {bal}")
            return

        long_margin = self.current_position['LONG']['margin']
        short_margin = self.current_position['SHORT']['margin']
        if long_margin > bal * 0.03 and side == 'LONG':
            return
        if short_margin > bal * 0.03 and side == 'SHORT':
            return

        price = self.get_current_price()
        if price is None or price <= 0:
            return

        try:
            symbol_info = next(
                s for s in client.futures_exchange_info()['symbols']
                if s['symbol'] == self.symbol
            )
            min_qty = float(next(
                f['minQty'] for f in symbol_info['filters']
                if f['filterType'] == 'LOT_SIZE'
            ))
            step_size = float(next(
                f['stepSize'] for f in symbol_info['filters']
                if f['filterType'] == 'LOT_SIZE'
            ))

            qty = bal * percentage * self.leverage / price
            qty = round(qty / step_size) * step_size
            qty = max(qty, min_qty)
            qty = self._round_qty(qty)
            if qty <= 0:
                self.logger.warning(f"[市价开仓] {side} 数量过小，跳过")
                return

            order_side = SIDE_BUY if side == 'LONG' else SIDE_SELL
            order = client.futures_create_order(
                symbol=self.symbol,
                side=order_side,
                type=ORDER_TYPE_MARKET,
                quantity=qty,
                positionSide=side,
                newClientOrderId=f"{self.symbol}_open_{side}_{int(time.time())}"
            )
            self.logger.info(
                f"[紧急市价开仓] {side} 数量={qty}，参考价={price:.4f}，订单ID: {order['orderId']}"
            )
            time.sleep(1)
            self.sync_position()
        except Exception as e:
            self.logger.error(f"紧急市价开仓失败: {e}")

    # ===== 止盈挂单（减仓50%）=====

    def close(self, side, reason=""):
        """限价止盈：减仓 50%，已存在 '_close_' 挂单则不重复下单"""
        self.sync_position()
        pos = self.current_position[side]
        if pos['quantity'] <= 0 or not pos['entry_price']:
            return

        cp = self.get_current_price()
        if not cp:
            return

        entry = pos['entry_price']
        profit_ratio = (cp - entry) / entry if side == 'LONG' else (entry - cp) / entry

        if profit_ratio > 0.003:
            tp_price = self._round_price(cp + 0.001) if side == 'LONG' else self._round_price(cp - 0.001)
            offset_desc = "盈利偏移0.001"
        else:
            tp_price = self._round_price(entry * (1 + 0.004)) if side == 'LONG' else self._round_price(entry * (1 - 0.004))
            offset_desc = "反弹至盈利0.004"

        qty = self._round_qty(pos['quantity'] * 0.5)
        if qty <= 0:
            self.logger.warning(
                f"[减仓50%止盈] {side} 减仓数量过小（原持仓 {pos['quantity']}），跳过挂单"
            )
            return

        order_side = SIDE_SELL if side == 'LONG' else SIDE_BUY

        try:
            open_orders = client.futures_get_open_orders(symbol=self.symbol)
            for order in open_orders:
                if '_close_' in order.get('clientOrderId', '') and order.get('positionSide') == side:
                    self._close_pending[side] = True
                    self.logger.debug(f"[减仓50%止盈] {side} 方向已存在止盈挂单，不允许再挂单")
                    return
        except Exception as e:
            self.logger.warning(f"[减仓50%止盈] 查询挂单失败，出于安全不挂单: {e}")
            return

        try:
            order = client.futures_create_order(
                symbol=self.symbol,
                side=order_side,
                type=ORDER_TYPE_LIMIT,
                timeInForce='GTC',
                quantity=qty,
                price=tp_price,
                positionSide=side,
                newClientOrderId=f"{self.symbol}_close_{side}_{int(time.time())}"
            )
            self.logger.info(
                f"[减仓50%止盈挂单] {side} 数量 {qty}（原持仓 {pos['quantity']}，减仓50%），"
                f"止盈价 {tp_price:.4f}（当前价 {cp:.4f}，{offset_desc}），原因: {reason}"
            )
            self._close_pending[side] = True
        except Exception as e:
            self.logger.error(f"[减仓50%止盈挂单失败] {side}: {e}")
            self._close_pending[side] = False
        finally:
            if side in self._close_timer:
                self._close_timer[side] = None

    # ===== 全部平仓 =====

    def close_all(self, side, reason=""):
        """市价平掉指定方向全部仓位，并清理该方向挂单；失败则限价兜底"""
        self.sync_position()

        try:
            open_orders = client.futures_get_open_orders(symbol=self.symbol)
            for order in open_orders:
                if order.get('positionSide') == side:
                    client.futures_cancel_order(symbol=self.symbol, orderId=order['orderId'])
                    self.logger.info(f"[close_all] 取消 {side} 方向挂单，订单ID: {order['orderId']}")
        except Exception as e:
            self.logger.warning(f"[close_all] 取消挂单失败: {e}")

        pos = self.current_position.get(side)
        if not pos or pos['quantity'] <= 0:
            self.logger.info(f"[close_all] {side} 无持仓，跳过")
            return

        qty = pos['quantity']
        entry_price = pos['entry_price']
        if not entry_price or entry_price <= 0:
            self.logger.error(f"[close_all] {side} 入场价无效，无法平仓")
            return

        order_side = SIDE_SELL if side == 'LONG' else SIDE_BUY
        qty_fmt = self._round_qty(qty)
        if qty_fmt <= 0:
            self.logger.warning(f"[close_all] {side} 数量过小（{qty}），无法下单")
            return

        max_retry = 3
        for attempt in range(max_retry):
            try:
                order = client.futures_create_order(
                    symbol=self.symbol,
                    side=order_side,
                    type=ORDER_TYPE_MARKET,
                    quantity=qty_fmt,
                    positionSide=side
                )
                self.logger.info(
                    f"[close_all] {side} 全部平仓成功，数量 {qty_fmt}，"
                    f"订单ID {order.get('orderId')}，原因: {reason}"
                )
                time.sleep(1)
                self.sync_position()
                try:
                    open_orders = client.futures_get_open_orders(symbol=self.symbol)
                    for order in open_orders:
                        if order.get('positionSide') == side:
                            client.futures_cancel_order(symbol=self.symbol, orderId=order['orderId'])
                            self.logger.debug(
                                f"[平仓清理] 取消订单 {order['orderId']} ({order.get('side')})"
                            )
                except Exception as e:
                    self.logger.warning(f"[平仓清理] 取消订单失败: {e}")
                return
            except Exception as e:
                self.logger.error(f"[close_all] 平仓尝试 {attempt + 1}/{max_retry} 失败: {e}")
                if attempt < max_retry - 1:
                    time.sleep(2)
                else:
                    self.logger.warning("[close_all] 市价单全部失败，尝试限价单兜底")
                    try:
                        current_price = self.get_current_price()
                        if not current_price:
                            raise ValueError("无有效价格")
                        limit_price = self._round_price(
                            current_price * 0.995 if side == 'LONG' else current_price * 1.005
                        )
                        order = client.futures_create_order(
                            symbol=self.symbol,
                            side=order_side,
                            type=ORDER_TYPE_LIMIT,
                            timeInForce='GTC',
                            quantity=qty_fmt,
                            price=limit_price,
                            positionSide=side
                        )
                        self.logger.info(f"[close_all] 限价单兜底平仓成功，订单ID {order.get('orderId')}")
                        time.sleep(1)
                        self.sync_position()
                        try:
                            open_orders = client.futures_get_open_orders(symbol=self.symbol)
                            for order in open_orders:
                                if order.get('positionSide') == side:
                                    client.futures_cancel_order(symbol=self.symbol, orderId=order['orderId'])
                        except Exception:
                            pass
                    except Exception as e2:
                        self.logger.error(f"[close_all] 限价单兜底也失败: {e2}，请手动处理")

    def check_stop_conditions(self, side):
        """盈利 ≥ 9% 时安排 8 秒后减仓50%止盈"""
        if self._close_pending.get(side, False):
            return

        pos = self.current_position[side]
        if pos['quantity'] <= 0 or not pos['entry_price']:
            return

        cp = self.market_data['price']
        if not cp:
            return

        entry = pos['entry_price']
        profit_ratio = (cp - entry) / entry if side == 'LONG' else (entry - cp) / entry

        if profit_ratio < 0.09:
            if side in self._close_timer and self._close_timer[side]:
                self._close_timer[side].cancel()
                self._close_timer[side] = None
            return

        if side in self._close_timer and self._close_timer[side] is not None:
            return
        timer = threading.Timer(8.0, self.close, args=[side])
        timer.daemon = True
        timer.start()
        self._close_timer[side] = timer
        self.logger.info(f"[延时平仓] 安排 {side} 8秒后止盈")


# =============================================================================
# 模块 10：全局函数与线程
# =============================================================================

def btc_ws_callback(msg):
    """BTC ticker WebSocket 回调，喂给 GlobalBTCManager"""
    try:
        if 'c' not in msg:
            return
        price = float(msg['c'])
        if price <= 0 or price > 1000000:
            return
        global_btc_manager.update_btc_price(price, time.time())
    except Exception as e:
        logging.error(f"BTC WebSocket异常: {e}")


def initialize_global_client():
    """初始化 Binance REST + WebSocket 客户端（含重试）"""
    global client, twm
    max_retries = 5
    retry_delay = 10

    for attempt in range(max_retries):
        try:
            if twm:
                try:
                    twm.stop()
                    time.sleep(2)
                except Exception:
                    pass
                twm = None

            client = Client(API_KEY, API_SECRET, requests_params={'timeout': 30})
            twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
            twm.start()
            for _ in range(10):
                if hasattr(twm, '_running') and twm._running:
                    break
                time.sleep(0.5)
            else:
                raise RuntimeError("WebSocket管理器启动超时")

            client.futures_ping()
            logging.info("全局客户端初始化成功")
            return True
        except Exception as e:
            logging.error(f"客户端初始化尝试 {attempt + 1}/{max_retries} 失败: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logging.error("客户端初始化最终失败")
                return False


def global_data_updater():
    """后台线程：每60秒刷新 BTC 3m 数据/趋势，SPY 1m/3m 数据"""
    while True:
        try:
            global_btc_manager.update_btc_data()
            global_btc_manager.update_btc_trend()
            global_spy_manager.update_spy_1m_data()
            global_spy_manager.update_spy_3m_data()
            time.sleep(60)
        except Exception as e:
            logging.error(f"全局数据更新异常: {e}")
            time.sleep(60)


def cleanup_old_logs(log_file='multi_bot.log', max_lines=1000):
    """主循环内定期截断日志"""
    try:
        if os.path.exists(log_file):
            with open(log_file, 'r', errors='ignore') as f:
                lines = f.readlines()
            if len(lines) > max_lines:
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.writelines(lines[-max_lines:])
                    f.write("\n=== 日志已清理 ===\n")
    except Exception as e:
        logging.error(f"日志清理失败: {e}")


def global_monitor():
    """后台线程：整点输出持仓报告/情绪报告，并触发情绪开仓"""
    while True:
        try:
            now = datetime.datetime.utcnow()
            if now.minute == 0 and now.second < 10:
                logging.info(f"[全局监控] 时间: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")

                for bot in trading_bots:
                    long_qty = bot.current_position['LONG']['quantity']
                    short_qty = bot.current_position['SHORT']['quantity']
                    long_margin = bot.current_position['LONG'].get('margin', 0)
                    short_margin = bot.current_position['SHORT'].get('margin', 0)
                    if long_qty > 0 or short_qty > 0:
                        logging.info(
                            f"[持仓报告] {bot.symbol}: LONG={long_qty:.4f}(保证金:{long_margin:.2f}), "
                            f"SHORT={short_qty:.4f}(保证金:{short_margin:.2f})"
                        )

                btc_prices = global_btc_manager.get_btc_3m_prices()
                if btc_prices:
                    logging.info(f"[BTC报告] 当前价格: {btc_prices[-1]:.2f}")

                btc_mode, btc_start = global_btc_manager.get_trend_state()
                if btc_mode != 'DUAL' and btc_start > 0:
                    dur_h = (time.time() - btc_start) / 3600
                    logging.info(f"[BTC趋势] {btc_mode} 已持续 {dur_h:.2f}h")

                for bot in trading_bots:
                    try:
                        trigger_key = now.strftime('%Y%m%d%H%M')
                        if getattr(bot, '_sentiment_trigger_key', None) == trigger_key:
                            continue

                        symbol_sentiment = sentiment_analyzer.get_sentiment_level(bot.symbol)
                        if symbol_sentiment == "SLIGHTLY_BULLISH":
                            bot.logger.info(f"[情绪触发] {bot.symbol} SLIGHTLY_BULLISH，开多仓 0.01")
                            bot.execute_open_order('LONG', 0.01)
                            bot._sentiment_trigger_key = trigger_key
                        elif symbol_sentiment == "SLIGHTLY_BEARISH":
                            bot.logger.info(f"[情绪触发] {bot.symbol} SLIGHTLY_BEARISH，开空仓 0.01")
                            bot.execute_open_order('SHORT', 0.01)
                            bot._sentiment_trigger_key = trigger_key
                    except Exception as e:
                        logging.error(f"[情绪触发] {bot.symbol} 开仓失败: {e}")

                for symbol in TRADING_SYMBOLS + ASSISTANT_SYMBOLS:
                    sentiment = sentiment_analyzer.get_sentiment_level(symbol)
                    logging.info(f"[情绪报告] {symbol}: {sentiment}")

            correlation_analyzer.update_correlation_matrix()
            time.sleep(10)
        except Exception as e:
            logging.error(f"[全局监控异常] {e}")
            time.sleep(60)


def emergency_stop():
    """紧急停止：市价平掉所有 bot 的所有方向仓位"""
    logging.warning("[紧急停止] 触发紧急停止，开始市价平仓")
    try:
        for bot in trading_bots:
            for side in ['LONG', 'SHORT']:
                try:
                    bot.close_all(side, reason='emergency_stop')
                except Exception as e:
                    logging.error(f"[紧急停止] {bot.symbol} 平{side}失败: {e}")
                time.sleep(1)
        logging.warning("[紧急停止] 所有仓位平仓指令已发送")
    except Exception as e:
        logging.error(f"[紧急停止失败] {e}")


def cleanup_all_orders_on_startup(bots):
    """重启时清理所有挂单，避免遗留旧挂单干扰"""
    total_cancelled = 0
    total_failed = 0

    for bot in bots:
        try:
            open_orders = client.futures_get_open_orders(symbol=bot.symbol)
        except Exception as e:
            logging.error(f"[启动清理] {bot.symbol} 查询挂单失败: {e}")
            continue

        if not open_orders:
            logging.info(f"[启动清理] {bot.symbol} 无挂单")
            continue

        for order in open_orders:
            order_id = order.get('orderId')
            cid = order.get('clientOrderId', 'N/A')
            side = order.get('side')
            pos_side = order.get('positionSide')
            qty = order.get('origQty')
            price = order.get('price')

            cancelled = False
            for attempt in range(3):
                try:
                    client.futures_cancel_order(symbol=bot.symbol, orderId=order_id)
                    cancelled = True
                    break
                except Exception as e:
                    err_str = str(e)
                    if '-2011' in err_str or 'Unknown order' in err_str:
                        cancelled = True
                        break
                    if attempt < 2:
                        time.sleep(1)
                    else:
                        logging.error(
                            f"[启动清理失败] {bot.symbol} 订单 {order_id} ({cid}) 撤销失败: {e}"
                        )

            if cancelled:
                total_cancelled += 1
                logging.info(
                    f"[启动清理] {bot.symbol} 撤销挂单 "
                    f"{cid} | {side} {pos_side} qty={qty} price={price}"
                )
            else:
                total_failed += 1

    logging.info(f"[启动清理] 完成：成功撤销 {total_cancelled} 个挂单，失败 {total_failed} 个")
    return total_cancelled, total_failed


def setup_signal_handlers():
    """安装 SIGINT/SIGTERM/SIGQUIT 优雅退出处理器"""
    def signal_handler(signum, frame):
        signame = (
            signal.Signals(signum).name
            if hasattr(signal.Signals, signum)
            else str(signum)
        )
        logging.info(f"[优雅关闭] 接收到信号 {signame}，准备关闭程序...")

        if 'twm' in globals() and twm:
            try:
                twm.stop()
                logging.info("WebSocket管理器已停止")
                time.sleep(2)
            except Exception as e:
                logging.warning(f"停止WebSocket管理器时出错: {e}")

        logging.info("[优雅关闭] 当前仓位状态:")
        for bot in trading_bots:
            long_qty = bot.current_position['LONG']['quantity']
            short_qty = bot.current_position['SHORT']['quantity']
            if long_qty > 0 or short_qty > 0:
                logging.info(f"  {bot.symbol}: LONG={long_qty:.4f}, SHORT={short_qty:.4f}")

        logging.info("[优雅关闭] 程序退出，仓位保持")
        exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGQUIT, signal_handler)
    logging.info("[信号处理器] 已安装优雅关闭处理器")


def daemon_restart():
    """异常时指数退避自动重启子进程（最多50次）"""
    restart_count = 0
    max_restarts = 50
    base_restart_delay = 10

    setup_signal_handlers()

    while restart_count < max_restarts:
        try:
            logging.info(f"启动交易程序 (重启次数: {restart_count})")
            process = subprocess.Popen(
                [sys.executable, __file__, '--child'],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True
            )
            for line in iter(process.stdout.readline, ''):
                print(line.strip())

            return_code = process.wait()
            logging.error(f"主程序退出，返回码: {return_code}")

            delay = min(base_restart_delay * (2 ** restart_count), 300)
            restart_count += 1

            if return_code == 0:
                logging.info("程序正常退出，不再重启")
                break
            elif return_code == 100:
                logging.info("程序请求不重启")
                break
            else:
                logging.warning(f"程序异常退出，{delay}秒后重启... (第{restart_count}次)")
                time.sleep(delay)
        except KeyboardInterrupt:
            logging.info("守护进程被用户中断")
            break
        except Exception as e:
            logging.error(f"守护进程异常: {e}")
            restart_count += 1
            delay = min(base_restart_delay * (2 ** restart_count), 300)
            logging.warning(f"{delay}秒后重启... (第{restart_count}次)")
            time.sleep(delay)

    if restart_count >= max_restarts:
        logging.error(f"达到最大重启次数({max_restarts})，停止重启")


def main_program():
    """主程序：初始化、启动各线程、主循环"""
    global trading_bots
    try:
        if not initialize_global_client():
            logging.error("全局客户端初始化失败")
            return 1

        data_thread = threading.Thread(target=global_data_updater, daemon=True)
        data_thread.start()
        logging.info("全局数据更新线程已启动")

        monitor_thread = threading.Thread(target=global_monitor, daemon=True)
        monitor_thread.start()
        logging.info("全局监控线程已启动")

        try:
            twm.start_symbol_ticker_socket(callback=btc_ws_callback, symbol=BTC_SYMBOL)
            logging.info("BTC WebSocket启动成功")
        except Exception as e:
            logging.error(f"BTC WebSocket启动失败: {e}")
            return 2

        all_bots = []
        for symbol in TRADING_SYMBOLS:
            bot = TradingBot(symbol)
            if bot.initialize():
                all_bots.append(bot)
                logging.info(f"主动交易机器人 {symbol} 初始化成功")
            else:
                logging.error(f"主动交易机器人 {symbol} 初始化失败")

        for symbol in ASSISTANT_SYMBOLS:
            bot = TradingBot(symbol)
            if bot.initialize():
                all_bots.append(bot)
                logging.info(f"助理机器人 {symbol} 初始化成功（被动模式）")
            else:
                logging.error(f"助理机器人 {symbol} 初始化失败")

        if not all_bots:
            logging.error("没有成功的交易机器人实例")
            return 3

        trading_bots = all_bots

        try:
            cleanup_all_orders_on_startup(trading_bots)
        except Exception as e:
            logging.error(f"[启动清理] 执行异常: {e}")

        for bot in trading_bots:
            try:
                twm.start_symbol_ticker_socket(callback=bot.symbol_ws_callback, symbol=bot.symbol)
                logging.info(f"{bot.symbol} WebSocket启动成功")
                time.sleep(0.5)
            except Exception as e:
                logging.error(f"{bot.symbol} WebSocket启动失败: {e}")

        logging.info(
            f"共启动 {len(trading_bots)} 个交易机器人"
            f"（{len(TRADING_SYMBOLS)} 主动 + {len(ASSISTANT_SYMBOLS)} 助理）"
        )

        setup_signal_handlers()

        health_check_counter = 0
        last_successful_operation = time.time()

        while True:
            try:
                if trading_bots:
                    from concurrent.futures import ThreadPoolExecutor, as_completed
                    with ThreadPoolExecutor(max_workers=min(len(trading_bots), 4)) as executor:
                        future_to_bot = {
                            executor.submit(bot.enhanced_trading_strategy): bot
                            for bot in trading_bots
                        }
                        for future in as_completed(future_to_bot.keys(), timeout=30):
                            bot = future_to_bot[future]
                            try:
                                future.result()
                            except Exception as e:
                                logging.error(f"{bot.symbol} 策略执行异常: {e}")

                last_successful_operation = time.time()
                health_check_counter += 1
                if health_check_counter >= 30:
                    cleanup_old_logs()
                    health_check_counter = 0

                if time.time() - last_successful_operation > 300:
                    logging.warning("长时间无成功操作，但已禁用WebSocket手动重启，依赖自动重连")
                    last_successful_operation = time.time()

                if global_risk_manager.should_stop_trading():
                    logging.error("[全局风险] 触发停止交易条件，程序退出")
                    emergency_stop()
                    return 100

                time.sleep(2)
            except TimeoutError:
                logging.warning("交易策略执行超时，继续下一轮")
            except Exception as e:
                logging.error(f"主监控循环异常: {e}")
                time.sleep(10)

    except Exception as e:
        logging.error(f"主程序异常: {e}")
        return 4
    finally:
        if 'twm' in globals() and twm:
            try:
                twm.stop()
                logging.info("WebSocket管理器已停止")
            except Exception as e:
                logging.error(f"停止WebSocket管理器时出错: {e}")
        logging.info("程序退出")
    return 0


# =============================================================================
# 主程序入口
# =============================================================================
if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--child':
        exit_code = main_program()
        sys.exit(exit_code)
    elif len(sys.argv) > 1 and sys.argv[1] == '--daemon':
        print("启动虎鲸策略 V1 守护进程...")
        print("程序日志将输出到 multi_bot.log")
        print("按 Ctrl+C 停止守护进程")
        daemon_restart()
    else:
        print("直接运行虎鲸策略 V1...")
        print("按 Ctrl+C 停止程序")
        exit_code = main_program()
        sys.exit(exit_code)