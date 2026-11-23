import os
import sys
import json
import math
import time
import signal
import logging
import logging.handlers
import datetime
import threading
import subprocess
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait

from binance.client import Client
from binance.enums import ORDER_TYPE_MARKET, SIDE_BUY, SIDE_SELL, ORDER_TYPE_LIMIT
from binance import ThreadedWebsocketManager

# ============================== 日志 ==============================

LOG_FILE = 'multi_bot.log'
LOG_KEEP_LINES = 300


def truncate_log(max_lines=LOG_KEEP_LINES):
    try:
        for i in range(1, 10):
            backup = f"{LOG_FILE}.{i}"
            if os.path.exists(backup):
                os.remove(backup)
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            if len(lines) > max_lines:
                with open(LOG_FILE, 'w', encoding='utf-8') as f:
                    f.writelines(lines[-max_lines:])
    except Exception as e:
        print(f"日志清理失败: {e}")


truncate_log()


class SafeRotatingFileHandler(logging.handlers.RotatingFileHandler):

    def handleError(self, record):
        exc = sys.exc_info()[1]
        if isinstance(exc, OSError) and exc.errno == 28:
            print(f"[磁盘满] {record.getMessage()}")
        else:
            super().handleError(record)


class SymbolFormatter(logging.Formatter):

    def format(self, record):
        if not hasattr(record, 'symbol'):
            record.symbol = 'GLOBAL'
        return super().format(record)


_handlers = [
    SafeRotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=1, encoding='utf-8'),
    logging.StreamHandler(),
]
for _h in _handlers:
    _h.setFormatter(SymbolFormatter('%(asctime)s - %(levelname)s - [%(symbol)s] - %(message)s'))
logging.basicConfig(level=logging.INFO, handlers=_handlers)


def periodic_log_cleanup():
    while True:
        time.sleep(36000)
        truncate_log()


threading.Thread(target=periodic_log_cleanup, daemon=True).start()


# ============================== 配置 ==============================

API_KEY = os.getenv('BINANCE_API_KEY', '')
API_SECRET = os.getenv('BINANCE_API_SECRET', '')


def _cfg(stop_loss_pct):
    return {
        'leverage': 10,
        'stop_loss_pct': stop_loss_pct,
        'take_profit_pct': 0.03,
        'position_percentage': 0.02,
        'min_position_qty': 0.001,
        'price_check_interval': 2,
    }


TRADING_CONFIG = {
    'SOLUSDT': _cfg(0.065),
}

TRADING_SYMBOLS = ['SOLUSDT']
ASSISTANT_SYMBOLS = ['XRPUSDT']
BTC_SYMBOL = 'BTCUSDT'
SPY_SYMBOL = 'SPYUSDT'
SIDES = ('LONG', 'SHORT')

CLOSE_RATIO = 0.2
CLOSE_COOLDOWN = 180
CLOSE_ORDER_TTL = 1800
TAKE_PROFIT_TRIGGER = 0.09
RUNNER_FLOOR = 0.25
TP_STEP = 0.05
TRAIL_GIVEBACK = 0.35
MAX_MARGIN_PCT = 0.03
MAX_DRAWDOWN = 0.45
RISK_STATE_FILE = 'risk_state.json'

client = None
twm = None
trading_bots = []
exchange_filters = {}
shutdown_event = threading.Event()
child_proc = None


# ============================== 指标 ==============================

def klines(symbol, interval, limit):
    data = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    return [float(k[4]) for k in data], [float(k[5]) for k in data]


def ema(vals, n):
    k = 2.0 / (n + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def macd(prices, fast=12, slow=26, sig=9):
    if len(prices) < slow + sig:
        return None, None, None
    line = [f - s for f, s in zip(ema(prices, fast), ema(prices, slow))]
    dea = ema(line, sig)[-1]
    return line[-1], dea, line[-1] - dea


def rsi(prices, n=9):
    if len(prices) <= n:
        return None
    d = [b - a for a, b in zip(prices, prices[1:])]
    gain = [max(x, 0) for x in d]
    loss = [max(-x, 0) for x in d]
    ag, al = sum(gain[:n]) / n, sum(loss[:n]) / n
    for i in range(n, len(d)):
        ag = (ag * (n - 1) + gain[i]) / n
        al = (al * (n - 1) + loss[i]) / n
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)


# ============================== 趋势 / 情绪 / 相关性 / 风控 ==============================

class TrendClassifier:

    def __init__(self, long_label, short_label, min_hold=300):
        self.labels = {'LONG': long_label, 'SHORT': short_label}
        self.mode = 'DUAL'
        self.pending, self.confirm, self.changed = None, 0, 0.0
        self.min_hold = min_hold

    def update(self, p):
        now = time.time()
        if len(p) < 90 or now - self.changed < self.min_hold:
            return self.mode
        dif, dea, _ = macd(p)
        if dif is None:
            return self.mode

        close = p[-1]
        ma7, ma30, ma90 = (sum(p[-n:]) / n for n in (7, 30, 90))
        rec = p[-10:]
        r = rsi(p)
        flat = min(rec) > 0 and (max(rec) - min(rec)) / min(rec) < 0.01 and r is not None and 40 <= r <= 60

        k, z = (1.01, 0.01) if flat else (1.0, 0.0)
        if ma7 > ma30 > ma90 and dif > dea and close > ma30 * k and dif > z:
            raw = self.labels['LONG']
        elif ma7 < ma30 < ma90 and dif < dea and close < ma30 * (2 - k) and dif < -z:
            raw = self.labels['SHORT']
        else:
            raw = 'DUAL'

        if flat and self.mode != 'DUAL':
            weak = {
                self.labels['LONG']: ma7 > ma30 and dif > dea,
                self.labels['SHORT']: ma7 < ma30 and dif < dea,
            }
            raw = self.mode if weak[self.mode] else 'DUAL'

        if raw == self.mode:
            self.pending, self.confirm = None, 0
            return self.mode
        if raw == self.pending:
            self.confirm += 1
        else:
            self.pending, self.confirm = raw, 1
        if self.confirm >= (3 if flat else 2):
            self.mode, self.pending, self.confirm, self.changed = raw, None, 0, now
        return self.mode


class MarketSentimentAnalyzer:

    def __init__(self):
        self.cache = {}
        self.update_interval = 60

    def get_funding_rate(self, symbol):
        try:
            info = client.futures_funding_rate(symbol=symbol, limit=1)
            if info:
                return float(info[0]['fundingRate'])
        except Exception as e:
            logging.error(f"[资金费率获取失败] {symbol}: {e}")
        return 0.0

    def get_volume_ratio(self, symbol):
        try:
            _, vols = klines(symbol, '1h', 24)
            if len(vols) >= 13:
                avg = sum(vols[-13:-2]) / 11
                return vols[-2] / avg if avg > 0 else 1.0
        except Exception as e:
            logging.error(f"[成交量比率获取失败] {symbol}: {e}")
        return 1.0

    def analyze_sentiment(self, symbol):
        now = time.time()
        cached = self.cache.get(symbol)
        if cached and now - cached['timestamp'] < self.update_interval:
            return cached['sentiment']
        try:
            funding = self.get_funding_rate(symbol)
            vol_ratio = self.get_volume_ratio(symbol)

            score = 0.0
            if funding > 0.001:
                score += 0.4
            elif funding > 0.0005:
                score += 0.2
            elif funding < -0.001:
                score -= 0.4
            elif funding < -0.0005:
                score -= 0.2

            if vol_ratio > 2.0:
                score += 0.15
            elif vol_ratio < 0.5:
                score -= 0.09

            score = max(-1.0, min(1.0, score))
            self.cache[symbol] = {'timestamp': now, 'sentiment': score,
                                  'funding_rate': funding, 'volume_ratio': vol_ratio}
            return score
        except Exception as e:
            logging.error(f"[情绪分析失败] {symbol}: {e}")
            return 0.0

    @staticmethod
    def level_of(score):
        if score > 0.7:
            return "EXTREME_BULLISH"
        if score > 0.3:
            return "BULLISH"
        if score > 0.1:
            return "SLIGHTLY_BULLISH"
        if score < -0.7:
            return "EXTREME_BEARISH"
        if score < -0.3:
            return "BEARISH"
        if score < -0.1:
            return "SLIGHTLY_BEARISH"
        return "NEUTRAL"

    def get_sentiment_level(self, symbol):
        return self.level_of(self.analyze_sentiment(symbol))


class CorrelationAnalyzer:

    def __init__(self):
        self.price_history = {}
        self.correlation_matrix = {}
        self.max_history = 100
        self.update_interval = 300
        self.last_update = 0

    def update_price(self, symbol, price):
        self.price_history.setdefault(symbol, deque(maxlen=self.max_history)).append(price)

    @staticmethod
    def _returns(prices):
        return [(b - a) / a for a, b in zip(prices, prices[1:]) if a]

    def calculate_correlation(self, s1, s2):
        h1, h2 = self.price_history.get(s1), self.price_history.get(s2)
        if not h1 or not h2:
            return 0.0
        n = min(len(h1), len(h2))
        if n < 30:
            return 0.0
        x = self._returns(list(h1)[-n:])
        y = self._returns(list(h2)[-n:])
        m = min(len(x), len(y))
        if m < 20:
            return 0.0
        x, y = x[-m:], y[-m:]
        mx, my = sum(x) / m, sum(y) / m
        cov = sum((a - mx) * (b - my) for a, b in zip(x, y))
        vx = sum((a - mx) ** 2 for a in x)
        vy = sum((b - my) ** 2 for b in y)
        den = (vx * vy) ** 0.5
        return cov / den if den else 0.0

    def update_correlation_matrix(self):
        now = time.time()
        if now - self.last_update < self.update_interval:
            return
        symbols = list(self.price_history)
        for i, s1 in enumerate(symbols):
            for s2 in symbols[i + 1:]:
                c = self.calculate_correlation(s1, s2)
                self.correlation_matrix.setdefault(s1, {})[s2] = c
                self.correlation_matrix.setdefault(s2, {})[s1] = c
        self.last_update = now


class DynamicRiskManager:

    def __init__(self):
        self.max_drawdown = 0.0
        self.current_drawdown = 0.0
        self.peak_balance = 0.0
        self._load_state()

    def _load_state(self):
        try:
            if os.path.exists(RISK_STATE_FILE):
                with open(RISK_STATE_FILE, 'r') as f:
                    s = json.load(f)
                self.max_drawdown = float(s.get('max_drawdown', 0.0))
                self.peak_balance = float(s.get('peak_balance', 0.0))
                logging.info(f"[风控] 状态已加载: max_drawdown={self.max_drawdown:.2%}, "
                             f"peak_balance={self.peak_balance:.4f}")
        except Exception as e:
            logging.error(f"[风控] 状态加载失败: {e}")

    def _save_state(self):
        try:
            with open(RISK_STATE_FILE, 'w') as f:
                json.dump({'max_drawdown': self.max_drawdown, 'peak_balance': self.peak_balance}, f)
        except Exception as e:
            logging.error(f"[风控] 状态保存失败: {e}")

    def update_drawdown(self, equity):
        if equity <= 0:
            return
        if equity > self.peak_balance:
            self.peak_balance = equity
            self.current_drawdown = 0.0
            self._save_state()
            return
        self.current_drawdown = (self.peak_balance - equity) / self.peak_balance
        if self.current_drawdown > self.max_drawdown:
            self.max_drawdown = self.current_drawdown
            self._save_state()

    def should_stop_trading(self):
        return self.max_drawdown > MAX_DRAWDOWN


# ============================== 全局行情管理 ==============================

class GlobalBTCManager:

    def __init__(self):
        self.btc_3m_prices = []
        self.last_update_time = 0
        self._trend_fed = 0
        self.update_interval = 60
        self.data_lock = threading.Lock()

        self.ticks = deque(maxlen=200)
        self.tick_lock = threading.Lock()

        self.trend = TrendClassifier('LONG_ONLY', 'SHORT_ONLY')
        self.trend_start_time = 0
        self.trend_lock = threading.Lock()

        self.last_que_key = None
        self.que_lock = threading.Lock()

    def update_btc_data(self):
        now = time.time()
        if now - self.last_update_time < self.update_interval:
            return
        try:
            closes, _ = klines(BTC_SYMBOL, '3m', 120)
            with self.data_lock:
                self.btc_3m_prices = closes
                self.last_update_time = now
        except Exception as e:
            logging.error(f"[全局BTC数据] 更新失败: {e}")

    def update_btc_trend(self):
        prices = self.get_btc_3m_prices()
        if not prices or self.last_update_time == self._trend_fed:
            return
        self._trend_fed = self.last_update_time
        now = time.time()
        with self.trend_lock:
            old = self.trend.mode
            new = self.trend.update(prices)
            if new != old:
                self.trend_start_time = now
                logging.info(f"[BTC趋势] {old} → {new}，计时重置")
            elif self.trend_start_time == 0:
                self.trend_start_time = now

    def get_trend_state(self):
        with self.trend_lock:
            return self.trend.mode, self.trend_start_time

    def try_acquire_que_trigger(self, key):
        with self.que_lock:
            if self.last_que_key == key:
                return False
            self.last_que_key = key
            return True

    def release_que_trigger(self):
        with self.que_lock:
            self.last_que_key = None

    def update_btc_price(self, price, ts):
        with self.tick_lock:
            self.ticks.append((ts, price))

    def check_2s_spike(self):
        now = time.time()
        with self.tick_lock:
            w = [(t, p) for t, p in self.ticks if now - t <= 2]
            if len(w) < 2 or w[0][1] <= 0:
                return None
            prices = [p for _, p in w]
            if (max(prices) - min(prices)) / w[0][1] >= 0.0018:
                self.ticks.clear()
                return 'SPIKE_UP' if w[-1][1] > w[0][1] else 'SPIKE_DOWN'
        return None

    def get_btc_3m_prices(self):
        with self.data_lock:
            return list(self.btc_3m_prices)


class GlobalSPYManager:

    def __init__(self):
        self.spy_1m_prices, self.spy_1m_volumes = [], []
        self.spy_avg_volume = None
        self.last_1m_update = 0
        self.spy_3m_closes = []
        self.last_3m_update = 0
        self.trend = TrendClassifier('LONG_SPY', 'SHORT_SPY')
        self.data_lock = threading.Lock()

    def update_spy_1m_data(self):
        now = time.time()
        if now - self.last_1m_update < 60:
            return
        try:
            closes, vols = klines(SPY_SYMBOL, '1m', 25)
            if not closes:
                return
            with self.data_lock:
                self.spy_1m_prices, self.spy_1m_volumes = closes, vols
                recent = vols[-21:-1]
                self.spy_avg_volume = sum(recent) / len(recent) if recent else None
                self.last_1m_update = now
        except Exception as e:
            logging.error(f"[SPY K线更新] 失败: {e}")

    def check_spy_1m_signal(self):
        self.update_spy_1m_data()
        with self.data_lock:
            if len(self.spy_1m_prices) < 2 or not self.spy_avg_volume:
                return None
            chg = self.spy_1m_prices[-1] / self.spy_1m_prices[-2] - 1
            if self.spy_1m_volumes[-1] / self.spy_avg_volume < 3.0:
                return None
            if chg >= 0.0006:
                return 'STRONG_UP'
            if chg <= -0.0006:
                return 'STRONG_DOWN'
        return None

    def update_spy_3m_data(self):
        now = time.time()
        if now - self.last_3m_update < 60:
            return
        try:
            closes, _ = klines(SPY_SYMBOL, '3m', 150)
            if not closes:
                return
            with self.data_lock:
                self.spy_3m_closes = closes
                self.last_3m_update = now
                old = self.trend.mode
                new = self.trend.update(closes)
            if new != old:
                logging.info(f"[SPY趋势] {old} → {new}")
        except Exception as e:
            logging.error(f"[SPY 3m K线更新] 失败: {e}")

    def get_trend_mode(self):
        with self.data_lock:
            return self.trend.mode


global_btc_manager = GlobalBTCManager()
global_spy_manager = GlobalSPYManager()
sentiment_analyzer = MarketSentimentAnalyzer()
correlation_analyzer = CorrelationAnalyzer()
global_risk_manager = DynamicRiskManager()


# ============================== 交易机器人 ==============================

class SymbolLogger:

    def __init__(self, symbol):
        self.extra = {'symbol': symbol}

    def info(self, msg):
        logging.info(msg, extra=self.extra)

    def error(self, msg):
        logging.error(msg, extra=self.extra)

    def warning(self, msg):
        logging.warning(msg, extra=self.extra)

    def debug(self, msg):
        logging.debug(msg, extra=self.extra)


def parse_ticker_price(msg):
    """兼容 {'stream','data'} 包装；优先取最新成交价 c，没有则取买一卖一中间价。"""
    if isinstance(msg, dict) and isinstance(msg.get('data'), dict):
        msg = msg['data']
    if not isinstance(msg, dict) or msg.get('e') == 'error':
        return None
    try:
        if msg.get('c') is not None:
            price = float(msg['c'])
        elif msg.get('b') is not None and msg.get('a') is not None:
            price = (float(msg['b']) + float(msg['a'])) / 2
        else:
            return None
    except (TypeError, ValueError):
        return None
    return price if 0 < price < 1_000_000 else None


class TradingBot:

    def __init__(self, symbol, btc_symbol=BTC_SYMBOL):
        self.symbol = symbol
        self.btc_symbol = btc_symbol
        self.config = TRADING_CONFIG.get(symbol, _cfg(0.06))
        self.logger = SymbolLogger(symbol)
        self.leverage = self.config['leverage']

        self.current_position = {s: self._empty_pos() for s in SIDES}
        self.last_clear_time = {s: None for s in SIDES}
        self.ban_period = 18 * 60

        self.market_data = {'price': None, 'ts': 0.0}
        self.trend_mode = 'DUAL'
        self.spy_trend_mode = 'DUAL'
        self.active_trading = False

        self._delayed_timers = {}
        self._last = {}
        self._tp_since = {s: None for s in SIDES}
        self._peak_qty = {s: 0.0 for s in SIDES}
        self._peak_profit = {s: 0.0 for s in SIDES}
        self._tp_stage = {s: 0 for s in SIDES}
        self._last_tp_profit = {s: 0.0 for s in SIDES}
        self._last_close = {s: 0.0 for s in SIDES}
        self._bal_time, self._bal_value = 0, 0
        self._lock = threading.RLock()
        self._run_lock = threading.Lock()
        self._open_lock = threading.Lock()

    @staticmethod
    def _empty_pos():
        return {'entry_price': None, 'quantity': 0, 'margin': 0, 'open_time': None}

    def _due(self, key, interval):
        now = time.time()
        if now - self._last.get(key, 0) < interval:
            return False
        self._last[key] = now
        return True

    def initialize(self):
        try:
            client.futures_change_leverage(symbol=self.symbol, leverage=self.leverage)
            if self.symbol not in exchange_filters:
                raise RuntimeError("未加载交易所精度信息")
            self.logger.info(f"初始化成功，杠杆设置为{self.leverage}")
            if self.symbol in TRADING_SYMBOLS:
                self.active_trading = True
            return True
        except Exception as e:
            self.logger.error(f"初始化失败: {e}")
            return False

    def symbol_ws_callback(self, msg):
        try:
            if isinstance(msg, dict) and msg.get('e') == 'error':
                self.logger.warning(f"WebSocket错误消息: {msg}")
                return
            price = parse_ticker_price(msg)
            if price:
                self.market_data['price'] = price
                self.market_data['ts'] = time.time()
                correlation_analyzer.update_price(self.symbol, price)
        except Exception as e:
            self.logger.error(f"WebSocket异常: {e}, 消息: {msg}")

    @staticmethod
    def get_ma_signal(prices):
        dif, dea, _ = macd(prices)
        if dif is None:
            return 'NONE'
        return 'UP' if dif > dea else 'DOWN' if dif < dea else 'NONE'

    def get_current_price(self):
        p, ts = self.market_data['price'], self.market_data['ts']
        if p and p > 0 and time.time() - ts < 10:
            return p
        try:
            price = float(client.futures_symbol_ticker(symbol=self.symbol)['price'])
            if price > 0:
                self.market_data['price'] = price
                self.market_data['ts'] = time.time()
                return price
        except Exception as e:
            self.logger.error(f"获取价格失败: {e}")
        return p if p and p > 0 else None

    def _round_qty(self, qty):
        f = exchange_filters[self.symbol]
        q = int(qty / f['step'] + 1e-9) * f['step']
        return round(q, f['qp']) if q >= f['min_qty'] else 0

    def _round_price(self, price):
        f = exchange_filters[self.symbol]
        return round(round(price / f['tick']) * f['tick'], f['pp'])

    def _fq(self, qty):
        return f"{qty:.{exchange_filters[self.symbol]['qp']}f}"

    def _fp(self, price):
        return f"{price:.{exchange_filters[self.symbol]['pp']}f}"

    @staticmethod
    def _profit_ratio(side, entry, price):
        return (price - entry) / entry if side == 'LONG' else (entry - price) / entry

    def can_open_side(self, side):
        t = self.last_clear_time.get(side)
        return t is None or time.time() >= t

    def get_balance(self):
        now = time.time()
        if now - self._bal_time < 20:
            return self._bal_value
        for attempt in range(3):
            try:
                items = [i for i in client.futures_account_balance() if i.get('asset') == 'USDT']
                self._bal_value = round(float(items[0]['availableBalance']), 4) if items else 0
                self._bal_time = now
                return self._bal_value
            except Exception as e:
                if ("Timeout" in str(e) or "-1007" in str(e)) and attempt < 2:
                    time.sleep(3)
                    continue
                self.logger.error(f"获取余额失败: {e}")
                break
        return self._bal_value

    def sync_position(self):
        with self._lock:
            try:
                positions = client.futures_position_information(symbol=self.symbol)
                new = {s: self._empty_pos() for s in SIDES}
                for p in positions:
                    amt = float(p.get('positionAmt', 0))
                    if amt == 0:
                        continue
                    side = p.get('positionSide')
                    if side not in SIDES:
                        side = 'LONG' if amt > 0 else 'SHORT'
                    entry = float(p.get('entryPrice', 0)) or (self.current_position[side]['entry_price'] or 0)
                    if entry == 0:
                        self.logger.error("[sync_position] 无法获取有效入场价，跳过该仓位")
                        continue
                    new[side].update(entry_price=entry, quantity=abs(amt),
                                     margin=round(abs(amt) * entry / self.leverage, 4))

                for side in SIDES:
                    old_qty = self.current_position[side]['quantity']
                    new_qty = new[side]['quantity']
                    if old_qty != new_qty:
                        self.logger.info(f"[仓位变化] {side}仓位从 {old_qty} 变为 {new_qty}")

                    if new_qty > 0:
                        if new_qty > old_qty:
                            new[side]['open_time'] = time.time()
                        else:
                            new[side]['open_time'] = self.current_position[side]['open_time']
                    elif old_qty > 0:
                        self.logger.info(f"[仓位清零] {side}仓位归零")
                        self.last_clear_time[side] = time.time() + self.ban_period

                self.current_position = new
            except Exception as e:
                self.logger.error(f"持仓同步异常: {e}，保持原有仓位数据")

    def execute_open_order(self, side, percentage, delay=0):
        """返回 True 表示已下单或已安排延时下单，False 表示未执行。"""
        if not self.can_open_side(side):
            return False

        if delay > 0:
            old = self._delayed_timers.pop(side, None)
            if old:
                old.cancel()

            def delayed():
                try:
                    self.execute_open_order(side, percentage)
                except Exception as e:
                    self.logger.error(f"[延时开仓执行失败] {e}")
                finally:
                    if self._delayed_timers.get(side) is timer:
                        self._delayed_timers.pop(side, None)

            timer = threading.Timer(delay, delayed)
            timer.daemon = True
            self._delayed_timers[side] = timer
            timer.start()
            return True

        with self._open_lock:
            return self._open_market(side, percentage)

    def _open_market(self, side, percentage):
        bal = self.get_balance()
        if bal <= 0:
            self.logger.warning(f"余额无效: {bal}")
            return False
        if self.current_position[side]['margin'] > bal * MAX_MARGIN_PCT:
            return False
        price = self.get_current_price()
        if not price:
            return False

        f = exchange_filters[self.symbol]
        qty = self._round_qty(bal * percentage * self.leverage / price) or f['min_qty']
        min_notional = f.get('min_notional', 0)
        if min_notional and qty * price < min_notional:
            steps = math.ceil(min_notional / price / f['step'] - 1e-9)
            qty = round(max(steps * f['step'], f['min_qty']), f['qp'])
        if qty * price / self.leverage > bal:
            self.logger.warning(f"[开仓跳过] 可用余额{bal}不足以支付最小下单量{qty}所需保证金")
            return False
        try:
            order = client.futures_create_order(
                symbol=self.symbol,
                side=SIDE_BUY if side == 'LONG' else SIDE_SELL,
                type=ORDER_TYPE_MARKET,
                quantity=self._fq(qty),
                positionSide=side,
                newClientOrderId=f"{self.symbol}_open_{side}_{int(time.time())}"
            )
            self.logger.info(f"[市价开仓] {side} 数量={qty}，参考价={price:.4f}，订单ID: {order['orderId']}")
            self._bal_time = 0
            time.sleep(1)
            self.sync_position()
            return True
        except Exception as e:
            self.logger.error(f"市价开仓失败: {e}")
            return False

    def _has_close_order(self, side):
        try:
            for o in client.futures_get_open_orders(symbol=self.symbol):
                if '_close_' in o.get('clientOrderId', '') and o.get('positionSide') == side:
                    return True
            return False
        except Exception as e:
            self.logger.warning(f"[减仓] 查询挂单失败: {e}")
            return True

    def close(self, side, reason=""):
        pos = self.current_position[side]
        if pos['quantity'] <= 0 or not pos['entry_price']:
            return False
        if time.time() - self._last_close[side] < CLOSE_COOLDOWN:
            return False
        cp = self.get_current_price()
        if not cp:
            return False

        entry = pos['entry_price']
        sign = 1 if side == 'LONG' else -1
        if self._profit_ratio(side, entry, cp) > 0.01:
            price, desc = self._round_price(cp + sign * 0.001), "现价附近"
        else:
            price, desc = self._round_price(entry * (1 + sign * 0.009)), "反弹至盈利0.9%"

        keep = self._peak_qty[side] * RUNNER_FLOOR
        qty = self._round_qty(min(pos['quantity'] * CLOSE_RATIO, pos['quantity'] - keep))
        if qty <= 0 or self._has_close_order(side):
            return False

        try:
            order = client.futures_create_order(
                symbol=self.symbol,
                side=SIDE_SELL if side == 'LONG' else SIDE_BUY,
                type=ORDER_TYPE_LIMIT,
                timeInForce='GTC',
                quantity=self._fq(qty),
                price=self._fp(price),
                positionSide=side,
                newClientOrderId=f"{self.symbol}_close_{side}_{int(time.time())}"
            )
            self._last_close[side] = time.time()
            self.logger.info(f"[减仓挂单] {side} {qty}/{pos['quantity']} 底仓={keep:.4f} 挂单价={price}"
                             f"（{desc}，现价{cp:.4f}），订单ID={order['orderId']}，原因: {reason}")
            return True
        except Exception as e:
            self.logger.error(f"[减仓挂单失败] {side}: {e}")
            return False

    def close_all(self, side, reason=""):
        self.sync_position()
        try:
            for o in client.futures_get_open_orders(symbol=self.symbol):
                if o.get('positionSide') == side:
                    client.futures_cancel_order(symbol=self.symbol, orderId=o['orderId'])
        except Exception as e:
            self.logger.warning(f"[close_all] 取消挂单失败: {e}")

        qty = self._round_qty(self.current_position[side]['quantity'])
        if qty <= 0:
            return
        for attempt in range(3):
            try:
                order = client.futures_create_order(
                    symbol=self.symbol,
                    side=SIDE_SELL if side == 'LONG' else SIDE_BUY,
                    type=ORDER_TYPE_MARKET,
                    quantity=self._fq(qty),
                    positionSide=side,
                    newClientOrderId=f"{self.symbol}_exit_{side}_{int(time.time())}"
                )
                self.logger.info(f"[close_all] {side} 市价全平 数量={qty}，"
                                 f"订单ID={order.get('orderId')}，原因: {reason}")
                self._bal_time = 0
                return
            except Exception as e:
                self.logger.error(f"[close_all] 尝试 {attempt + 1}/3 失败: {e}")
                time.sleep(2)
        self.logger.error(f"[close_all] {side} 平仓失败，请手动处理")

    def cancel_stale_close_orders(self):
        try:
            now_ms = time.time() * 1000
            for o in client.futures_get_open_orders(symbol=self.symbol):
                if '_close_' in o.get('clientOrderId', '') and now_ms - o.get('time', now_ms) > CLOSE_ORDER_TTL * 1000:
                    client.futures_cancel_order(symbol=self.symbol, orderId=o['orderId'])
                    side = o.get('positionSide')
                    if side in self._last_close:
                        self._last_close[side] = 0.0
                    self.logger.info(f"[撤单] 减仓挂单超时未成交，已撤销 {o['orderId']}")
        except Exception as e:
            self.logger.warning(f"[撤单检查失败] {e}")

    def check_stop_conditions(self, side):
        pos = self.current_position[side]
        if pos['quantity'] <= 0 or not pos['entry_price']:
            self._tp_since[side] = None
            self._peak_qty[side] = self._peak_profit[side] = self._last_tp_profit[side] = 0.0
            self._tp_stage[side] = 0
            return
        cp = self.get_current_price()
        if not cp:
            return

        qty = pos['quantity']
        pr = self._profit_ratio(side, pos['entry_price'], cp)
        self._peak_qty[side] = max(self._peak_qty[side], qty)
        self._peak_profit[side] = max(self._peak_profit[side], pr)
        if qty <= self._peak_qty[side] * RUNNER_FLOOR * 1.001:
            return

        peak, last = self._peak_profit[side], self._last_tp_profit[side]
        if self._tp_stage[side] == 0:
            hit = pr >= TAKE_PROFIT_TRIGGER
        else:
            hit = pr >= last + TP_STEP or (peak > last and pr > 0 and peak - pr >= peak * TRAIL_GIVEBACK)
        if not hit:
            self._tp_since[side] = None
            return

        now = time.time()
        if self._tp_since[side] is None:
            self._tp_since[side] = now
            self.logger.info(f"[延时止盈] {side} 盈利{pr:.2%}，8秒后减仓")
        elif now - self._tp_since[side] >= 8:
            self._tp_since[side] = None
            if self.close(side, f"第{self._tp_stage[side] + 1}次止盈 盈利{pr:.2%}"):
                self._tp_stage[side] += 1
                self._last_tp_profit[side] = self._peak_profit[side] = pr

    def close_position_by_ban(self, side):
        self.sync_position()
        bal = self.get_balance()
        pos = self.current_position[side]
        cp = self.get_current_price()
        if bal <= 0 or pos['quantity'] <= 0 or not pos['entry_price'] or not cp:
            return

        pr = self._profit_ratio(side, pos['entry_price'], cp)
        opposite = 'SHORT' if side == 'LONG' else 'LONG'
        if -0.3 < pr < -0.01:
            pct = 0.05 if pos['margin'] > bal * 0.05 else 0.02
            self.execute_open_order(opposite, pct)
            self.logger.info(f"对冲-反向加{opposite}仓{pct}")
        elif pr >= 0.01:
            self.close(side, f"反向信号，盈利{pr:.2%}减仓")

    def enhanced_trading_strategy(self):
        if not self._run_lock.acquire(blocking=False):
            return
        try:
            self.sync_position()

            for attr, new, long_mode, short_mode, name in (
                ('trend_mode', global_btc_manager.get_trend_state()[0], 'LONG_ONLY', 'SHORT_ONLY', 'BTC趋势'),
                ('spy_trend_mode', global_spy_manager.get_trend_mode(), 'LONG_SPY', 'SHORT_SPY', 'SPY趋势'),
            ):
                old = getattr(self, attr)
                if new != old:
                    self.logger.info(f"[{name}切换] {old} → {new}")
                    setattr(self, attr, new)
                    if new == long_mode:
                        self.close_position_by_ban('SHORT')
                    elif new == short_mode:
                        self.close_position_by_ban('LONG')

            for side in SIDES:
                self.check_stop_conditions(side)
            if self._due('stale_orders', 60):
                self.cancel_stale_close_orders()

            if self.active_trading:
                spike = global_btc_manager.check_2s_spike()
                if spike:
                    self.handle_btc_spike(trading_bots, up=(spike == 'SPIKE_UP'))

                spy_signal = global_spy_manager.check_spy_1m_signal()
                if spy_signal and self._due('spy_signal', 60):
                    self.handle_spy_signal(trading_bots, up=(spy_signal == 'STRONG_UP'))

                self.check_fen(trading_bots)
                self.check_qiang(trading_bots)
                self.check_he(trading_bots)
        except Exception as e:
            self.logger.error(f"[增强策略异常] {e}")
        finally:
            self._run_lock.release()

    @staticmethod
    def _find_bot(all_bots, symbol):
        return next((b for b in all_bots if b.symbol == symbol), None)

    def _reduce_all_then_open(self, all_bots, close_side, open_symbol, open_side, pct, note):
        for bot in all_bots:
            bot.close_position_by_ban(close_side)
        target = self._find_bot(all_bots, open_symbol)
        if target:
            target.execute_open_order(open_side, pct)
            self.logger.info(f"{note}：所有交易对处理{close_side}仓，仅对{open_symbol}开{open_side}仓{pct}")

    def handle_btc_spike(self, all_bots, up):
        trend = self.get_ma_signal(global_btc_manager.get_btc_3m_prices())
        if (up and trend == 'DOWN') or (not up and trend == 'UP'):
            return
        self._reduce_all_then_open(all_bots, 'SHORT' if up else 'LONG', 'SOLUSDT',
                                   'LONG' if up else 'SHORT', 0.03,
                                   f"BTC 2秒{'暴涨' if up else '暴跌'}")

    def handle_spy_signal(self, all_bots, up):
        self._reduce_all_then_open(all_bots, 'SHORT' if up else 'LONG', 'SOLUSDT',
                                   'LONG' if up else 'SHORT', 0.03,
                                   f"SPY强势{'上涨' if up else '下跌'}")

    def check_fen(self, all_bots):
        if not self._due('fen_scan', 10) or time.time() - self._last.get('fen_trigger', 0) < 60:
            return
        try:
            closes, vols = klines(self.btc_symbol, '1m', 60)
            if len(closes) < 30:
                return
            avg = sum(vols[:-1]) / (len(vols) - 1)
            dif, dea, hist = macd(closes)
            if avg <= 0 or dif is None:
                return
            ratio, close = vols[-1] / avg, closes[-1]
            ma7, ma30 = sum(closes[-7:]) / 7, sum(closes[-30:]) / 30
            if ratio <= 3.0:
                return

            up = close > ma7 > ma30 and dif > dea and hist > 0 and close >= max(closes[-10:])
            down = close < ma7 < ma30 and dif < dea and hist < 0 and close <= min(closes[-10:])
            if not (up or down):
                return

            side = 'LONG' if up else 'SHORT'
            opposite = 'SHORT' if up else 'LONG'
            self._last['fen_trigger'] = time.time()
            if ratio > 8.0:
                self.logger.info(f"[check_fen] BTC极端放量{ratio:.1f}倍（>8倍），减仓{side}")
                self.close(side, f"BTC极端放量{ratio:.1f}倍")
            else:
                self.logger.info(f"[check_fen] BTC{'上涨' if up else '下跌'}形态，放量{ratio:.1f}倍，开{side}")
                self.close_position_by_ban(opposite)
                self.execute_open_order(side, 0.03, delay=1)
        except Exception as e:
            self.logger.error(f"[check_fen] 执行异常: {e}")

    def get_btc_30d_breakout(self):
        try:
            data = client.futures_klines(symbol=BTC_SYMBOL, interval='1d', limit=31)
            if len(data) < 31:
                return 'NONE'
            high = max(float(k[2]) for k in data[:-1])
            low = min(float(k[3]) for k in data[:-1])
            price = float(data[-1][4])
            if price > high:
                # 突破新高，但相对30日低点涨幅≥24%，视为涨疯，不追多
                if low > 0 and (price - low) / low >= 0.24:
                    return 'RISE_TOO_HIGH'
                return 'NEW_HIGH'
            if price < low:
                # 跌破新低，但相对30日高点跌幅≥24%，视为跌透，不追空
                if high > 0 and (high - price) / high >= 0.24:
                    return 'DROP_TOO_DEEP'
                return 'NEW_LOW'
            return 'NONE'
        except Exception as e:
            self.logger.error(f"[BTC 30日突破] 计算失败: {e}")
            return 'NONE'

    def check_qiang(self, all_bots):
        if not self._due('qiang', 60):
            return
        try:
            closes, _ = klines(BTC_SYMBOL, '15m', 50)
            _, vols = klines(BTC_SYMBOL, '3m', 30)
            if len(closes) < 30 or len(vols) < 21:
                return
            avg = sum(vols[-21:-1]) / 20
            dif, dea, _ = macd(closes)
            if avg <= 0 or dif is None:
                return
            ratio, close = vols[-1] / avg, closes[-1]
            ma7, ma30 = sum(closes[-7:]) / 7, sum(closes[-30:]) / 30
            if ratio < 8:
                return

            up = close > ma7 > ma30 and dif > dea and close >= max(closes[-10:])
            down = close < ma7 < ma30 and dif < dea and close <= min(closes[-10:])
            if not (up or down):
                return

            side = 'LONG' if up else 'SHORT'
            opposite = 'SHORT' if up else 'LONG'
            breakout = self.get_btc_30d_breakout()

            if breakout == 'DROP_TOO_DEEP':
                self.logger.info("[check_qiang] BTC 30日跌幅超24%，已跌透不追空，跳过本轮")
                return
            if breakout == 'RISE_TOO_HIGH':
                self.logger.info("[check_qiang] BTC 30日涨幅超24%，已涨疯不追多，跳过本轮")
                return

            confirmed = breakout == ('NEW_HIGH' if up else 'NEW_LOW')

            if confirmed:
                targets = [('XRPUSDT', 0.09)]
            else:
                targets = [('SOLUSDT', 0.03)]

            target_desc = ', '.join(f"{s} {side} {p}" for s, p in targets)
            self.logger.info(f"[check_qiang] BTC强势{'上涨' if up else '下跌'}（放量{ratio:.1f}倍，"
                             f"30日状态={breakout}），减仓{opposite}，仅开 {target_desc}")

            for bot in all_bots:
                bot.close(opposite, "check_qiang反向减仓")
            for sym, pct in targets:
                target = self._find_bot(all_bots, sym)
                if target:
                    target.execute_open_order(side, pct)
                else:
                    self.logger.warning(f"[check_qiang] 未找到 {sym} 机器人，跳过开仓")
        except Exception as e:
            self.logger.error(f"[check_qiang] 执行异常: {e}")

    def check_he(self, all_bots):
        if not self._due('he', 60):
            return
        try:
            profit = loss = 0.0
            count = 0
            has = {s: False for s in SIDES}
            for bot in all_bots:
                bot.sync_position()
                cp = bot.get_current_price()
                if not cp:
                    continue
                for side in SIDES:
                    pos = bot.current_position[side]
                    if pos['quantity'] <= 0 or not pos['entry_price']:
                        continue
                    count += 1
                    has[side] = True
                    pnl = (cp - pos['entry_price']) * pos['quantity'] * (1 if side == 'LONG' else -1)
                    if pnl > 0:
                        profit += pnl
                    else:
                        loss -= pnl

            if count < 4 or not (has['LONG'] and has['SHORT']) or profit <= 0:
                return
            if loss > 0 and profit / loss <= 1.3:
                return

            self.logger.info(f"[check_he] 触发！盈利合计={profit:.4f}，亏损合计={loss:.4f}，"
                             f"持仓数={count}，所有仓位市价全平")
            for bot in all_bots:
                for side in SIDES:
                    bot.close_all(side, "check_he合计盈利>合计亏损1.3倍")
        except Exception as e:
            self.logger.error(f"[check_he] 执行异常: {e}")


# ============================== 启动 / 运行 / 守护 ==============================

def btc_ws_callback(msg):
    price = parse_ticker_price(msg)
    if price:
        global_btc_manager.update_btc_price(price, time.time())


def start_futures_ticker(callback, symbol):
    """订阅 U 本位合约行情；旧版库没有该方法时退回现货流并告警。"""
    fn = getattr(twm, 'start_individual_symbol_ticker_futures_socket', None)
    if fn is None:
        logging.warning(f"[WebSocket] 当前python-binance无合约ticker接口，{symbol}退回现货流")
        return twm.start_symbol_ticker_socket(callback=callback, symbol=symbol)
    return fn(callback=callback, symbol=symbol)


def load_exchange_filters(symbols):
    wanted = set(symbols)
    for s in client.futures_exchange_info()['symbols']:
        if s['symbol'] not in wanted:
            continue
        f = {x['filterType']: x for x in s['filters']}
        step = float(f['LOT_SIZE']['stepSize'])
        tick = float(f['PRICE_FILTER']['tickSize'])
        exchange_filters[s['symbol']] = {
            'step': step, 'min_qty': float(f['LOT_SIZE']['minQty']), 'tick': tick,
            'min_notional': float((f.get('MIN_NOTIONAL') or {}).get('notional', 0) or 0),
            'qp': max(0, len(f"{step:.10f}".rstrip('0').split('.')[1])),
            'pp': max(0, len(f"{tick:.10f}".rstrip('0').split('.')[1])),
        }


def initialize_global_client():
    global client, twm
    if not API_KEY or not API_SECRET:
        logging.error("未设置环境变量 BINANCE_API_KEY / BINANCE_API_SECRET")
        return False
    for attempt in range(5):
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
                if getattr(twm, '_running', True):
                    break
                time.sleep(0.5)
            else:
                raise RuntimeError("WebSocket管理器启动超时")

            client.futures_ping()
            try:
                client.futures_change_position_mode(dualSidePosition='true')
            except Exception as e:
                logging.info(f"持仓模式无需修改: {e}")
            load_exchange_filters(TRADING_SYMBOLS + ASSISTANT_SYMBOLS)
            logging.info("全局客户端初始化成功")
            return True
        except Exception as e:
            logging.error(f"客户端初始化尝试 {attempt + 1}/5 失败: {e}")
            if attempt < 4:
                time.sleep(10)
    logging.error("客户端初始化最终失败")
    return False


def update_global_equity():
    try:
        equity = float(client.futures_account().get('totalMarginBalance', 0))
        if equity > 0:
            global_risk_manager.update_drawdown(equity)
        else:
            logging.warning(f"[全局风控] 获取到无效权益: {equity}")
    except Exception as e:
        logging.error(f"[全局风控] 权益更新失败: {e}")


def global_data_updater():
    while True:
        try:
            global_btc_manager.update_btc_data()
            global_btc_manager.update_btc_trend()
            global_spy_manager.update_spy_1m_data()
            global_spy_manager.update_spy_3m_data()
        except Exception as e:
            logging.error(f"全局数据更新异常: {e}")
        time.sleep(60)


def global_monitor():
    last_key = None
    while True:
        try:
            now = datetime.datetime.now(datetime.timezone.utc)
            key = now.strftime('%Y%m%d%H')
            if now.minute == 0 and key != last_key:
                last_key = key
                logging.info(f"[全局监控] 时间: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")

                for bot in trading_bots:
                    lp, sp = bot.current_position['LONG'], bot.current_position['SHORT']
                    if lp['quantity'] > 0 or sp['quantity'] > 0:
                        logging.info(f"[持仓报告] {bot.symbol}: LONG={lp['quantity']:.4f}(保证金:{lp['margin']:.2f}), "
                                     f"SHORT={sp['quantity']:.4f}(保证金:{sp['margin']:.2f})")

                prices = global_btc_manager.get_btc_3m_prices()
                if prices:
                    logging.info(f"[BTC报告] 当前价格: {prices[-1]:.2f}")
                mode, start = global_btc_manager.get_trend_state()
                if mode != 'DUAL' and start > 0:
                    logging.info(f"[BTC趋势] {mode} 已持续 {(time.time() - start) / 3600:.2f}h")

                candidates = []
                for bot in trading_bots:
                    score = sentiment_analyzer.analyze_sentiment(bot.symbol)
                    level = sentiment_analyzer.level_of(score)
                    logging.info(f"[情绪报告] {bot.symbol}: {level}")
                    if level in ("SLIGHTLY_BULLISH", "SLIGHTLY_BEARISH"):
                        candidates.append((abs(score), bot, level))
                if candidates:
                    _, bot, level = max(candidates, key=lambda c: c[0])
                    side = 'LONG' if level == "SLIGHTLY_BULLISH" else 'SHORT'
                    bot.logger.info(f"[情绪触发] {level}，开{side}仓 0.01（本轮仅此一个交易对）")
                    bot.execute_open_order(side, 0.01)

            correlation_analyzer.update_correlation_matrix()
            time.sleep(10)
        except Exception as e:
            logging.error(f"[全局监控异常] {e}")
            time.sleep(60)


def emergency_stop():
    logging.warning("[紧急停止] 触发紧急停止，开始平仓")
    for bot in trading_bots:
        for side in SIDES:
            try:
                bot.close_all(side, reason='emergency_stop')
            except Exception as e:
                logging.error(f"[紧急停止] {bot.symbol} 平{side}失败: {e}")
            time.sleep(1)
    logging.warning("[紧急停止] 所有平仓挂单已发送")


def cleanup_all_orders_on_startup(bots):
    cancelled = failed = 0
    for bot in bots:
        try:
            orders = client.futures_get_open_orders(symbol=bot.symbol)
        except Exception as e:
            logging.error(f"[启动清理] {bot.symbol} 查询挂单失败: {e}")
            continue
        for o in orders:
            try:
                client.futures_cancel_order(symbol=bot.symbol, orderId=o['orderId'])
                cancelled += 1
            except Exception as e:
                if '-2011' in str(e) or 'Unknown order' in str(e):
                    cancelled += 1
                else:
                    failed += 1
                    logging.error(f"[启动清理失败] {bot.symbol} 订单 {o['orderId']}: {e}")
    logging.info(f"[启动清理] 完成：撤销 {cancelled} 个挂单，失败 {failed} 个")


def setup_signal_handlers():
    def handler(signum, frame):
        # 信号处理函数里不写日志（logging 非可重入），只置位并通知子进程
        shutdown_event.set()
        proc = child_proc
        if proc is not None and proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def daemon_restart():
    global child_proc
    restart_count, max_restarts, base_delay = 0, 50, 10
    setup_signal_handlers()

    while restart_count < max_restarts and not shutdown_event.is_set():
        started = time.time()
        try:
            logging.info(f"启动交易程序 (重启次数: {restart_count})")
            child_proc = subprocess.Popen([sys.executable, __file__, '--child'],
                                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                          universal_newlines=True, encoding='utf-8', errors='replace')
            for line in iter(child_proc.stdout.readline, ''):
                print(line.rstrip())
            code = child_proc.wait()
            logging.error(f"主程序退出，返回码: {code}")
            if shutdown_event.is_set():
                logging.info("[优雅关闭] 守护进程退出，仓位保持")
                return
            if code in (0, 100):
                logging.info("程序正常退出或请求不重启，不再重启")
                return
            if time.time() - started > 3600:
                restart_count = 0
            delay = min(base_delay * (2 ** restart_count), 300)
            restart_count += 1
            logging.warning(f"程序异常退出，{delay}秒后重启... (第{restart_count}次)")
            if shutdown_event.wait(delay):
                return
        except KeyboardInterrupt:
            logging.info("守护进程被用户中断")
            return
        except Exception as e:
            logging.error(f"守护进程异常: {e}")
            restart_count += 1
            if shutdown_event.wait(min(base_delay * (2 ** restart_count), 300)):
                return
    if not shutdown_event.is_set():
        logging.error(f"达到最大重启次数({max_restarts})，停止重启")


def main_program():
    global trading_bots
    executor = None
    try:
        if not initialize_global_client():
            return 1

        threading.Thread(target=global_data_updater, daemon=True).start()
        threading.Thread(target=global_monitor, daemon=True).start()
        logging.info("全局数据更新线程、全局监控线程已启动")

        try:
            start_futures_ticker(btc_ws_callback, BTC_SYMBOL)
            logging.info("BTC WebSocket启动成功")
        except Exception as e:
            logging.error(f"BTC WebSocket启动失败: {e}")
            return 2

        bots = []
        for symbol in TRADING_SYMBOLS + ASSISTANT_SYMBOLS:
            bot = TradingBot(symbol)
            if bot.initialize():
                bots.append(bot)
                logging.info(f"{'主动交易' if bot.active_trading else '助理'}机器人 {symbol} 初始化成功")
            else:
                logging.error(f"机器人 {symbol} 初始化失败")
        if not bots:
            logging.error("没有成功的交易机器人实例")
            return 3
        trading_bots = bots

        try:
            cleanup_all_orders_on_startup(trading_bots)
        except Exception as e:
            logging.error(f"[启动清理] 执行异常: {e}")

        for bot in trading_bots:
            try:
                start_futures_ticker(bot.symbol_ws_callback, bot.symbol)
                time.sleep(0.5)
            except Exception as e:
                logging.error(f"{bot.symbol} WebSocket启动失败: {e}")

        logging.info("虎鲸策略 VIP v3 已成功启动！"
                     f"共 {len(trading_bots)} 个机器人（{len(TRADING_SYMBOLS)} 主动 + {len(ASSISTANT_SYMBOLS)} 助理）")
        setup_signal_handlers()

        executor = ThreadPoolExecutor(max_workers=min(len(trading_bots), 4))
        last_equity_check = 0
        while not shutdown_event.is_set():
            try:
                futures = {executor.submit(b.enhanced_trading_strategy): b for b in trading_bots}
                done, pending = wait(futures, timeout=30)
                for f in done:
                    try:
                        f.result()
                    except Exception as e:
                        logging.error(f"{futures[f].symbol} 策略执行异常: {e}")
                if pending:
                    logging.warning("交易策略执行超时，继续下一轮")

                if time.time() - last_equity_check >= 90:
                    last_equity_check = time.time()
                    update_global_equity()
                    if global_risk_manager.should_stop_trading():
                        logging.error(f"[全局风险] 最大回撤 {global_risk_manager.max_drawdown:.2%} "
                                      f"> {MAX_DRAWDOWN:.0%}，触发熔断并全平")
                        emergency_stop()
                        return 100
                shutdown_event.wait(2)
            except Exception as e:
                logging.error(f"主监控循环异常: {e}")
                shutdown_event.wait(10)

        logging.info("[优雅关闭] 接收到退出信号，准备关闭程序...")
        for bot in trading_bots:
            lq, sq = bot.current_position['LONG']['quantity'], bot.current_position['SHORT']['quantity']
            if lq > 0 or sq > 0:
                logging.info(f"  {bot.symbol}: LONG={lq:.4f}, SHORT={sq:.4f}")
        logging.info("[优雅关闭] 程序退出，仓位保持")
        return 0
    except Exception as e:
        logging.error(f"主程序异常: {e}")
        return 4
    finally:
        if executor:
            executor.shutdown(wait=False)
        if twm:
            try:
                twm.stop()
            except Exception as e:
                logging.error(f"停止WebSocket管理器时出错: {e}")
        logging.info("程序退出")


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--child':
        sys.exit(main_program())
    elif len(sys.argv) > 1 and sys.argv[1] == '--daemon':
        print("虎鲸策略守护进程启动")
        daemon_restart()
    else:
        print("虎鲸策略启动")
        sys.exit(main_program())
