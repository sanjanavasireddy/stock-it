"""
Real-Time Stream Processor
Consumes trades from Kafka, applies:
  - VWAP (Volume-Weighted Average Price) computation
  - Moving average & price change detection
  - Anomaly detection (price spikes, volume spikes, circuit breakers)
  - Windowed aggregations (1m, 5m, 15m OHLCV)
  - Publishes enriched events + alerts back to Kafka
  - Persists aggregates to PostgreSQL (TimescaleDB)
  - Maintains real-time state in Redis
"""

import os
import json
import time
import uuid
import logging
import signal
import threading
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Deque
import redis
import psycopg2
from psycopg2.extras import execute_batch
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from prometheus_client import start_http_server, Counter, Histogram, Gauge, Summary

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("stream-processor")

# ─── Prometheus Metrics ────────────────────────────────────────────────────────
TRADES_PROCESSED     = Counter("processor_trades_total", "Total trades processed", ["symbol"])
ALERTS_GENERATED     = Counter("processor_alerts_total", "Alerts generated", ["alert_type", "severity"])
PROCESSING_LATENCY   = Histogram("processor_latency_seconds", "Per-trade processing latency",
                                  buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1.0])
CONSUMER_LAG         = Gauge("processor_consumer_lag", "Kafka consumer lag", ["partition"])
REDIS_OPS            = Counter("processor_redis_ops_total", "Redis operations", ["op"])
DB_WRITES            = Counter("processor_db_writes_total", "Database write operations", ["table"])
ACTIVE_WINDOWS       = Gauge("processor_active_windows", "Active time windows in memory")
CURRENT_PRICES       = Gauge("processor_price", "Current price per symbol", ["symbol"])


# ─── Data Structures ──────────────────────────────────────────────────────────
@dataclass
class TradeEvent:
    trade_id: str
    symbol:   str
    price:    float
    quantity: float
    side:     str
    exchange: str
    trader_id: str
    timestamp: str
    sequence: int
    metadata: dict


@dataclass
class OHLCV:
    symbol:       str
    window_start: datetime
    window_end:   datetime
    window_sec:   int
    open:         float
    high:         float
    low:          float
    close:        float
    volume:       float
    vwap:         float
    trade_count:  int
    buy_volume:   float
    sell_volume:  float
    pv_sum:       float   # price * volume accumulator for VWAP


@dataclass
class Alert:
    alert_id:    str
    alert_type:  str
    severity:    str
    symbol:      str
    message:     str
    details:     dict
    triggered_at: str


class WindowManager:
    """Manages tumbling time windows for OHLCV aggregations."""

    WINDOWS = [60, 300, 900]   # 1m, 5m, 15m

    def __init__(self):
        # {symbol -> {window_seconds -> OHLCV}}
        self._windows: Dict[str, Dict[int, OHLCV]] = defaultdict(dict)

    def _window_start(self, ts: datetime, window_sec: int) -> datetime:
        epoch = ts.timestamp()
        start = (epoch // window_sec) * window_sec
        return datetime.fromtimestamp(start, tz=timezone.utc)

    def update(self, trade: TradeEvent) -> List[OHLCV]:
        """Update windows; returns list of completed windows."""
        ts = datetime.fromisoformat(trade.timestamp)
        completed = []

        for wsec in self.WINDOWS:
            wstart = self._window_start(ts, wsec)
            wend   = wstart + timedelta(seconds=wsec)

            key = trade.symbol
            existing = self._windows[key].get(wsec)

            if existing and existing.window_start != wstart:
                # Window rolled over → finalize the old one
                completed.append(existing)
                existing = None

            if existing is None:
                self._windows[key][wsec] = OHLCV(
                    symbol       = trade.symbol,
                    window_start = wstart,
                    window_end   = wend,
                    window_sec   = wsec,
                    open         = trade.price,
                    high         = trade.price,
                    low          = trade.price,
                    close        = trade.price,
                    volume       = trade.quantity,
                    vwap         = trade.price,
                    trade_count  = 1,
                    buy_volume   = trade.quantity if trade.side == "BUY" else 0.0,
                    sell_volume  = trade.quantity if trade.side == "SELL" else 0.0,
                    pv_sum       = trade.price * trade.quantity,
                )
            else:
                w = existing
                w.high         = max(w.high, trade.price)
                w.low          = min(w.low, trade.price)
                w.close        = trade.price
                w.volume      += trade.quantity
                w.pv_sum      += trade.price * trade.quantity
                w.vwap         = w.pv_sum / w.volume
                w.trade_count += 1
                if trade.side == "BUY":
                    w.buy_volume += trade.quantity
                else:
                    w.sell_volume += trade.quantity

        ACTIVE_WINDOWS.set(sum(len(v) for v in self._windows.values()))
        return completed


class AnomalyDetector:
    """Detects price and volume anomalies in real-time."""

    def __init__(self,
                 price_spike_threshold: float = 0.05,
                 circuit_breaker_threshold: float = 0.10,
                 volume_spike_multiplier: float = 3.0,
                 lookback_window: int = 100):
        self.price_threshold     = price_spike_threshold
        self.cb_threshold        = circuit_breaker_threshold
        self.volume_multiplier   = volume_spike_multiplier
        # Rolling buffers for each symbol
        self._prices:  Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=lookback_window))
        self._volumes: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=lookback_window))

    def _moving_avg(self, dq: Deque[float]) -> Optional[float]:
        return sum(dq) / len(dq) if len(dq) >= 5 else None

    def check(self, trade: TradeEvent) -> List[Alert]:
        alerts = []
        symbol = trade.symbol

        prev_prices  = self._prices[symbol]
        prev_volumes = self._volumes[symbol]

        avg_price  = self._moving_avg(prev_prices)
        avg_volume = self._moving_avg(prev_volumes)

        # ── Price Spike Detection ──────────────────────────────────────────────
        if avg_price:
            change_pct = abs(trade.price - avg_price) / avg_price

            if change_pct >= self.cb_threshold:
                alerts.append(Alert(
                    alert_id    = str(uuid.uuid4()),
                    alert_type  = "CIRCUIT_BREAKER",
                    severity    = "CRITICAL",
                    symbol      = symbol,
                    message     = (f"CIRCUIT BREAKER: {symbol} price moved {change_pct:.1%} "
                                   f"(avg={avg_price:.4f}, current={trade.price:.4f})"),
                    details     = {"change_pct": change_pct, "avg_price": avg_price,
                                   "current_price": trade.price, "threshold": self.cb_threshold},
                    triggered_at = datetime.now(timezone.utc).isoformat(),
                ))
            elif change_pct >= self.price_threshold:
                alerts.append(Alert(
                    alert_id    = str(uuid.uuid4()),
                    alert_type  = "PRICE_SPIKE",
                    severity    = "HIGH",
                    symbol      = symbol,
                    message     = (f"Price spike on {symbol}: {change_pct:.1%} move "
                                   f"(avg={avg_price:.4f}, current={trade.price:.4f})"),
                    details     = {"change_pct": change_pct, "avg_price": avg_price,
                                   "current_price": trade.price, "threshold": self.price_threshold},
                    triggered_at = datetime.now(timezone.utc).isoformat(),
                ))

        # ── Volume Spike Detection ─────────────────────────────────────────────
        if avg_volume and trade.quantity > avg_volume * self.volume_multiplier:
            alerts.append(Alert(
                alert_id    = str(uuid.uuid4()),
                alert_type  = "VOLUME_SPIKE",
                severity    = "MEDIUM",
                symbol      = symbol,
                message     = (f"Volume spike on {symbol}: {trade.quantity:.2f} "
                               f"vs avg {avg_volume:.2f} ({trade.quantity/avg_volume:.1f}x)"),
                details     = {"quantity": trade.quantity, "avg_volume": avg_volume,
                               "multiplier": trade.quantity / avg_volume},
                triggered_at = datetime.now(timezone.utc).isoformat(),
            ))

        # Update buffers
        prev_prices.append(trade.price)
        prev_volumes.append(trade.quantity)
        return alerts


class StreamProcessor:
    def __init__(self):
        self.kafka_servers    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic_trades     = os.getenv("KAFKA_TOPIC_TRADES", "raw-trades")
        self.topic_alerts     = os.getenv("KAFKA_TOPIC_ALERTS", "trade-alerts")
        self.topic_aggregates = os.getenv("KAFKA_TOPIC_AGGREGATES", "trade-aggregates")
        self.consumer_group   = os.getenv("KAFKA_CONSUMER_GROUP", "stream-processor-group")
        self.redis_host       = os.getenv("REDIS_HOST", "localhost")
        self.redis_port       = int(os.getenv("REDIS_PORT", "6379"))
        self.postgres_dsn     = os.getenv("POSTGRES_DSN", "postgresql://trader:trader_secret@localhost:5432/trades_db")
        self._running         = True

        self.window_manager = WindowManager()
        self.anomaly_detector = AnomalyDetector(
            price_spike_threshold     = float(os.getenv("ANOMALY_PRICE_THRESHOLD", "0.05")),
            circuit_breaker_threshold = 0.10,
            volume_spike_multiplier   = float(os.getenv("ANOMALY_VOLUME_MULTIPLIER", "3.0")),
        )

        self._init_kafka()
        self._init_redis()
        self._init_postgres()

        # Batch buffer for DB writes
        self._pg_batch:  List[dict] = []
        self._agg_batch: List[OHLCV] = []
        self._last_flush = time.time()
        self._flush_interval = 5.0       # flush every 5 seconds

        logger.info("Stream processor initialized.")

    def _init_kafka(self):
        self.consumer = Consumer({
            "bootstrap.servers":  self.kafka_servers,
            "group.id":           self.consumer_group,
            "auto.offset.reset":  "latest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms":   30000,
        })
        self.consumer.subscribe([self.topic_trades])

        self.producer = Producer({
            "bootstrap.servers": self.kafka_servers,
            "client.id":         "stream-processor-producer",
            "acks":              "1",
            "compression.type":  "snappy",
        })

    def _init_redis(self):
        self.redis = redis.Redis(
            host=self.redis_host, port=self.redis_port,
            decode_responses=True, socket_connect_timeout=5
        )
        self.redis.ping()
        logger.info("Redis connected.")

    def _init_postgres(self):
        self.pg_conn = psycopg2.connect(self.postgres_dsn)
        self.pg_conn.autocommit = False
        logger.info("PostgreSQL connected.")

    def _update_redis(self, trade: TradeEvent):
        """Maintain real-time state in Redis for API consumption."""
        pipe = self.redis.pipeline()
        ts   = int(datetime.now(timezone.utc).timestamp() * 1000)
        sym  = trade.symbol

        # Latest price
        pipe.hset(f"price:{sym}", mapping={"price": trade.price, "ts": trade.timestamp, "side": trade.side})
        pipe.expire(f"price:{sym}", 3600)

        # Recent trades ring buffer (last 100)
        trade_payload = json.dumps({"p": trade.price, "q": trade.quantity, "s": trade.side, "t": trade.timestamp})
        pipe.lpush(f"trades:{sym}", trade_payload)
        pipe.ltrim(f"trades:{sym}", 0, 99)
        pipe.expire(f"trades:{sym}", 3600)

        # Trade count (daily)
        pipe.incr(f"count:daily:{sym}")
        pipe.expire(f"count:daily:{sym}", 86400)

        # Price time-series (sorted set for range queries)
        pipe.zadd(f"prices:ts:{sym}", {json.dumps({"p": trade.price, "t": trade.timestamp}): ts})
        pipe.zremrangebyrank(f"prices:ts:{sym}", 0, -1001)   # keep last 1000
        pipe.expire(f"prices:ts:{sym}", 3600)

        pipe.execute()
        REDIS_OPS.labels(op="set").inc()
        CURRENT_PRICES.labels(symbol=sym).set(trade.price)

    def _update_redis_ohlcv(self, ohlcv: OHLCV):
        key = f"ohlcv:{ohlcv.symbol}:{ohlcv.window_sec}"
        data = {
            "open":  ohlcv.open,   "high":        ohlcv.high,
            "low":   ohlcv.low,    "close":       ohlcv.close,
            "volume": ohlcv.volume, "vwap":        ohlcv.vwap,
            "trades": ohlcv.trade_count,
            "window_start": ohlcv.window_start.isoformat(),
            "window_end":   ohlcv.window_end.isoformat(),
        }
        self.redis.hset(key, mapping={k: str(v) for k, v in data.items()})
        self.redis.expire(key, 3600)

    def _publish_alert(self, alert: Alert):
        payload = json.dumps(asdict(alert)).encode()
        self.producer.produce(
            topic=self.topic_alerts,
            key=alert.symbol.encode(),
            value=payload,
        )
        ALERTS_GENERATED.labels(alert_type=alert.alert_type, severity=alert.severity).inc()
        logger.warning(f"ALERT [{alert.severity}] {alert.alert_type} | {alert.message}")

    def _publish_aggregate(self, ohlcv: OHLCV):
        payload = json.dumps({
            "symbol":      ohlcv.symbol,
            "window_sec":  ohlcv.window_sec,
            "window_start": ohlcv.window_start.isoformat(),
            "open":  ohlcv.open,  "high":  ohlcv.high,
            "low":   ohlcv.low,   "close": ohlcv.close,
            "volume": ohlcv.volume, "vwap": ohlcv.vwap,
            "trades": ohlcv.trade_count,
        }).encode()
        self.producer.produce(topic=self.topic_aggregates, key=ohlcv.symbol.encode(), value=payload)

    def _flush_to_postgres(self):
        if not self._pg_batch and not self._agg_batch:
            return

        try:
            cur = self.pg_conn.cursor()

            # Flush raw trades
            if self._pg_batch:
                execute_batch(cur, """
                    INSERT INTO trades (trade_id, symbol, price, quantity, side, exchange, trader_id, timestamp, processed_at)
                    VALUES (%(trade_id)s, %(symbol)s, %(price)s, %(quantity)s, %(side)s, %(exchange)s, %(trader_id)s,
                            %(timestamp)s, NOW())
                    ON CONFLICT DO NOTHING
                """, self._pg_batch, page_size=500)
                DB_WRITES.labels(table="trades").inc(len(self._pg_batch))
                self._pg_batch.clear()

            # Flush aggregates
            if self._agg_batch:
                agg_rows = [{
                    "symbol":       a.symbol,
                    "window_start": a.window_start,
                    "window_end":   a.window_end,
                    "window_sec":   a.window_sec,
                    "trade_count":  a.trade_count,
                    "total_volume": a.volume,
                    "total_value":  a.pv_sum,
                    "vwap":         a.vwap,
                    "open_price":   a.open,
                    "high_price":   a.high,
                    "low_price":    a.low,
                    "close_price":  a.close,
                    "buy_volume":   a.buy_volume,
                    "sell_volume":  a.sell_volume,
                } for a in self._agg_batch]

                execute_batch(cur, """
                    INSERT INTO trade_aggregates
                        (symbol, window_start, window_end, window_seconds, trade_count, total_volume,
                         total_value, vwap, open_price, high_price, low_price, close_price, buy_volume, sell_volume)
                    VALUES
                        (%(symbol)s, %(window_start)s, %(window_end)s, %(window_sec)s, %(trade_count)s,
                         %(total_volume)s, %(total_value)s, %(vwap)s, %(open_price)s, %(high_price)s,
                         %(low_price)s, %(close_price)s, %(buy_volume)s, %(sell_volume)s)
                    ON CONFLICT (symbol, window_start, window_seconds) DO UPDATE SET
                        trade_count  = EXCLUDED.trade_count,
                        total_volume = EXCLUDED.total_volume,
                        vwap         = EXCLUDED.vwap,
                        close_price  = EXCLUDED.close_price
                """, agg_rows, page_size=200)
                DB_WRITES.labels(table="trade_aggregates").inc(len(agg_rows))
                self._agg_batch.clear()

            self.pg_conn.commit()
            cur.close()

        except Exception as e:
            logger.error(f"PostgreSQL flush error: {e}")
            self.pg_conn.rollback()

    def _process_trade(self, raw: bytes) -> Optional[TradeEvent]:
        try:
            data = json.loads(raw)
            return TradeEvent(**data)
        except Exception as e:
            logger.error(f"Failed to parse trade: {e}")
            return None

    def run(self):
        signal.signal(signal.SIGTERM, lambda *_: self.stop())
        signal.signal(signal.SIGINT,  lambda *_: self.stop())

        start_http_server(8002)
        logger.info("Prometheus metrics on :8002/metrics")
        logger.info("Stream processor running...")

        while self._running:
            msg = self.consumer.poll(timeout=1.0)

            if msg is None:
                # Flush on idle
                if time.time() - self._last_flush > self._flush_interval:
                    self._flush_to_postgres()
                    self.producer.poll(0)
                    self._last_flush = time.time()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            start = time.perf_counter()

            trade = self._process_trade(msg.value())
            if not trade:
                self.consumer.commit(message=msg)
                continue

            # 1. Update real-time Redis state
            self._update_redis(trade)

            # 2. Windowed aggregations
            completed_windows = self.window_manager.update(trade)
            for ohlcv in completed_windows:
                self._update_redis_ohlcv(ohlcv)
                self._publish_aggregate(ohlcv)
                self._agg_batch.append(ohlcv)

            # 3. Anomaly detection
            alerts = self.anomaly_detector.check(trade)
            for alert in alerts:
                self._publish_alert(alert)

            # 4. Buffer for batch DB write
            self._pg_batch.append({
                "trade_id":  trade.trade_id,
                "symbol":    trade.symbol,
                "price":     trade.price,
                "quantity":  trade.quantity,
                "side":      trade.side,
                "exchange":  trade.exchange,
                "trader_id": trade.trader_id,
                "timestamp": trade.timestamp,
            })

            # Flush every 5s
            if time.time() - self._last_flush > self._flush_interval:
                self._flush_to_postgres()
                self.producer.poll(0)
                self._last_flush = time.time()

            self.consumer.commit(message=msg)

            elapsed = time.perf_counter() - start
            PROCESSING_LATENCY.observe(elapsed)
            TRADES_PROCESSED.labels(symbol=trade.symbol).inc()

        # Final flush
        self._flush_to_postgres()
        self.producer.flush(timeout=10)
        self.consumer.close()
        self.pg_conn.close()
        logger.info("Processor shut down cleanly.")

    def stop(self):
        logger.info("Shutdown signal received.")
        self._running = False


if __name__ == "__main__":
    StreamProcessor().run()
