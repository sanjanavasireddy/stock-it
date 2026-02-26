"""
Real-Time Financial Trade Producer
Simulates realistic market trade data and publishes to Kafka topics.

Design patterns:
  - Geometric Brownian Motion for realistic price simulation
  - Fat-tailed volume distribution to simulate market microstructure
  - Periodic volatility spikes to trigger anomaly detection
  - Circuit breaker simulation for extreme price moves
"""

import os
import json
import time
import uuid
import random
import logging
import signal
import math
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Dict, Optional
from confluent_kafka import Producer, KafkaException
from prometheus_client import start_http_server, Counter, Histogram, Gauge

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("trade-producer")

# ─── Prometheus Metrics ────────────────────────────────────────────────────────
TRADES_PRODUCED = Counter("producer_trades_total", "Total trades produced", ["symbol", "side"])
PRODUCE_LATENCY = Histogram("producer_latency_seconds", "Kafka produce latency")
PRODUCE_ERRORS  = Counter("producer_errors_total", "Kafka produce errors", ["error_type"])
ACTIVE_SYMBOLS  = Gauge("producer_active_symbols", "Number of active trading symbols")
TRADES_PER_SEC  = Gauge("producer_trades_per_second", "Current trade production rate")

# ─── Market Configuration ─────────────────────────────────────────────────────
MARKET_CONFIG = {
    # Stocks
    "AAPL":    {"base_price": 182.50, "volatility": 0.0015, "avg_volume": 500,   "exchange": "NASDAQ"},
    "GOOGL":   {"base_price": 141.20, "volatility": 0.0018, "avg_volume": 200,   "exchange": "NASDAQ"},
    "MSFT":    {"base_price": 415.30, "volatility": 0.0012, "avg_volume": 350,   "exchange": "NASDAQ"},
    "AMZN":    {"base_price": 185.60, "volatility": 0.0020, "avg_volume": 300,   "exchange": "NASDAQ"},
    "TSLA":    {"base_price": 248.40, "volatility": 0.0035, "avg_volume": 800,   "exchange": "NASDAQ"},
    # Crypto
    "BTC-USD": {"base_price": 52340.0, "volatility": 0.0040, "avg_volume": 0.5,  "exchange": "COINBASE"},
    "ETH-USD": {"base_price": 3120.0,  "volatility": 0.0045, "avg_volume": 5.0,  "exchange": "COINBASE"},
    # Forex
    "EUR-USD": {"base_price": 1.0850,  "volatility": 0.0003, "avg_volume": 100000, "exchange": "FX"},
    "GBP-USD": {"base_price": 1.2650,  "volatility": 0.0004, "avg_volume": 80000,  "exchange": "FX"},
    "JPY-USD": {"base_price": 0.00667, "volatility": 0.0003, "avg_volume": 500000, "exchange": "FX"},
}

EXCHANGES = ["NYSE", "NASDAQ", "CBOE", "COINBASE", "BINANCE", "FX", "CME"]
TRADER_IDS = [f"trader_{i:04d}" for i in range(1, 201)]


@dataclass
class Trade:
    trade_id:   str
    symbol:     str
    price:      float
    quantity:   float
    side:       str           # BUY | SELL
    exchange:   str
    trader_id:  str
    timestamp:  str           # ISO 8601
    sequence:   int
    metadata:   dict


class MarketSimulator:
    """Simulates realistic market price dynamics using Geometric Brownian Motion."""

    def __init__(self, config: Dict):
        self.prices: Dict[str, float] = {sym: cfg["base_price"] for sym, cfg in config.items()}
        self.config = config
        self._volatility_multiplier = 1.0
        self._spike_countdown = 0

    def next_price(self, symbol: str) -> float:
        cfg = self.config[symbol]
        price = self.prices[symbol]
        vol = cfg["volatility"] * self._volatility_multiplier

        # Geometric Brownian Motion: dS = S * (μ dt + σ dW)
        dt = 1 / 252 / 6.5 / 3600          # 1-second step in trading-year units
        drift = 0.0                          # zero drift for simplicity
        shock = random.gauss(0, 1)
        new_price = price * math.exp((drift - 0.5 * vol**2) * dt + vol * math.sqrt(dt) * shock)

        # Bound price from going negative or unrealistically large
        new_price = max(price * 0.90, min(price * 1.10, new_price))
        self.prices[symbol] = new_price
        return round(new_price, 6)

    def next_volume(self, symbol: str) -> float:
        cfg = self.config[symbol]
        avg = cfg["avg_volume"]
        # Log-normal distribution → fat tails (realistic)
        vol = random.lognormvariate(math.log(avg), 0.8)
        return round(max(vol, avg * 0.01), 6)

    def inject_volatility_spike(self, symbol: str, magnitude: float = 3.0):
        """Simulate news event or liquidity shock."""
        self._volatility_multiplier = magnitude
        self._spike_countdown = random.randint(5, 20)
        logger.warning(f"VOLATILITY SPIKE injected for {symbol} (x{magnitude:.1f})")

    def step(self):
        if self._spike_countdown > 0:
            self._spike_countdown -= 1
        else:
            self._volatility_multiplier = 1.0


class TradeProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic_trades  = os.getenv("KAFKA_TOPIC_TRADES", "raw-trades")
        self.topic_orders  = os.getenv("KAFKA_TOPIC_ORDERS", "order-book")
        self.target_tps    = int(os.getenv("TRADES_PER_SECOND", "50"))
        self.symbols       = os.getenv("SYMBOLS", "AAPL,GOOGL,MSFT").split(",")

        # Filter config to requested symbols
        self.market_config = {s: MARKET_CONFIG[s] for s in self.symbols if s in MARKET_CONFIG}
        self.simulator     = MarketSimulator(self.market_config)
        self.sequence      = 0
        self._running      = True

        self.producer = Producer({
            "bootstrap.servers":        self.bootstrap_servers,
            "client.id":                "trade-producer-01",
            "acks":                     "all",
            "retries":                  5,
            "retry.backoff.ms":         500,
            "compression.type":         "snappy",
            "linger.ms":                5,          # micro-batching
            "batch.size":               65536,
            "queue.buffering.max.kbytes": 32768,
        })

        ACTIVE_SYMBOLS.set(len(self.market_config))
        logger.info(f"Producer initialized | symbols={list(self.market_config.keys())} | tps={self.target_tps}")

    def _delivery_callback(self, err, msg):
        if err:
            PRODUCE_ERRORS.labels(error_type=type(err).__name__).inc()
            logger.error(f"Delivery failed: {err}")

    def _make_trade(self, symbol: str) -> Trade:
        self.sequence += 1
        price    = self.simulator.next_price(symbol)
        quantity = self.simulator.next_volume(symbol)
        exchange = self.market_config[symbol]["exchange"]

        return Trade(
            trade_id  = str(uuid.uuid4()),
            symbol    = symbol,
            price     = price,
            quantity  = quantity,
            side      = random.choice(["BUY", "SELL"]),
            exchange  = exchange,
            trader_id = random.choice(TRADER_IDS),
            timestamp = datetime.now(timezone.utc).isoformat(),
            sequence  = self.sequence,
            metadata  = {
                "source":   "simulator-v1",
                "latency_us": random.randint(50, 5000),
            }
        )

    def _produce_trade(self, trade: Trade):
        payload = json.dumps(asdict(trade)).encode("utf-8")
        key     = trade.symbol.encode("utf-8")   # partition by symbol

        start = time.perf_counter()
        try:
            self.producer.produce(
                topic=self.topic_trades,
                key=key,
                value=payload,
                callback=self._delivery_callback,
                headers={"content-type": b"application/json", "source": b"simulator"},
            )
            PRODUCE_LATENCY.observe(time.perf_counter() - start)
            TRADES_PRODUCED.labels(symbol=trade.symbol, side=trade.side).inc()
        except KafkaException as e:
            PRODUCE_ERRORS.labels(error_type="KafkaException").inc()
            logger.error(f"Produce error: {e}")

    def _maybe_inject_spike(self):
        """Randomly inject volatility spike every ~60 seconds for demo."""
        if random.random() < (1 / (60 * self.target_tps)):
            symbol = random.choice(list(self.market_config.keys()))
            magnitude = random.uniform(2.0, 5.0)
            self.simulator.inject_volatility_spike(symbol, magnitude)

    def run(self):
        signal.signal(signal.SIGTERM, lambda *_: self.stop())
        signal.signal(signal.SIGINT,  lambda *_: self.stop())

        start_http_server(8001)       # Prometheus metrics endpoint
        logger.info("Prometheus metrics on :8001/metrics")

        logger.info(f"Starting trade production at {self.target_tps} trades/sec")
        interval = 1.0 / self.target_tps
        poll_counter = 0

        while self._running:
            loop_start = time.perf_counter()

            symbol = random.choice(list(self.market_config.keys()))
            trade  = self._make_trade(symbol)
            self._produce_trade(trade)
            self._maybe_inject_spike()
            self.simulator.step()

            # Flush and poll periodically
            poll_counter += 1
            if poll_counter % 100 == 0:
                self.producer.poll(0)
                TRADES_PER_SEC.set(self.target_tps)
                logger.debug(f"seq={self.sequence} | queue={len(self.producer)}")

            # Rate limiting
            elapsed = time.perf_counter() - loop_start
            sleep_time = interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

        self.producer.flush(timeout=30)
        logger.info("Producer shut down cleanly.")

    def stop(self):
        logger.info("Shutdown signal received.")
        self._running = False


if __name__ == "__main__":
    TradeProducer().run()
