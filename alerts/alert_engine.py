"""
Alert Engine
Consumes alert events from Kafka and:
  - Applies deduplication (Redis-backed, 60s cooldown per alert type+symbol)
  - Enforces severity-based routing rules
  - Delivers notifications via Webhook / Slack / Email stubs
  - Persists alerts to PostgreSQL
  - Exposes Prometheus metrics
  - Implements circuit breaker state machine per symbol
"""

import os
import json
import time
import logging
import signal
import requests
import psycopg2
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Optional
import redis
from confluent_kafka import Consumer, KafkaError
from prometheus_client import start_http_server, Counter, Gauge, Histogram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("alert-engine")

# ─── Metrics ──────────────────────────────────────────────────────────────────
ALERTS_RECEIVED   = Counter("alertengine_received_total", "Alerts received", ["alert_type"])
ALERTS_DELIVERED  = Counter("alertengine_delivered_total", "Alerts delivered", ["channel"])
ALERTS_DEDUPED    = Counter("alertengine_deduped_total", "Alerts suppressed by dedup")
CIRCUIT_BREAKERS  = Gauge("alertengine_circuit_breakers_open", "Open circuit breakers")
DELIVERY_LATENCY  = Histogram("alertengine_delivery_latency_seconds", "Notification delivery latency")


@dataclass
class AlertEvent:
    alert_id:    str
    alert_type:  str
    severity:    str
    symbol:      str
    message:     str
    details:     dict
    triggered_at: str


class CircuitBreakerState:
    """Per-symbol circuit breaker state machine."""

    STATES = {"CLOSED", "OPEN", "HALF_OPEN"}

    def __init__(self):
        self._states = {}         # symbol -> state
        self._open_at = {}        # symbol -> timestamp

    def trip(self, symbol: str):
        self._states[symbol] = "OPEN"
        self._open_at[symbol] = datetime.now(timezone.utc)
        CIRCUIT_BREAKERS.inc()
        logger.critical(f"CIRCUIT BREAKER TRIPPED: {symbol}")

    def check(self, symbol: str) -> str:
        state = self._states.get(symbol, "CLOSED")
        if state == "OPEN":
            opened = self._open_at.get(symbol)
            if opened and datetime.now(timezone.utc) - opened > timedelta(minutes=5):
                self._states[symbol] = "HALF_OPEN"
                logger.warning(f"Circuit breaker entering HALF_OPEN: {symbol}")
                return "HALF_OPEN"
        return state

    def reset(self, symbol: str):
        if self._states.get(symbol) in ("OPEN", "HALF_OPEN"):
            CIRCUIT_BREAKERS.dec()
        self._states[symbol] = "CLOSED"
        logger.info(f"Circuit breaker RESET: {symbol}")


class DeduplicationFilter:
    """Redis-backed deduplication with configurable cooldown per alert type."""

    COOLDOWNS = {
        "CIRCUIT_BREAKER": 300,    # 5 min
        "PRICE_SPIKE":     60,     # 1 min
        "VOLUME_SPIKE":    30,     # 30 sec
    }

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    def is_duplicate(self, alert: AlertEvent) -> bool:
        cooldown = self.COOLDOWNS.get(alert.alert_type, 60)
        key = f"alert_dedup:{alert.alert_type}:{alert.symbol}"
        if self.redis.exists(key):
            return True
        self.redis.setex(key, cooldown, "1")
        return False


class NotificationRouter:
    """Routes alerts to appropriate notification channels based on severity."""

    def __init__(self):
        self.webhook_url = os.getenv("WEBHOOK_URL", "")

    def route(self, alert: AlertEvent):
        start = time.perf_counter()

        if alert.severity == "CRITICAL":
            self._send_webhook(alert)
            self._log_critical(alert)
        elif alert.severity == "HIGH":
            self._send_webhook(alert)
            self._log_high(alert)
        elif alert.severity == "MEDIUM":
            self._log_medium(alert)
        else:
            self._log_low(alert)

        DELIVERY_LATENCY.observe(time.perf_counter() - start)

    def _send_webhook(self, alert: AlertEvent):
        if not self.webhook_url:
            logger.info(f"[WEBHOOK STUB] Would POST alert: {alert.alert_type} - {alert.symbol}")
            ALERTS_DELIVERED.labels(channel="webhook_stub").inc()
            return

        payload = {
            "text":  f"*[{alert.severity}] {alert.alert_type}*\n{alert.message}",
            "alert": {
                "id":      alert.alert_id,
                "type":    alert.alert_type,
                "severity": alert.severity,
                "symbol":  alert.symbol,
                "ts":      alert.triggered_at,
            }
        }
        try:
            r = requests.post(self.webhook_url, json=payload, timeout=5)
            r.raise_for_status()
            ALERTS_DELIVERED.labels(channel="webhook").inc()
            logger.info(f"Webhook delivered: {alert.alert_type}/{alert.symbol}")
        except Exception as e:
            logger.error(f"Webhook delivery failed: {e}")

    def _log_critical(self, alert: AlertEvent):
        logger.critical(f"🚨 CRITICAL | {alert.alert_type} | {alert.symbol} | {alert.message}")
        ALERTS_DELIVERED.labels(channel="log_critical").inc()

    def _log_high(self, alert: AlertEvent):
        logger.error(f"🔴 HIGH | {alert.alert_type} | {alert.symbol} | {alert.message}")
        ALERTS_DELIVERED.labels(channel="log_high").inc()

    def _log_medium(self, alert: AlertEvent):
        logger.warning(f"🟡 MEDIUM | {alert.alert_type} | {alert.symbol} | {alert.message}")
        ALERTS_DELIVERED.labels(channel="log_medium").inc()

    def _log_low(self, alert: AlertEvent):
        logger.info(f"🔵 LOW | {alert.alert_type} | {alert.symbol} | {alert.message}")
        ALERTS_DELIVERED.labels(channel="log_low").inc()


class AlertEngine:
    def __init__(self):
        self.kafka_servers  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic_alerts   = os.getenv("KAFKA_TOPIC_ALERTS", "trade-alerts")
        self.consumer_group = os.getenv("KAFKA_CONSUMER_GROUP", "alert-engine-group")
        self.redis_host     = os.getenv("REDIS_HOST", "localhost")
        self.redis_port     = int(os.getenv("REDIS_PORT", "6379"))
        self.postgres_dsn   = os.getenv("POSTGRES_DSN", "")
        self._running       = True

        self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
        self.dedup         = DeduplicationFilter(self.redis_client)
        self.router        = NotificationRouter()
        self.cb_state      = CircuitBreakerState()

        self.consumer = Consumer({
            "bootstrap.servers": self.kafka_servers,
            "group.id":          self.consumer_group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        self.consumer.subscribe([self.topic_alerts])

        # PostgreSQL (optional - alert-engine also stores critical alerts)
        self.pg_conn: Optional[psycopg2.extensions.connection] = None
        if self.postgres_dsn:
            try:
                self.pg_conn = psycopg2.connect(self.postgres_dsn)
                self.pg_conn.autocommit = True
                logger.info("PostgreSQL connected for alert persistence.")
            except Exception as e:
                logger.warning(f"PostgreSQL unavailable: {e} (continuing without)")

        logger.info("Alert engine initialized.")

    def _parse_alert(self, raw: bytes) -> Optional[AlertEvent]:
        try:
            d = json.loads(raw)
            return AlertEvent(
                alert_id    = d["alert_id"],
                alert_type  = d["alert_type"],
                severity    = d["severity"],
                symbol      = d["symbol"],
                message     = d["message"],
                details     = d.get("details", {}),
                triggered_at = d["triggered_at"],
            )
        except Exception as e:
            logger.error(f"Failed to parse alert: {e}")
            return None

    def _persist_alert(self, alert: AlertEvent):
        if not self.pg_conn:
            return
        try:
            cur = self.pg_conn.cursor()
            cur.execute("""
                INSERT INTO trade_alerts (alert_id, alert_type, severity, symbol, message, details, triggered_at)
                VALUES (%s::uuid, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (alert_id) DO NOTHING
            """, (alert.alert_id, alert.alert_type, alert.severity, alert.symbol,
                  alert.message, json.dumps(alert.details), alert.triggered_at))
            cur.close()
        except Exception as e:
            logger.error(f"Alert persistence error: {e}")

    def _update_redis_alert_count(self, alert: AlertEvent):
        """Track alert counts per symbol in Redis for dashboard KPIs."""
        pipe = self.redis_client.pipeline()
        pipe.incr(f"alerts:count:total")
        pipe.incr(f"alerts:count:{alert.symbol}")
        pipe.incr(f"alerts:count:{alert.severity}")
        pipe.lpush(f"alerts:recent", json.dumps({
            "id": alert.alert_id, "type": alert.alert_type, "severity": alert.severity,
            "symbol": alert.symbol, "msg": alert.message, "ts": alert.triggered_at,
        }))
        pipe.ltrim(f"alerts:recent", 0, 99)
        pipe.expire(f"alerts:recent", 86400)
        pipe.execute()

    def _handle_circuit_breaker(self, alert: AlertEvent):
        if alert.alert_type == "CIRCUIT_BREAKER":
            self.cb_state.trip(alert.symbol)
        elif alert.alert_type == "PRICE_SPIKE":
            state = self.cb_state.check(alert.symbol)
            if state != "CLOSED":
                logger.info(f"CB is {state} for {alert.symbol} — escalating spike to CRITICAL")
                alert.severity = "CRITICAL"

    def run(self):
        signal.signal(signal.SIGTERM, lambda *_: self.stop())
        signal.signal(signal.SIGINT,  lambda *_: self.stop())

        start_http_server(8003)
        logger.info("Prometheus metrics on :8003/metrics")
        logger.info("Alert engine running...")

        while self._running:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            alert = self._parse_alert(msg.value())
            if not alert:
                continue

            ALERTS_RECEIVED.labels(alert_type=alert.alert_type).inc()

            # Deduplication
            if self.dedup.is_duplicate(alert):
                ALERTS_DEDUPED.inc()
                continue

            # Circuit breaker state machine
            self._handle_circuit_breaker(alert)

            # Route notifications
            self.router.route(alert)

            # Persist + update Redis
            self._persist_alert(alert)
            self._update_redis_alert_count(alert)

        self.consumer.close()
        if self.pg_conn:
            self.pg_conn.close()
        logger.info("Alert engine shut down cleanly.")

    def stop(self):
        logger.info("Shutdown signal received.")
        self._running = False


if __name__ == "__main__":
    AlertEngine().run()
