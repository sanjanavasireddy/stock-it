"""
Shared Kafka configuration utilities.
"""
from typing import Dict


def base_producer_config(bootstrap_servers: str, client_id: str) -> Dict:
    return {
        "bootstrap.servers":            bootstrap_servers,
        "client.id":                    client_id,
        "acks":                         "all",
        "retries":                      5,
        "retry.backoff.ms":             500,
        "compression.type":             "snappy",
        "linger.ms":                    5,
        "batch.size":                   65536,
        "queue.buffering.max.kbytes":   32768,
        "delivery.timeout.ms":          120000,
    }


def base_consumer_config(bootstrap_servers: str, group_id: str,
                          auto_offset_reset: str = "latest") -> Dict:
    return {
        "bootstrap.servers":        bootstrap_servers,
        "group.id":                 group_id,
        "auto.offset.reset":        auto_offset_reset,
        "enable.auto.commit":       False,
        "max.poll.interval.ms":     300000,
        "session.timeout.ms":       30000,
        "heartbeat.interval.ms":    3000,
        "fetch.min.bytes":          1024,
        "fetch.wait.max.ms":        500,
    }


TOPICS = {
    "raw_trades":       "raw-trades",
    "trade_alerts":     "trade-alerts",
    "trade_aggregates": "trade-aggregates",
    "order_book":       "order-book",
}
