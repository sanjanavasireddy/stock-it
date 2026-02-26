"""
Unit tests for the stream processor components.
Run with: python -m pytest tests/ -v
"""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'processor'))

from stream_processor import (
    TradeEvent, WindowManager, AnomalyDetector, OHLCV
)


def make_trade(symbol="AAPL", price=100.0, quantity=100.0, side="BUY",
               ts: datetime = None) -> TradeEvent:
    if ts is None:
        ts = datetime.now(timezone.utc)
    return TradeEvent(
        trade_id  = "test-trade-001",
        symbol    = symbol,
        price     = price,
        quantity  = quantity,
        side      = side,
        exchange  = "NASDAQ",
        trader_id = "trader_001",
        timestamp = ts.isoformat(),
        sequence  = 1,
        metadata  = {},
    )


# ─── WindowManager Tests ──────────────────────────────────────────────────────
class TestWindowManager:

    def test_first_trade_creates_window(self):
        wm = WindowManager()
        trade = make_trade(price=100.0, quantity=10.0)
        completed = wm.update(trade)
        assert completed == []
        assert "AAPL" in wm._windows
        assert 60 in wm._windows["AAPL"]

    def test_window_ohlcv_updates(self):
        wm = WindowManager()
        t0 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

        trade1 = make_trade(price=100.0, quantity=10.0, ts=t0)
        trade2 = make_trade(price=105.0, quantity=20.0, side="SELL", ts=t0 + timedelta(seconds=5))
        trade3 = make_trade(price=95.0,  quantity=5.0,  ts=t0 + timedelta(seconds=10))

        wm.update(trade1)
        wm.update(trade2)
        wm.update(trade3)

        w = wm._windows["AAPL"][60]
        assert w.open  == 100.0
        assert w.high  == 105.0
        assert w.low   == 95.0
        assert w.close == 95.0
        assert w.volume == 35.0
        assert w.trade_count == 3

    def test_vwap_calculation(self):
        wm = WindowManager()
        t0 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

        wm.update(make_trade(price=100.0, quantity=10.0, ts=t0))
        wm.update(make_trade(price=200.0, quantity=10.0, ts=t0 + timedelta(seconds=5)))

        w = wm._windows["AAPL"][60]
        expected_vwap = (100.0*10 + 200.0*10) / (10 + 10)   # = 150.0
        assert abs(w.vwap - expected_vwap) < 0.0001

    def test_window_rollover_returns_completed(self):
        wm = WindowManager()
        t0 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 1, 10, 1, 0, tzinfo=timezone.utc)   # next 1-min window

        wm.update(make_trade(price=100.0, ts=t0))
        completed = wm.update(make_trade(price=105.0, ts=t1))

        # 1m, 5m, 15m windows roll at different times; only 1m should roll here
        assert any(c.window_sec == 60 for c in completed)

    def test_buy_sell_volume_tracking(self):
        wm = WindowManager()
        t0 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

        wm.update(make_trade(side="BUY",  quantity=100.0, ts=t0))
        wm.update(make_trade(side="SELL", quantity=50.0,  ts=t0 + timedelta(seconds=5)))

        w = wm._windows["AAPL"][60]
        assert w.buy_volume  == 100.0
        assert w.sell_volume == 50.0


# ─── AnomalyDetector Tests ────────────────────────────────────────────────────
class TestAnomalyDetector:

    def test_no_alert_insufficient_history(self):
        detector = AnomalyDetector(price_spike_threshold=0.05)
        # Only 4 trades — below lookback minimum of 5
        for _ in range(4):
            alerts = detector.check(make_trade(price=100.0))
        assert all(len(a) == 0 for a in [alerts])

    def test_price_spike_alert(self):
        detector = AnomalyDetector(price_spike_threshold=0.05, volume_spike_multiplier=999)
        # Establish baseline of 100.0
        for _ in range(10):
            detector.check(make_trade(price=100.0, quantity=10.0))
        # Spike to 110.0 (10% move)
        alerts = detector.check(make_trade(price=110.0, quantity=10.0))
        assert len(alerts) >= 1
        types = [a.alert_type for a in alerts]
        assert "CIRCUIT_BREAKER" in types or "PRICE_SPIKE" in types

    def test_circuit_breaker_triggered_on_large_spike(self):
        detector = AnomalyDetector(price_spike_threshold=0.05)
        for _ in range(10):
            detector.check(make_trade(price=100.0))
        # 15% move → circuit breaker
        alerts = detector.check(make_trade(price=115.0))
        cb_alerts = [a for a in alerts if a.alert_type == "CIRCUIT_BREAKER"]
        assert len(cb_alerts) == 1
        assert cb_alerts[0].severity == "CRITICAL"

    def test_volume_spike_alert(self):
        detector = AnomalyDetector(price_spike_threshold=0.99, volume_spike_multiplier=3.0)
        # Establish baseline volume of 10
        for _ in range(10):
            detector.check(make_trade(price=100.0, quantity=10.0))
        # Spike volume to 100 (10x)
        alerts = detector.check(make_trade(price=100.0, quantity=100.0))
        vol_alerts = [a for a in alerts if a.alert_type == "VOLUME_SPIKE"]
        assert len(vol_alerts) == 1
        assert vol_alerts[0].severity == "MEDIUM"

    def test_no_alert_normal_conditions(self):
        detector = AnomalyDetector(price_spike_threshold=0.05, volume_spike_multiplier=3.0)
        alerts_all = []
        for i in range(20):
            price = 100.0 + (i % 3) * 0.1   # tiny movements
            alerts_all.extend(detector.check(make_trade(price=price, quantity=10.0)))
        assert all(a.alert_type not in ("PRICE_SPIKE", "CIRCUIT_BREAKER") for a in alerts_all)

    def test_separate_state_per_symbol(self):
        detector = AnomalyDetector(price_spike_threshold=0.05)
        for _ in range(10):
            detector.check(make_trade(symbol="AAPL",  price=100.0))
            detector.check(make_trade(symbol="GOOGL", price=50.0))
        # Spike AAPL but not GOOGL
        aapl_alerts  = detector.check(make_trade(symbol="AAPL",  price=115.0))
        googl_alerts = detector.check(make_trade(symbol="GOOGL", price=51.0))
        assert len(aapl_alerts)  >= 1
        assert len(googl_alerts) == 0
