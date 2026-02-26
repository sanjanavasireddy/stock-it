"""
Unit tests for the alert engine components.
"""
import pytest
import json
from unittest.mock import MagicMock, patch
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'alerts'))

from alert_engine import AlertEvent, DeduplicationFilter, CircuitBreakerState, NotificationRouter


def make_alert(alert_type="PRICE_SPIKE", severity="HIGH", symbol="AAPL") -> AlertEvent:
    return AlertEvent(
        alert_id     = "test-alert-001",
        alert_type   = alert_type,
        severity     = severity,
        symbol       = symbol,
        message      = f"Test alert for {symbol}",
        details      = {"change_pct": 0.06},
        triggered_at = "2024-01-01T10:00:00+00:00",
    )


# ─── DeduplicationFilter Tests ────────────────────────────────────────────────
class TestDeduplicationFilter:

    def _make_redis(self, exists_val=False):
        r = MagicMock()
        r.exists.return_value = exists_val
        r.setex.return_value  = True
        return r

    def test_first_alert_not_duplicate(self):
        redis_mock = self._make_redis(exists_val=False)
        f = DeduplicationFilter(redis_mock)
        alert = make_alert()
        assert f.is_duplicate(alert) is False

    def test_second_alert_is_duplicate(self):
        redis_mock = self._make_redis(exists_val=True)
        f = DeduplicationFilter(redis_mock)
        alert = make_alert()
        assert f.is_duplicate(alert) is True

    def test_sets_redis_key_on_first_alert(self):
        redis_mock = self._make_redis(exists_val=False)
        f = DeduplicationFilter(redis_mock)
        alert = make_alert(alert_type="PRICE_SPIKE")
        f.is_duplicate(alert)
        redis_mock.setex.assert_called_once()
        call_args = redis_mock.setex.call_args[0]
        assert "PRICE_SPIKE" in call_args[0]
        assert "AAPL" in call_args[0]
        assert call_args[1] == 60    # PRICE_SPIKE cooldown

    def test_circuit_breaker_longer_cooldown(self):
        redis_mock = self._make_redis(exists_val=False)
        f = DeduplicationFilter(redis_mock)
        alert = make_alert(alert_type="CIRCUIT_BREAKER", severity="CRITICAL")
        f.is_duplicate(alert)
        cooldown = redis_mock.setex.call_args[0][1]
        assert cooldown == 300    # 5 minutes

    def test_volume_spike_shorter_cooldown(self):
        redis_mock = self._make_redis(exists_val=False)
        f = DeduplicationFilter(redis_mock)
        alert = make_alert(alert_type="VOLUME_SPIKE", severity="MEDIUM")
        f.is_duplicate(alert)
        cooldown = redis_mock.setex.call_args[0][1]
        assert cooldown == 30


# ─── CircuitBreakerState Tests ────────────────────────────────────────────────
class TestCircuitBreakerState:

    def test_initially_closed(self):
        cb = CircuitBreakerState()
        assert cb.check("AAPL") == "CLOSED"

    def test_trip_opens_breaker(self):
        cb = CircuitBreakerState()
        cb.trip("AAPL")
        assert cb.check("AAPL") == "OPEN"

    def test_reset_closes_breaker(self):
        cb = CircuitBreakerState()
        cb.trip("AAPL")
        cb.reset("AAPL")
        assert cb.check("AAPL") == "CLOSED"

    def test_independent_per_symbol(self):
        cb = CircuitBreakerState()
        cb.trip("AAPL")
        assert cb.check("AAPL")  == "OPEN"
        assert cb.check("GOOGL") == "CLOSED"

    def test_half_open_after_timeout(self):
        from datetime import timedelta
        cb = CircuitBreakerState()
        cb.trip("AAPL")
        # Backdate the open time by 6 minutes
        cb._open_at["AAPL"] = cb._open_at["AAPL"] - timedelta(minutes=6)
        assert cb.check("AAPL") == "HALF_OPEN"


# ─── NotificationRouter Tests ─────────────────────────────────────────────────
class TestNotificationRouter:

    def test_critical_calls_webhook(self):
        router = NotificationRouter()
        router.webhook_url = ""   # stub mode
        alert = make_alert(severity="CRITICAL", alert_type="CIRCUIT_BREAKER")
        # Should not raise; logs stub webhook call
        router.route(alert)

    def test_low_alert_does_not_call_webhook(self):
        router = NotificationRouter()
        router.webhook_url = "http://test-webhook"
        alert = make_alert(severity="LOW")
        with patch("requests.post") as mock_post:
            router.route(alert)
            mock_post.assert_not_called()

    def test_high_alert_calls_webhook_when_configured(self):
        router = NotificationRouter()
        router.webhook_url = "http://test-webhook"
        alert = make_alert(severity="HIGH")
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        with patch("requests.post", return_value=mock_response) as mock_post:
            router.route(alert)
            mock_post.assert_called_once()
            call_kwargs = mock_post.call_args
            assert call_kwargs[0][0] == "http://test-webhook"
