-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ─── Raw Trades Table (Hypertable for time-series) ────────────────────────────
CREATE TABLE IF NOT EXISTS trades (
    id              BIGSERIAL,
    trade_id        UUID NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    price           NUMERIC(18, 6) NOT NULL,
    quantity        NUMERIC(18, 6) NOT NULL,
    trade_value     NUMERIC(24, 6) GENERATED ALWAYS AS (price * quantity) STORED,
    side            VARCHAR(4) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    exchange        VARCHAR(20) NOT NULL,
    trader_id       VARCHAR(50),
    timestamp       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ,
    PRIMARY KEY (id, timestamp)
);

-- Convert to hypertable (partitioned by time)
SELECT create_hypertable('trades', 'timestamp', if_not_exists => TRUE);

-- ─── Trade Aggregates Table ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_aggregates (
    symbol          VARCHAR(20) NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    window_seconds  INTEGER NOT NULL,
    trade_count     INTEGER NOT NULL,
    total_volume    NUMERIC(24, 6) NOT NULL,
    total_value     NUMERIC(30, 6) NOT NULL,
    vwap            NUMERIC(18, 6) NOT NULL,
    open_price      NUMERIC(18, 6) NOT NULL,
    high_price      NUMERIC(18, 6) NOT NULL,
    low_price       NUMERIC(18, 6) NOT NULL,
    close_price     NUMERIC(18, 6) NOT NULL,
    buy_volume      NUMERIC(24, 6) NOT NULL DEFAULT 0,
    sell_volume     NUMERIC(24, 6) NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, window_start, window_seconds)
);

SELECT create_hypertable('trade_aggregates', 'window_start', if_not_exists => TRUE);

-- ─── Alerts Table ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_alerts (
    id              BIGSERIAL PRIMARY KEY,
    alert_id        UUID NOT NULL UNIQUE,
    alert_type      VARCHAR(50) NOT NULL,
    severity        VARCHAR(10) NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    symbol          VARCHAR(20) NOT NULL,
    message         TEXT NOT NULL,
    details         JSONB,
    triggered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged    BOOLEAN NOT NULL DEFAULT FALSE,
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by VARCHAR(100)
);

CREATE INDEX idx_alerts_symbol ON trade_alerts(symbol);
CREATE INDEX idx_alerts_type ON trade_alerts(alert_type);
CREATE INDEX idx_alerts_severity ON trade_alerts(severity);
CREATE INDEX idx_alerts_triggered ON trade_alerts(triggered_at DESC);
CREATE INDEX idx_alerts_ack ON trade_alerts(acknowledged) WHERE NOT acknowledged;

-- ─── Indexes for performance ──────────────────────────────────────────────────
CREATE INDEX idx_trades_symbol ON trades(symbol, timestamp DESC);
CREATE INDEX idx_trades_exchange ON trades(exchange, timestamp DESC);
CREATE INDEX idx_trades_side ON trades(side, timestamp DESC);

-- ─── Continuous Aggregates (TimescaleDB feature) ──────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS trades_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', timestamp) AS bucket,
    symbol,
    COUNT(*) AS trade_count,
    SUM(quantity) AS total_volume,
    SUM(price * quantity) / SUM(quantity) AS vwap,
    FIRST(price, timestamp) AS open_price,
    MAX(price) AS high_price,
    MIN(price) AS low_price,
    LAST(price, timestamp) AS close_price
FROM trades
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('trades_1min',
    start_offset => INTERVAL '10 minutes',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);

-- ─── Retention Policy (keep 30 days of raw data) ──────────────────────────────
SELECT add_retention_policy('trades', INTERVAL '30 days', if_not_exists => TRUE);

-- ─── Sample Data for testing ──────────────────────────────────────────────────
INSERT INTO trades (trade_id, symbol, price, quantity, side, exchange, trader_id, timestamp)
VALUES
    (gen_random_uuid(), 'AAPL', 182.50, 100, 'BUY', 'NASDAQ', 'trader_001', NOW() - INTERVAL '5 minutes'),
    (gen_random_uuid(), 'AAPL', 182.75, 200, 'SELL', 'NASDAQ', 'trader_002', NOW() - INTERVAL '4 minutes'),
    (gen_random_uuid(), 'GOOGL', 141.20, 50, 'BUY', 'NASDAQ', 'trader_003', NOW() - INTERVAL '3 minutes'),
    (gen_random_uuid(), 'MSFT', 415.30, 75, 'BUY', 'NASDAQ', 'trader_001', NOW() - INTERVAL '2 minutes'),
    (gen_random_uuid(), 'BTC-USD', 52340.00, 0.5, 'BUY', 'COINBASE', 'trader_004', NOW() - INTERVAL '1 minute')
ON CONFLICT DO NOTHING;
