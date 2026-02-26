"""
Real-Time Trade API
FastAPI application exposing:
  - REST endpoints for historical data, aggregates, and alerts
  - WebSocket endpoint for live trade/alert streaming
  - Server-Sent Events (SSE) for price feeds
  - Prometheus metrics instrumentation
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, List, Optional, Set
from contextlib import asynccontextmanager

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from prometheus_fastapi_instrumentator import Instrumentator
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("trade-api")

# ─── Config ───────────────────────────────────────────────────────────────────
REDIS_HOST     = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
POSTGRES_DSN   = os.getenv("POSTGRES_DSN", "postgresql://trader:trader_secret@localhost:5432/trades_db")
KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_ALERTS   = os.getenv("KAFKA_TOPIC_ALERTS", "trade-alerts")
TOPIC_AGGS     = os.getenv("KAFKA_TOPIC_AGGREGATES", "trade-aggregates")


# ─── WebSocket Connection Manager ────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self._connections: Dict[str, Set[WebSocket]] = {}   # channel -> set of sockets

    async def connect(self, websocket: WebSocket, channel: str):
        await websocket.accept()
        self._connections.setdefault(channel, set()).add(websocket)
        logger.info(f"WS connected: channel={channel} total={len(self._connections.get(channel, set()))}")

    def disconnect(self, websocket: WebSocket, channel: str):
        self._connections.get(channel, set()).discard(websocket)

    async def broadcast(self, channel: str, message: dict):
        dead = set()
        for ws in self._connections.get(channel, set()):
            try:
                await ws.send_json(message)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self._connections.get(channel, set()).discard(ws)

    async def broadcast_all(self, message: dict):
        for channel in list(self._connections.keys()):
            await self.broadcast(channel, message)


manager = ConnectionManager()
redis_client: Optional[aioredis.Redis] = None
pg_pool: Optional[asyncpg.Pool] = None


# ─── Kafka→WebSocket Bridge ───────────────────────────────────────────────────
async def kafka_to_websocket(topics: List[str], channel: str, group_id: str):
    """Background task: consume Kafka topic and broadcast to WebSocket clients."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_SERVERS,
        "group.id":          group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(topics)
    logger.info(f"Kafka bridge started: topics={topics} channel={channel}")

    try:
        while True:
            msg = consumer.poll(timeout=0.1)
            if msg is None:
                await asyncio.sleep(0.01)
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                data = json.loads(msg.value())
                await manager.broadcast(channel, {"type": channel, "data": data})
            except Exception as e:
                logger.error(f"Broadcast error: {e}")
    finally:
        consumer.close()


# ─── App Lifespan ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, pg_pool

    redis_client = await aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True
    )
    pg_pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=2, max_size=10)
    logger.info("Database connections established.")

    # Start Kafka bridge tasks
    tasks = [
        asyncio.create_task(kafka_to_websocket([TOPIC_ALERTS],   "alerts",     "api-alerts-group")),
        asyncio.create_task(kafka_to_websocket([TOPIC_AGGS],     "aggregates", "api-aggs-group")),
    ]

    yield

    for t in tasks:
        t.cancel()
    await redis_client.aclose()
    await pg_pool.close()


app = FastAPI(
    title="Real-Time Trade Monitoring API",
    description="Streaming financial trade data pipeline",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
Instrumentator().instrument(app).expose(app)


# ─── Pydantic Models ──────────────────────────────────────────────────────────
class TradeResponse(BaseModel):
    trade_id:   str
    symbol:     str
    price:      float
    quantity:   float
    trade_value: float
    side:       str
    exchange:   str
    timestamp:  str

class AlertResponse(BaseModel):
    alert_id:    str
    alert_type:  str
    severity:    str
    symbol:      str
    message:     str
    triggered_at: str
    acknowledged: bool

class OHLCVResponse(BaseModel):
    symbol:      str
    window_sec:  int
    window_start: str
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float
    vwap:   float
    trades: int

class PriceResponse(BaseModel):
    symbol: str
    price:  float
    timestamp: str
    side:   str


# ─── REST Endpoints ───────────────────────────────────────────────────────────
@app.get("/", response_class=FileResponse)
async def dashboard():
    return FileResponse("static/index.html", media_type="text/html")

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/v1/prices", response_model=List[PriceResponse])
async def get_prices(symbols: Optional[str] = Query(None, description="Comma-separated symbols")):
    """Get latest price for all or specific symbols from Redis."""
    if symbols:
        sym_list = [s.strip().upper() for s in symbols.split(",")]
    else:
        keys = await redis_client.keys("price:*")
        sym_list = [k.replace("price:", "") for k in keys]

    results = []
    for sym in sym_list:
        data = await redis_client.hgetall(f"price:{sym}")
        if data:
            results.append(PriceResponse(
                symbol=sym, price=float(data["price"]),
                timestamp=data["ts"], side=data["side"]
            ))
    return results


@app.get("/api/v1/prices/{symbol}", response_model=PriceResponse)
async def get_price(symbol: str):
    symbol = symbol.upper()
    data = await redis_client.hgetall(f"price:{symbol}")
    if not data:
        raise HTTPException(status_code=404, detail=f"No price data for {symbol}")
    return PriceResponse(symbol=symbol, price=float(data["price"]), timestamp=data["ts"], side=data["side"])


@app.get("/api/v1/trades/{symbol}", response_model=List[dict])
async def get_recent_trades(symbol: str, limit: int = Query(50, ge=1, le=100)):
    """Get recent trades for a symbol from Redis ring buffer."""
    symbol = symbol.upper()
    raw = await redis_client.lrange(f"trades:{symbol}", 0, limit - 1)
    trades = []
    for r in raw:
        t = json.loads(r)
        trades.append({"symbol": symbol, "price": t["p"], "quantity": t["q"], "side": t["s"], "timestamp": t["t"]})
    return trades


@app.get("/api/v1/ohlcv/{symbol}", response_model=OHLCVResponse)
async def get_ohlcv(symbol: str, window_seconds: int = Query(60, description="Window size in seconds")):
    """Get current OHLCV window for a symbol from Redis."""
    symbol = symbol.upper()
    data = await redis_client.hgetall(f"ohlcv:{symbol}:{window_seconds}")
    if not data:
        raise HTTPException(status_code=404, detail=f"No OHLCV data for {symbol}")
    return OHLCVResponse(
        symbol=symbol, window_sec=window_seconds,
        window_start=data.get("window_start", ""),
        open=float(data["open"]), high=float(data["high"]),
        low=float(data["low"]),   close=float(data["close"]),
        volume=float(data["volume"]), vwap=float(data["vwap"]),
        trades=int(data["trades"])
    )


@app.get("/api/v1/trades/history", response_model=List[TradeResponse])
async def get_trade_history(
    symbol:  Optional[str] = None,
    limit:   int = Query(100, ge=1, le=1000),
    offset:  int = Query(0, ge=0),
):
    """Query historical trades from PostgreSQL."""
    query = "SELECT trade_id::text, symbol, price, quantity, price*quantity as trade_value, side, exchange, timestamp::text FROM trades"
    params = []
    if symbol:
        query += " WHERE symbol = $1"
        params.append(symbol.upper())
    query += f" ORDER BY timestamp DESC LIMIT ${len(params)+1} OFFSET ${len(params)+2}"
    params.extend([limit, offset])

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [TradeResponse(**dict(r)) for r in rows]


@app.get("/api/v1/alerts", response_model=List[AlertResponse])
async def get_alerts(
    symbol:       Optional[str] = None,
    severity:     Optional[str] = None,
    unacked_only: bool = Query(False),
    limit:        int  = Query(50, ge=1, le=500),
):
    """Query alerts from PostgreSQL."""
    conditions = []
    params = []

    if symbol:
        params.append(symbol.upper())
        conditions.append(f"symbol = ${len(params)}")
    if severity:
        params.append(severity.upper())
        conditions.append(f"severity = ${len(params)}")
    if unacked_only:
        conditions.append("acknowledged = FALSE")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.extend([limit])
    query = f"""
        SELECT alert_id::text, alert_type, severity, symbol, message, triggered_at::text, acknowledged
        FROM trade_alerts
        {where}
        ORDER BY triggered_at DESC
        LIMIT ${len(params)}
    """

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [AlertResponse(**dict(r)) for r in rows]


@app.patch("/api/v1/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: str, acknowledged_by: str = "api-user"):
    async with pg_pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE trade_alerts
            SET acknowledged = TRUE, acknowledged_at = NOW(), acknowledged_by = $1
            WHERE alert_id = $2::uuid AND acknowledged = FALSE
        """, acknowledged_by, alert_id)
    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail="Alert not found or already acknowledged")
    return {"status": "acknowledged", "alert_id": alert_id}


@app.get("/api/v1/stats/movers")
async def get_top_movers():
    """Top gainers/losers based on price vs VWAP deviation."""
    keys = await redis_client.keys("price:*")
    movers = []
    for key in keys:
        sym = key.replace("price:", "")
        price_data = await redis_client.hgetall(key)
        ohlcv = await redis_client.hgetall(f"ohlcv:{sym}:300")   # 5m window
        if not price_data or not ohlcv:
            continue
        current = float(price_data.get("price", 0))
        open_price = float(ohlcv.get("open", current))
        change_pct = ((current - open_price) / open_price * 100) if open_price else 0
        movers.append({
            "symbol":     sym,
            "price":      current,
            "open":       open_price,
            "change_pct": round(change_pct, 4),
            "direction":  "up" if change_pct >= 0 else "down",
            "volume":     float(ohlcv.get("volume", 0)),
            "vwap":       float(ohlcv.get("vwap", 0)),
        })
    movers.sort(key=lambda x: abs(x["change_pct"]), reverse=True)
    return {"gainers": [m for m in movers if m["change_pct"] >= 0][:5],
            "losers":  [m for m in movers if m["change_pct"] <  0][:5]}


@app.get("/api/v1/orderbook/{symbol}")
async def get_order_book(symbol: str, depth: int = Query(10, ge=5, le=20)):
    """Simulated order book derived from recent trade prices in Redis."""
    symbol = symbol.upper()
    price_data = await redis_client.hgetall(f"price:{symbol}")
    if not price_data:
        raise HTTPException(status_code=404, detail=f"No data for {symbol}")

    mid = float(price_data["price"])
    tick = mid * 0.0001   # 1 basis point tick

    import random, math
    random.seed(int(mid * 1000) % 9999)

    bids, asks = [], []
    for i in range(1, depth + 1):
        bid_p = round(mid - tick * i, 6)
        ask_p = round(mid + tick * i, 6)
        bid_q = round(random.lognormvariate(math.log(100), 0.6), 2)
        ask_q = round(random.lognormvariate(math.log(100), 0.6), 2)
        bids.append({"price": bid_p, "quantity": bid_q, "total": round(bid_p * bid_q, 4)})
        asks.append({"price": ask_p, "quantity": ask_q, "total": round(ask_p * ask_q, 4)})

    return {"symbol": symbol, "mid": mid, "spread": round(tick * 2, 8),
            "bids": bids, "asks": asks,
            "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/api/v1/stats/summary")
async def get_market_summary():
    """Market-wide summary: total trades, volume, alert counts, active symbols."""
    keys = await redis_client.keys("price:*")
    total_trades = 0
    total_volume = 0.0
    symbol_count = len(keys)

    for key in keys:
        sym = key.replace("price:", "")
        count = await redis_client.get(f"count:daily:{sym}")
        ohlcv = await redis_client.hgetall(f"ohlcv:{sym}:60")
        total_trades += int(count or 0)
        total_volume += float(ohlcv.get("volume", 0)) if ohlcv else 0

    alert_count = await redis_client.get("alerts:count:total") or 0
    critical    = await redis_client.get("alerts:count:CRITICAL") or 0
    high        = await redis_client.get("alerts:count:HIGH") or 0

    return {
        "active_symbols": symbol_count,
        "total_trades":   total_trades,
        "total_volume_1m": round(total_volume, 2),
        "total_alerts":   int(alert_count),
        "critical_alerts": int(critical),
        "high_alerts":    int(high),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/v1/trades/{symbol}/pressure")
async def get_buy_sell_pressure(symbol: str):
    """Buy vs sell volume ratio from recent Redis trade buffer."""
    symbol = symbol.upper()
    raw = await redis_client.lrange(f"trades:{symbol}", 0, 99)
    buy_vol = sell_vol = 0.0
    buy_count = sell_count = 0
    for r in raw:
        t = json.loads(r)
        if t["s"] == "BUY":
            buy_vol += float(t["q"]); buy_count += 1
        else:
            sell_vol += float(t["q"]); sell_count += 1
    total = buy_vol + sell_vol
    return {
        "symbol":      symbol,
        "buy_volume":  round(buy_vol, 4),
        "sell_volume": round(sell_vol, 4),
        "buy_pct":     round(buy_vol / total * 100, 1) if total else 50.0,
        "sell_pct":    round(sell_vol / total * 100, 1) if total else 50.0,
        "buy_count":   buy_count,
        "sell_count":  sell_count,
    }


@app.get("/api/v1/prices/{symbol}/history")
async def get_price_history(symbol: str, limit: int = Query(100, ge=10, le=500)):
    """Ordered price time-series for a symbol from Redis sorted set."""
    symbol = symbol.upper()
    raw = await redis_client.zrange(f"prices:ts:{symbol}", -limit, -1, withscores=True)
    history = []
    for member, score in raw:
        try:
            d = json.loads(member)
            history.append({"price": d["p"], "timestamp": d["t"], "ms": int(score)})
        except Exception:
            continue
    return {"symbol": symbol, "history": history, "count": len(history)}


@app.get("/api/v1/stats/symbols")
async def get_symbol_stats():
    """Aggregate stats per symbol from Redis."""
    keys = await redis_client.keys("price:*")
    stats = {}
    for key in keys:
        sym = key.replace("price:", "")
        price_data = await redis_client.hgetall(key)
        count = await redis_client.get(f"count:daily:{sym}")
        ohlcv = await redis_client.hgetall(f"ohlcv:{sym}:60")
        stats[sym] = {
            "symbol":       sym,
            "price":        float(price_data.get("price", 0)),
            "daily_trades": int(count or 0),
            "vwap_1m":      float(ohlcv.get("vwap", 0)) if ohlcv else None,
            "volume_1m":    float(ohlcv.get("volume", 0)) if ohlcv else None,
        }
    return {"symbols": list(stats.values()), "count": len(stats)}


# ─── WebSocket Endpoints ──────────────────────────────────────────────────────
@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    """Stream live alerts to connected clients."""
    await manager.connect(websocket, "alerts")
    try:
        while True:
            await asyncio.sleep(30)    # keep-alive ping
            await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        manager.disconnect(websocket, "alerts")


@app.websocket("/ws/aggregates")
async def ws_aggregates(websocket: WebSocket):
    """Stream OHLCV window completions to connected clients."""
    await manager.connect(websocket, "aggregates")
    try:
        while True:
            await asyncio.sleep(30)
            await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        manager.disconnect(websocket, "aggregates")


@app.websocket("/ws/prices")
async def ws_prices(websocket: WebSocket):
    """Push live price updates from Redis every second."""
    await websocket.accept()
    try:
        while True:
            keys = await redis_client.keys("price:*")
            prices = {}
            for key in keys:
                sym = key.replace("price:", "")
                data = await redis_client.hgetall(key)
                if data:
                    prices[sym] = {"price": float(data["price"]), "ts": data["ts"], "side": data["side"]}
            await websocket.send_json({"type": "prices", "data": prices, "ts": datetime.now(timezone.utc).isoformat()})
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass


# ─── SSE Price Feed ───────────────────────────────────────────────────────────
@app.get("/api/v1/stream/prices")
async def sse_prices():
    """Server-Sent Events price stream."""
    async def generate() -> AsyncGenerator[str, None]:
        while True:
            keys = await redis_client.keys("price:*")
            prices = {}
            for key in keys:
                sym = key.replace("price:", "")
                data = await redis_client.hgetall(key)
                if data:
                    prices[sym] = float(data["price"])
            yield f"data: {json.dumps({'prices': prices, 'ts': datetime.now(timezone.utc).isoformat()})}\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(generate(), media_type="text/event-stream",
                              headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
