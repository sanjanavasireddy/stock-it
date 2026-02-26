#!/usr/bin/env bash
# ─── Quick-start script ───────────────────────────────────────────────────────
set -e

echo "🚀 Starting Real-Time Trade Pipeline..."

# Check Docker is running
if ! docker info &>/dev/null; then
  echo "❌ Docker is not running. Please start Docker Desktop."
  exit 1
fi

# Pull & start all services
docker compose up -d --build

echo ""
echo "⏳ Waiting for Kafka to be ready..."
until docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:29092 &>/dev/null 2>&1; do
  sleep 3
  printf "."
done
echo ""

echo ""
echo "✅ All services started!"
echo ""
echo "  📊 Dashboard     → open dashboard/index.html in your browser"
echo "  📡 API Docs      → http://localhost:8000/docs"
echo "  🔍 Kafka UI      → http://localhost:8090"
echo "  📈 Grafana       → http://localhost:3000  (admin / admin123)"
echo "  🔥 Prometheus    → http://localhost:9090"
echo ""
echo "  To tail all logs: docker compose logs -f"
echo "  To stop:          docker compose down"
echo "  To stop + wipe:   docker compose down -v"
echo ""
