#!/bin/bash
# Stock-IT Pipeline Startup Script

cd "$(dirname "$0")"

echo "Starting pipeline..."
DOCKER_API_VERSION=1.44 docker compose up -d

echo "Waiting for API to be ready..."
until curl -s http://localhost:8088/health > /dev/null 2>&1; do
  sleep 2
done

echo "Starting ngrok tunnel..."
pkill -f "ngrok http" 2>/dev/null
sleep 1
ngrok http 8088 --log=stdout > /tmp/ngrok.log 2>&1 &
sleep 5

URL=$(curl -s http://localhost:4040/api/tunnels | python3 -c "import sys,json; t=json.load(sys.stdin)['tunnels']; print(t[0]['public_url']) if t else print('tunnel not ready')")

echo ""
echo "======================================"
echo "  Stock-IT is LIVE"
echo "  Ngrok URL : $URL"
echo "  Short link: https://tinyurl.com/trade-monitor-stock-it"
echo ""
echo "  Update TinyURL destination to:"
echo "  $URL"
echo "======================================"

open "$URL"
