#!/usr/bin/env bash
# Start mock server, load securities and prices, then shut down.
set -e
cd "$(dirname "$0")"

if [ ! -d venv ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    venv/bin/pip install -r requirements.txt
fi

PY="venv/bin/python3"

cleanup() {
    if [ -n "$SERVER_PID" ]; then
        echo ""
        echo "Stopping mock server (PID $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
    fi
}
trap cleanup EXIT

echo "=== Starting mock gRPC server ==="
API_URL=localhost $PY mock_server.py &
SERVER_PID=$!
sleep 2

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "ERROR: Mock server failed to start"
    exit 1
fi
echo "Mock server running (PID $SERVER_PID)"
echo ""

echo "=== Step 1: Create equity securities (mock data) ==="
API_URL=localhost $PY equity/equities.py --mock --index "Dow Jones" 2>&1 | grep -E "^(Fetching|  \d|  Created|  Dow|Equity|  TOTAL|---)"
echo ""

echo "=== Step 2: Upload equity prices (mock data) ==="
API_URL=localhost $PY equity/yahoo.py --mock --tickers AAPL MSFT NVDA 2>&1 | grep -v "^SecurityService connecting"
echo ""

echo "=== Step 3: Preview Treasury bond prices (mock data, dry-run) ==="
API_URL=localhost $PY bond/fedinvest.py --mock --dry-run 2>&1 | tail -20
echo ""

echo "=== Pipeline complete ==="
