#!/usr/bin/env bash
# Start the mock gRPC server (SecurityService :8082, PriceService :8083)
set -e
cd "$(dirname "$0")"

if [ ! -d venv ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    venv/bin/pip install -r requirements.txt
fi

echo "Starting mock gRPC server..."
API_URL=localhost exec venv/bin/python3 mock_server.py --verbose "$@"
