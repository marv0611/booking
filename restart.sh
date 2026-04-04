#!/bin/bash
# Night Pulse — kill and restart server.py
# Usage: bash restart.sh

PORT=8000

echo "🔴 Stopping server on port $PORT..."
pkill -f "python3 server.py" 2>/dev/null
lsof -ti :$PORT | xargs kill -9 2>/dev/null
sleep 1

echo "🟢 Starting server..."
nohup python3 server.py > server.log 2>&1 &

sleep 1
if lsof -ti :$PORT > /dev/null 2>&1; then
  echo "✅ Server running on http://localhost:$PORT (PID $(lsof -ti :$PORT))"
  echo "📄 Logs: tail -f server.log"
else
  echo "❌ Server failed to start — check server.log"
fi
