#!/bin/bash
# Wrapper to launch both API and Worker processes
node src/server.js &
API_PID=$!
node src/worker.js &
WORKER_PID=$!

# Trap signals to ensure both are killed
trap "kill $API_PID $WORKER_PID; exit" SIGINT SIGTERM
wait $API_PID $WORKER_PID
