#!/bin/bash
# Starts Docker + Kafka, the live producers and the dashboard, then shows real output and a screenshot.
cd "$(dirname "$0")"; mkdir -p logs data
OUT=demo_output.txt
: > "$OUT"
say() { echo "$@" | tee -a "$OUT"; }

say "== 1. Docker and Kafka"
open -a Docker
until docker info >/dev/null 2>&1; do echo "waiting for Docker Desktop..."; sleep 3; done
docker compose up -d >/dev/null 2>&1
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do echo "waiting for Kafka..."; sleep 3; done
say "Kafka: $(docker inspect -f '{{.State.Health.Status}}' kafka)"
./create_topics.sh >/dev/null 2>&1

say; say "== 2. Start dashboard and live producers"
pkill -f dashboard/viz_web.py; pkill -f producer_all.py; pkill -f producer_bart.py; pkill -f producer_vta.py
CODES=$(python ingestion/producer_all.py --list | grep -iE 'muni|vta|santa clara|ac transit' | awk '{print $1}' | paste -sd, -)
say "Agencies: $CODES"
[ -z "$CODES" ] && { say "No agencies found. Check API_511_KEY in .env"; exit 1; }
nohup python dashboard/viz_web.py > logs/viz.log 2>&1 &
nohup python ingestion/producer_bart.py > logs/bart.log 2>&1 &
nohup python ingestion/producer_all.py --agencies "$CODES" > logs/transit.log 2>&1 &

say; say "== 3. Waiting 90 seconds for live data"
for i in 1 2 3 4 5 6 7 8 9; do sleep 10; printf "."; done; echo

say; say "== 4. Producer output (real feeds)"
tail -n 8 logs/transit.log | tee -a "$OUT"
tail -n 3 logs/bart.log | tee -a "$OUT"

say; say "== 5. Is the data real and does it follow the schema?"
python ingestion/check_real.py 2>&1 | tee -a "$OUT"

say; say "== 6. First 3 Kafka messages (console consumer)"
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic vehicle-positions --from-beginning --max-messages 3 --timeout-ms 10000 \
  --property print.key=true --property key.separator=' | ' 2>/dev/null | tee -a "$OUT"

say; say "== 7. Dashboard"
open http://localhost:8000
sleep 8
screencapture -x ~/Desktop/dashboard.png && say "Screenshot saved to Desktop: dashboard.png"
say "Saved text output: $PWD/$OUT"
say "Dashboard is still running at http://localhost:8000"
