#!/bin/bash
# Card check: run the VTA producer once, and confirm the NEW messages arrive in Kafka
# (read with the Kafka console consumer). Usage: ./producer_check.sh [511-agency-code, default SC]
# Saves proof to producer_evidence.txt (paste or screenshot it on the Trello card).
cd "$(dirname "$0")"
AGENCY=${1:-SC}
EVID=producer_evidence.txt
START=$(date +%s)
{ echo "Producer check for agency $AGENCY, $(date)"; echo; } > "$EVID"

echo "== 1. Start a console consumer that listens for new messages on vehicle-positions"
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic vehicle-positions --timeout-ms 20000 \
  --property print.key=true --property key.separator='|' 2>/dev/null > .consumed.tmp &
CONS=$!
sleep "${CHECK_WAIT:-8}"      # give the consumer time to join the topic

echo "== 2. Fetch $AGENCY, decode, map to the common schema, publish (one poll)"
${PRODUCER_CMD:-python ingestion/producer_vta.py --once --agency $AGENCY} 2>&1 | tee -a "$EVID"
if [ ${PIPESTATUS[0]} -ne 0 ]; then
  kill $CONS 2>/dev/null
  echo "FAIL: the producer did not publish (see above)"; echo "FAIL: producer" >> "$EVID"; rm -f .consumed.tmp; exit 1
fi

echo
echo "== 3. Wait for the console consumer to finish reading (up to ~20 seconds)"
wait $CONS
{ echo; echo "--- first 3 new messages the console consumer read (key|value)"; grep "\"source\": \"$AGENCY\"" .consumed.tmp | head -n 3; echo; } | tee -a "$EVID"

python - "$START" "$AGENCY" <<'PY' | tee -a "$EVID"
import json, sys
start, agency = int(sys.argv[1]), sys.argv[2]
ok = bad = fresh = 0
for line in open(".consumed.tmp"):
    key, _, val = line.rstrip("\n").partition("|")
    try:
        e = json.loads(val)
    except Exception:
        continue
    if e.get("source") != agency or e.get("ingest_ts", 0) < start - 5:
        continue
    fresh += 1
    if key == e.get("vehicle_id"):
        ok += 1
    else:
        bad += 1
print(f"{agency} messages from this run: {fresh}; keyed by vehicle_id: {ok}; wrong key: {bad}")
if fresh and not bad:
    print("PASS: the messages arrive in vehicle-positions, keyed by vehicle_id")
    sys.exit(0)
print("FAIL: no new messages from this run were read, or a message has the wrong key")
sys.exit(1)
PY
RC=${PIPESTATUS[0]}
rm -f .consumed.tmp
exit $RC
