#!/bin/bash
# Checks Kafka -> Spark Structured Streaming -> Parquet using the team's ONE archive job:
#   streaming/stream_to_parquet.py --source kafka
# Needs Java and pyspark on your computer. Producers must already be publishing (./demo.sh).
cd "$(dirname "$0")"
mkdir -p logs
EVID=spark_evidence.txt
{ echo "Spark archive check, $(date)"; echo; } > "$EVID"
say() { echo "$@" | tee -a "$EVID"; }

command -v java >/dev/null || { say "FAIL: Java is not installed. Run: brew install openjdk@17"; exit 1; }
python -c "import pyspark" 2>/dev/null || { say "FAIL: pyspark is missing. Run: python -m pip install pyspark"; exit 1; }

say "== 1. Run the archive job on the Kafka topic for ${SPARK_WAIT:-120} seconds"
rm -rf data/raw/vehicle_events data/checkpoints/raw_vehicle_events
KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092} \
  python streaming/stream_to_parquet.py --source kafka > logs/archive.log 2>&1 &
PID=$!
sleep "${SPARK_WAIT:-120}"
kill -INT $PID 2>/dev/null; wait $PID 2>/dev/null
grep -E "^batch|Stopped" logs/archive.log | tail -n 8 | tee -a "$EVID"

say; say "== 2. What was archived"
python streaming/stream_to_parquet.py --count 2>&1 | grep -vE "WARN|INFO|JAVA_TOOL|setLogLevel|^$" | tee -a "$EVID"

say; say "== 3. Does the archive follow the B4 schema?"
python - <<'PY' | tee -a "$EVID"
import sys
import pandas as pd
try:
    df = pd.read_parquet("data/raw/vehicle_events")
except Exception as e:
    print("FAIL: no Parquet data was written:", e); sys.exit(1)
rows = len(df)
ms_ok = int((df["event_ts"] > 1_577_836_800_000).sum())
ver_ok = int((df["schema_version"] == 1).sum())
print(f"rows: {rows}; event_ts in epoch milliseconds: {ms_ok}; schema_version 1: {ver_ok}")
print(f"event dates: {sorted(df['event_date'].astype(str).unique())}")
print(f"sources: {sorted(df['source'].astype(str).unique())}")
ok = rows > 0 and ms_ok == rows and ver_ok == rows
print("PASS: Kafka -> Spark -> Parquet works and every row follows the B4 schema" if ok
      else "FAIL: no rows, or rows with old-format timestamps/schema. Wipe old messages: docker compose down && docker compose up -d, then restart the producers")
sys.exit(0 if ok else 1)
PY
exit ${PIPESTATUS[0]}
