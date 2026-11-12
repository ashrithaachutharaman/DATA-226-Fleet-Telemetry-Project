#!/bin/bash
cd ~/kafka-spark
MODE=${1:-sim}   # sim = simulator, live = 511 feed

docker compose up -d
rm -f data/spark.log

echo "Starting Spark job in the background..."
docker exec -d spark-master bash -c "/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /opt/streaming/kafka_stream.py > /opt/data/spark.log 2>&1"

echo "Waiting for Spark to be ready (first run can take a few minutes)..."
for i in $(seq 1 60); do
  if grep -q "Batch:" data/spark.log 2>/dev/null; then break; fi
  sleep 5
done
if ! grep -q "Batch:" data/spark.log 2>/dev/null; then
  echo "Spark did not start. Last log lines:"; tail -n 30 data/spark.log; exit 1
fi
echo "Spark is ready. Sending data..."

if [ "$MODE" = "live" ]; then
  python ingestion/producer_511.py --once
else
  python ingestion/simulator_producer.py --seconds 60
fi

sleep 15
echo "---- Spark output (last lines) ----"
tail -n 40 data/spark.log
echo "---- Saved files ----"
find data/parquet -name "*.parquet" | head
