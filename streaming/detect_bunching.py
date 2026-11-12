"""
Bus-bunching detector (B12), Spark Structured Streaming.

Bus bunching: two buses on the same route, going the same direction, end up
right next to each other instead of evenly spaced. Riders then wait a long time
for a bus, and two arrive at once.

How it works:
  1. Read vehicle events (files or Kafka, same input as stream_to_parquet.py).
  2. Group reports into event-time windows per (source, route, direction).
     A watermark says how long to wait for late reports before closing a window.
  3. When a window closes, compare every pair of buses, using reports taken
     close together in time. Closer than max_distance_m -> bunching alert.
  4. Write alerts to Parquet (data/alerts/bunching) and print them.

Run from the repo root (start it before the feed):
    python streaming/detect_bunching.py
    python streaming/detect_bunching.py --source kafka
    python streaming/detect_bunching.py --max-distance 150 --window 120

Check results against planted events from fake_feed.py --bunch-every:
    python streaming/evaluate_bunching.py
"""

import argparse
import math
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.schema import (TOPIC_VEHICLE_POSITIONS,  # noqa: E402
                              VEHICLE_EVENT_FIELDS, spark_schema)
from streaming.config import BUNCHING  # noqa: E402
from streaming.stream_to_parquet import build_spark, read_stream  # noqa: E402

OUTPUT_PATH = "data/alerts/bunching"
CHECKPOINT_PATH = "data/checkpoints/bunching"


def haversine_m(lat1, lon1, lat2, lon2):
    """Distance in meters between two points on Earth (great-circle distance)."""
    r = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def close_pairs(reports, max_distance_m, max_time_gap_s):
    """Find pairs of buses that came closer than max_distance_m during the window.

    reports: every position report for one route and direction in one window.
    Compares each report of bus A with each report of bus B taken within
    max_time_gap_s of it, so bunching that starts and ends inside the window is
    still caught. Returns one row per bunched pair: their closest approach.
    Pure Python, so it can be tested without Spark.
    """
    by_bus = {}
    for r in reports:
        # A dict keyed by timestamp also drops duplicate reports.
        by_bus.setdefault(r["vehicle_id"], {})[r["event_ts"]] = r
    buses = sorted(by_bus)
    pairs = []
    for i in range(len(buses)):
        for j in range(i + 1, len(buses)):
            closest = None
            for a in by_bus[buses[i]].values():
                for b in by_bus[buses[j]].values():
                    gap_s = abs(a["event_ts"] - b["event_ts"]) / 1000
                    if gap_s > max_time_gap_s:
                        continue
                    d = haversine_m(a["lat"], a["lon"], b["lat"], b["lon"])
                    if closest is None or d < closest[0]:
                        closest = (d, gap_s)
            if closest and closest[0] < max_distance_m:     # bunched = too CLOSE
                pairs.append((buses[i], buses[j], round(closest[0], 1), round(closest[1], 1)))
    return pairs


def build_query(spark, args):
    from pyspark.sql.functions import (col, collect_list, count, current_timestamp,
                                       explode, struct, timestamp_millis, udf, window)
    from pyspark.sql.types import (ArrayType, DoubleType, StringType, StructField,
                                   StructType)

    schema = spark_schema(VEHICLE_EVENT_FIELDS)
    events = (read_stream(spark, args, schema)
              .where(col("event_id").isNotNull()
                     & col("route_id").isNotNull()
                     & col("direction_id").isNotNull())
              .withColumn("event_time", timestamp_millis(col("event_ts")))
              .withWatermark("event_time", f"{args.watermark} seconds"))

    windows = (events
               .groupBy(window(col("event_time"), f"{args.window} seconds"),
                        "source", "route_id", "direction_id")
               .agg(collect_list(struct("vehicle_id", "event_ts", "lat", "lon")).alias("reports"),
                    count("*").alias("n_reports")))

    pair_type = ArrayType(StructType([
        StructField("vehicle_a", StringType()), StructField("vehicle_b", StringType()),
        StructField("distance_m", DoubleType()), StructField("time_gap_s", DoubleType())]))
    max_d, max_gap = args.max_distance, args.max_time_gap
    find_pairs = udf(lambda reports: close_pairs(reports, max_d, max_gap), pair_type)

    stats = {"windows": 0, "alerts": 0}

    def handle_batch(batch_df, batch_id):
        """Runs once per micro-batch on the windows that just closed."""
        alerts = (batch_df
                  .withColumn("pair", explode(find_pairs(col("reports"))))
                  .select("source", "route_id", "direction_id",
                          col("window.start").alias("window_start"),
                          col("window.end").alias("window_end"),
                          "pair.*")
                  .withColumn("detected_at", current_timestamp())
                  .persist())
        n_windows = batch_df.count()
        rows = alerts.collect()
        if rows:
            alerts.write.mode("append").parquet(args.output)
        alerts.unpersist()

        stats["windows"] += n_windows
        stats["alerts"] += len(rows)
        if n_windows:
            print(f"micro-batch {batch_id}: {n_windows} route-windows closed, "
                  f"{len(rows)} bunching alert(s)", flush=True)
        for r in rows:
            print(f"  ALERT {r['source']} route {r['route_id']} dir {r['direction_id']} | "
                  f"{r['window_start']:%H:%M:%S}-{r['window_end']:%H:%M:%S} | "
                  f"buses {r['vehicle_a']} & {r['vehicle_b']} {r['distance_m']:.0f} m apart",
                  flush=True)

    query = (windows.writeStream
             .outputMode("append")            # emit each window once, after the watermark passes it
             .foreachBatch(handle_batch)
             .option("checkpointLocation", args.checkpoint)
             .trigger(processingTime=f"{args.trigger} seconds")
             .start())
    return query, stats


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["files", "kafka"], default="files")
    p.add_argument("--input", default="data/stream_in", help="folder to watch (files source)")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_POSITIONS", TOPIC_VEHICLE_POSITIONS))
    p.add_argument("--output", default=OUTPUT_PATH)
    p.add_argument("--checkpoint", default=CHECKPOINT_PATH)
    p.add_argument("--trigger", type=int, default=10, help="seconds between micro-batches")
    p.add_argument("--max-distance", type=float, default=BUNCHING["max_distance_m"])
    p.add_argument("--max-time-gap", type=float, default=BUNCHING["max_time_gap_s"])
    p.add_argument("--window", type=int, default=BUNCHING["window_s"])
    p.add_argument("--watermark", type=int, default=BUNCHING["watermark_s"])
    args = p.parse_args()

    spark = build_spark(use_kafka=(args.source == "kafka"))
    query, stats = build_query(spark, args)
    print(f"Bunching detector: < {args.max_distance:.0f} m, {args.window} s windows, "
          f"{args.watermark} s watermark, reading {args.source} (Ctrl+C to stop)")
    print("Alerts appear after each window closes (window + watermark later).")

    stop = {"requested": False}
    signal.signal(signal.SIGINT, lambda s, f: stop.update(requested=True))
    while query.isActive and not stop["requested"]:
        time.sleep(1)
    print("Stopping...")
    query.stop()
    print(f"Stopped. Route-windows checked: {stats['windows']} | alerts: {stats['alerts']}")


if __name__ == "__main__":
    main()
