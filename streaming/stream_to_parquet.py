"""
Spark Structured Streaming job: read vehicle events and archive them as Parquet (B7).

Input is either a folder of JSON-lines files (for testing with fake_feed.py)
or the Kafka topic (once Kafka is running). Everything after the input step is
the same for both.

Run from the repo root:
    python streaming/stream_to_parquet.py                    # read data/stream_in (fake feed)
    python streaming/stream_to_parquet.py --source kafka     # read the Kafka topic
    python streaming/stream_to_parquet.py --count            # count rows already archived

Stop with Ctrl+C. Restarting continues where it stopped, thanks to the checkpoint.
"""

import argparse
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.schema import (TOPIC_VEHICLE_POSITIONS,  # noqa: E402
                              VEHICLE_EVENT_FIELDS, spark_schema)

OUTPUT_PATH = "data/raw/vehicle_events"
CHECKPOINT_PATH = "data/checkpoints/raw_vehicle_events"


def build_spark(use_kafka):
    import pyspark
    from pyspark.sql import SparkSession

    builder = (SparkSession.builder
               .appName("fleet-telemetry-raw-archive")
               .master("local[*]")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.ui.showConsoleProgress", "false"))
    if use_kafka:
        # The Kafka connector is a separate package that must match our Spark version.
        major = int(pyspark.__version__.split(".")[0])
        scala = "2.13" if major >= 4 else "2.12"
        builder = builder.config(
            "spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{pyspark.__version__}")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_stream(spark, args, schema):
    """Step 1: where the data comes from. The only part that differs between file and Kafka."""
    from pyspark.sql.functions import col, from_json

    if args.source == "files":
        os.makedirs(args.input, exist_ok=True)
        return (spark.readStream
                .schema(schema)                  # streaming file sources require a schema
                .option("maxFilesPerTrigger", 50)
                .json(args.input))

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap)
           .option("subscribe", args.topic)
           .option("startingOffsets", "earliest")
           .load())
    # Kafka gives bytes in a 'value' column; turn them into our columns.
    return (raw.select(from_json(col("value").cast("string"), schema).alias("e"))
               .select("e.*"))


def transform(events):
    """Step 2: add an event-time column, a watermark, and partition columns."""
    from pyspark.sql.functions import col, current_timestamp, timestamp_millis, to_date

    return (events
            .where(col("event_id").isNotNull())                 # drop records that failed to parse
            .withColumn("event_time", timestamp_millis(col("event_ts")))
            .withColumn("spark_ts", current_timestamp())        # when Spark processed it
            .withWatermark("event_time", "10 minutes")          # how long to wait for late data
            .withColumn("event_date", to_date(col("event_time"))))


def write_stream(df, args):
    """Step 3: append each micro-batch to Parquet, with a checkpoint for recovery."""
    return (df.writeStream
            .format("parquet")
            .option("path", args.output)
            .option("checkpointLocation", args.checkpoint)
            .partitionBy("event_date", "source")
            .outputMode("append")
            .trigger(processingTime=f"{args.trigger} seconds")
            .start())


def report_progress(query):
    """Print throughput for every micro-batch until Ctrl+C, then stop cleanly."""
    stop = {"requested": False}

    def on_ctrl_c(signum, frame):
        stop["requested"] = True

    # Replace Spark's own Ctrl+C handler so stopping doesn't print a traceback.
    signal.signal(signal.SIGINT, on_ctrl_c)

    last_batch = -1
    total_rows = 0
    while query.isActive and not stop["requested"]:
        for p in query.recentProgress:
            if p["batchId"] <= last_batch:
                continue
            last_batch = p["batchId"]
            total_rows += p["numInputRows"]
            if p["numInputRows"] == 0:
                continue                      # skip empty batches to keep the output readable
            print(f"batch {p['batchId']:>4}: {p['numInputRows']:>5} rows | "
                  f"in {p.get('inputRowsPerSecond', 0):8.1f} rows/s | "
                  f"processed {p.get('processedRowsPerSecond', 0):8.1f} rows/s | "
                  f"took {p['durationMs'].get('triggerExecution', 0)} ms | "
                  f"total {total_rows}", flush=True)
        time.sleep(1)

    print("Stopping...")
    query.stop()
    # Count every batch, including one that finished while stopping.
    total_rows = sum(p["numInputRows"] for p in query.recentProgress)
    print(f"Stopped. Rows processed this run: {total_rows}")


def count_archive(spark, path):
    df = spark.read.parquet(path)
    print(f"Rows archived: {df.count()}  |  distinct event_id: {df.select('event_id').distinct().count()}")
    df.groupBy("event_date", "source").count().orderBy("event_date", "source").show()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["files", "kafka"], default="files")
    p.add_argument("--input", default="data/stream_in", help="folder to watch (files source)")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_POSITIONS", TOPIC_VEHICLE_POSITIONS))
    p.add_argument("--output", default=OUTPUT_PATH)
    p.add_argument("--checkpoint", default=CHECKPOINT_PATH)
    p.add_argument("--trigger", type=int, default=10, help="seconds between micro-batches")
    p.add_argument("--count", action="store_true", help="count archived rows and exit")
    args = p.parse_args()

    spark = build_spark(use_kafka=(args.source == "kafka" and not args.count))
    if args.count:
        count_archive(spark, args.output)
        return

    schema = spark_schema(VEHICLE_EVENT_FIELDS)
    query = write_stream(transform(read_stream(spark, args, schema)), args)
    print(f"Streaming from {args.source} -> {args.output} (Ctrl+C to stop)")
    report_progress(query)


if __name__ == "__main__":
    main()
