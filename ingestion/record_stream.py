"""Record live Kafka events to a file so they can be replayed later at higher speed.

    python ingestion/record_stream.py --minutes 120 --out data/recording_2h.jsonl
    python ingestion/av_replay.py --jsonl data/recording_2h.jsonl --speed 20

Run it while producer_all.py / producer_bart.py are running. Each line is one real event.
"""
import argparse
import json
import os
import time
import uuid

from confluent_kafka import Consumer

ap = argparse.ArgumentParser()
ap.add_argument("--minutes", type=float, default=120)
ap.add_argument("--out", default="data/recording.jsonl")
ap.add_argument("--topics", default="vehicle-positions,trip-updates")
ap.add_argument("--from-start", action="store_true", help="also include events already in Kafka")
args = ap.parse_args()

os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
c = Consumer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
              "group.id": f"rec-{uuid.uuid4()}",
              "auto.offset.reset": "earliest" if args.from_start else "latest"})
c.subscribe(args.topics.split(","))
end = time.time() + args.minutes * 60
n = 0
with open(args.out, "w") as f:
    while time.time() < end:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        try:
            ev = json.loads(m.value())
        except Exception:
            continue
        f.write(json.dumps({"topic": m.topic(), "event": ev}) + "\n")
        n += 1
        if n % 500 == 0:
            f.flush()
            print(f"[{time.strftime('%H:%M:%S')}] recorded {n} events", flush=True)
print(f"Recorded {n} events to {args.out}")
