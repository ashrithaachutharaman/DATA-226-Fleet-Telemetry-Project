import argparse
import hashlib
import json
import os
import random
import time

from confluent_kafka import Producer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vehicles", type=int, default=20)
    ap.add_argument("--seconds", type=int, default=120)
    ap.add_argument("--interval", type=float, default=5.0)
    args = ap.parse_args()

    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic = os.getenv("KAFKA_TOPIC_POSITIONS", "vehicle-positions")
    producer = Producer({"bootstrap.servers": servers})

    state = {f"SIM{i:03d}": [37.77 + random.random() * 0.05, -122.45 + random.random() * 0.05]
             for i in range(args.vehicles)}
    end = time.time() + args.seconds
    sent = 0
    while time.time() < end:
        now = int(time.time())
        for vid, p in state.items():
            p[0] += random.uniform(-0.0004, 0.0004)
            p[1] += random.uniform(-0.0004, 0.0004)
            ev = {
                "event_id": hashlib.sha1(f"sim|{vid}|{now}".encode()).hexdigest()[:16],
                "source": "SIM", "vehicle_id": vid,
                "route_id": f"R{sum(map(ord, vid)) % 5}", "trip_id": f"T{vid}",
                "direction_id": sum(map(ord, vid)) % 2,
                "lat": p[0], "lon": p[1], "bearing": None, "speed": None,
                "stop_status": "IN_TRANSIT_TO", "event_ts": now, "ingest_ts": now,
            }
            producer.produce(topic, key=vid, value=json.dumps(ev))
            sent += 1
        producer.flush()
        time.sleep(args.interval)
    print(f"Published {sent} events to {topic}")


if __name__ == "__main__":
    main()
