"""Publish SIMULATED vehicle events to Kafka (no 511 key needed). Source is "SIM".

For testing the Spark job before live data is flowing. Simulated data is not real:
the dashboard hides source SIM unless INCLUDE_SIM=1.
    python ingestion/simulator_producer.py --vehicles 20 --seconds 120
"""
import argparse
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from confluent_kafka import Producer
from dotenv import load_dotenv
from producer_511 import SCHEMA_VERSION, VEHICLE_EVENT_FIELDS, make_vehicle_event_id, now_ms, publish


def main():
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--vehicles", type=int, default=20)
    ap.add_argument("--seconds", type=int, default=120)
    ap.add_argument("--interval", type=float, default=5.0)
    args = ap.parse_args()

    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")})
    topic = os.getenv("KAFKA_TOPIC_POSITIONS", "vehicle-positions")
    state = {f"SIM{i:03d}": [37.77 + random.random() * 0.05, -122.45 + random.random() * 0.05]
             for i in range(args.vehicles)}
    end = time.time() + args.seconds
    sent = 0
    while time.time() < end:
        now = now_ms()
        events = []
        for i, (vid, p) in enumerate(state.items()):
            p[0] += random.uniform(-0.0004, 0.0004)
            p[1] += random.uniform(-0.0004, 0.0004)
            events.append({
                "event_id": make_vehicle_event_id("SIM", vid, now),
                "schema_version": SCHEMA_VERSION, "source": "SIM", "vehicle_id": vid,
                "route_id": f"R{i % 5}", "trip_id": f"T{vid}", "direction_id": i % 2,
                "event_ts": now, "ingest_ts": now, "lat": p[0], "lon": p[1],
                "bearing": None, "speed_mps": None, "current_status": "IN_TRANSIT_TO", "stop_id": None})
        sent += publish(producer, topic, events, "vehicle_id", VEHICLE_EVENT_FIELDS)
        time.sleep(args.interval)
    print(f"Published {sent} simulated events to {topic}")


if __name__ == "__main__":
    main()
