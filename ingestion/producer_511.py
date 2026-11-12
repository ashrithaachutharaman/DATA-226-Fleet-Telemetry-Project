import argparse
import hashlib
import json
import os
import sys
import time

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

BASE = "https://api.511.org/transit"
STATUS = {0: "INCOMING_AT", 1: "STOPPED_AT", 2: "IN_TRANSIT_TO"}


def event_id(*parts) -> str:
    return hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()[:16]


def fetch_feed(endpoint, agency, key):
    r = requests.get(f"{BASE}/{endpoint}", params={"api_key": key, "agency": agency}, timeout=30)
    r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)
    return feed


def vehicle_events(feed, agency, ingest_ts):
    for ent in feed.entity:
        if not ent.HasField("vehicle"):
            continue
        v = ent.vehicle
        if not v.HasField("position"):
            continue
        vehicle_id = v.vehicle.id or ent.id
        event_ts = int(v.timestamp) or int(feed.header.timestamp) or ingest_ts
        pos = v.position
        yield {
            "event_id": event_id("pos", agency, vehicle_id, event_ts),
            "source": agency,
            "vehicle_id": vehicle_id,
            "route_id": v.trip.route_id or None,
            "trip_id": v.trip.trip_id or None,
            "direction_id": v.trip.direction_id if v.trip.HasField("direction_id") else None,
            "lat": pos.latitude,
            "lon": pos.longitude,
            "bearing": pos.bearing if pos.HasField("bearing") else None,
            "speed": pos.speed if pos.HasField("speed") else None,
            "stop_status": STATUS.get(v.current_status) if v.HasField("current_status") else None,
            "event_ts": event_ts,
            "ingest_ts": ingest_ts,
        }


def trip_delay_events(feed, agency, ingest_ts):
    for ent in feed.entity:
        if not ent.HasField("trip_update"):
            continue
        tu = ent.trip_update
        event_ts = int(tu.timestamp) or int(feed.header.timestamp) or ingest_ts
        for stu in tu.stop_time_update:
            ev = stu.arrival if stu.HasField("arrival") else stu.departure
            if not ev.HasField("delay"):
                continue
            yield {
                "event_id": event_id("delay", agency, tu.trip.trip_id, stu.stop_id, event_ts),
                "source": agency,
                "trip_id": tu.trip.trip_id or None,
                "route_id": tu.trip.route_id or None,
                "direction_id": tu.trip.direction_id if tu.trip.HasField("direction_id") else None,
                "stop_id": stu.stop_id or None,
                "stop_sequence": stu.stop_sequence if stu.HasField("stop_sequence") else None,
                "delay_sec": ev.delay,
                "event_ts": event_ts,
                "ingest_ts": ingest_ts,
            }


def publish(producer, topic, events, key_field):
    n = 0
    for e in events:
        producer.produce(topic, key=str(e.get(key_field) or ""), value=json.dumps(e))
        n += 1
        producer.poll(0)
    producer.flush()
    return n


def main():
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--agency", default="RG")
    ap.add_argument("--interval", type=int, default=75)
    ap.add_argument("--feeds", choices=["vehicles", "trips", "both"], default="vehicles")
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    key = os.getenv("API_511_KEY")
    if not key or key.startswith("PASTE") or key.startswith("your-"):
        sys.exit("Set API_511_KEY in .env")
    trips_key = os.getenv("API_511_KEY_TRIPS") or key
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    producer = Producer({"bootstrap.servers": servers, "linger.ms": 50})
    pos_topic = os.getenv("KAFKA_TOPIC_POSITIONS", "vehicle-positions")
    trip_topic = os.getenv("KAFKA_TOPIC_TRIPS", "trip-updates")

    while True:
        started = time.time()
        ingest_ts = int(started)
        try:
            if args.feeds in ("vehicles", "both"):
                feed = fetch_feed("vehiclepositions", args.agency, key)
                n = publish(producer, pos_topic, vehicle_events(feed, args.agency, ingest_ts), "vehicle_id")
                print(f"[{time.strftime('%H:%M:%S')}] vehicle-positions: published {n}", flush=True)
            if args.feeds in ("trips", "both"):
                feed = fetch_feed("tripupdates", args.agency, trips_key)
                n = publish(producer, trip_topic, trip_delay_events(feed, args.agency, ingest_ts), "trip_id")
                print(f"[{time.strftime('%H:%M:%S')}] trip-updates: published {n}", flush=True)
        except requests.HTTPError as e:
            print(f"HTTP error: {e} (429 means rate limit: raise --interval)", flush=True)
        except Exception as e:
            print(f"Poll failed: {e!r}", flush=True)
        if args.once:
            break
        time.sleep(max(0, args.interval - (time.time() - started)))


if __name__ == "__main__":
    main()
