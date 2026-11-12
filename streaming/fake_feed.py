"""
Fake vehicle feed for testing the Spark job before Kafka is running (B7).

Writes batches of vehicle events, in the common schema from ingestion/schema.py,
as JSON-lines files into a folder. Spark's file source treats every new file
in that folder as new streaming data.

Run from the repo root:
    python streaming/fake_feed.py                      # 20 vehicles, a batch every 5 s, forever
    python streaming/fake_feed.py --batches 12         # stop after 12 batches (1 minute)
    python streaming/fake_feed.py --late 0.1 --dupes 0.05   # add messy data
"""

import argparse
import json
import math
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.schema import (SCHEMA_VERSION, VEHICLE_EVENT_FIELDS,  # noqa: E402
                              make_vehicle_event_id, validate)

# A few made-up routes in San Jose: (route_id, start lat, start lon, heading in degrees)
ROUTES = [("22", 37.3352, -121.8811, 270), ("23", 37.3230, -121.8950, 0),
          ("68", 37.3000, -121.8700, 180), ("72", 37.3500, -121.9000, 90)]
STATUSES = ["IN_TRANSIT_TO", "STOPPED_AT", "INCOMING_AT"]


def make_vehicles(n):
    vehicles = []
    for i in range(n):
        route_id, lat, lon, heading = ROUTES[i % len(ROUTES)]
        vehicles.append({"vehicle_id": str(7000 + i), "route_id": route_id,
                         "trip_id": f"T{route_id}-{i}", "direction_id": i % 2,
                         "lat": lat + random.uniform(-0.01, 0.01),
                         "lon": lon + random.uniform(-0.01, 0.01),
                         "heading": heading, "speed": random.uniform(3, 12)})
    return vehicles


def step(vehicle, seconds):
    """Move a vehicle forward along its heading; sometimes stop it."""
    if random.random() < 0.15:
        vehicle["speed"] = 0.0
    elif vehicle["speed"] == 0.0:
        vehicle["speed"] = random.uniform(3, 12)
    meters = vehicle["speed"] * seconds
    rad = math.radians(vehicle["heading"])
    vehicle["lat"] += (meters * math.cos(rad)) / 111_000
    vehicle["lon"] += (meters * math.sin(rad)) / (111_000 * math.cos(math.radians(vehicle["lat"])))


def to_event(vehicle, event_ts, now_ms):
    return {
        "event_id": make_vehicle_event_id("fake", vehicle["vehicle_id"], event_ts),
        "schema_version": SCHEMA_VERSION,
        "source": "fake",
        "vehicle_id": vehicle["vehicle_id"],
        "route_id": vehicle["route_id"],
        "trip_id": vehicle["trip_id"],
        "direction_id": vehicle["direction_id"],
        "event_ts": event_ts,
        "ingest_ts": now_ms,
        "lat": round(vehicle["lat"], 6),
        "lon": round(vehicle["lon"], 6),
        "bearing": float(vehicle["heading"]),
        "speed_mps": None if random.random() < 0.5 else round(vehicle["speed"], 2),
        "current_status": "STOPPED_AT" if vehicle["speed"] == 0 else random.choice(STATUSES[::2]),
        "stop_id": None,
    }


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default="data/stream_in", help="folder Spark reads from")
    p.add_argument("--vehicles", type=int, default=20)
    p.add_argument("--interval", type=float, default=5.0, help="seconds between batches")
    p.add_argument("--batches", type=int, default=0, help="stop after N batches (0 = run forever)")
    p.add_argument("--late", type=float, default=0.0, help="fraction of events delayed 1-10 minutes")
    p.add_argument("--dupes", type=float, default=0.0, help="fraction of events sent twice")
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)
    vehicles = make_vehicles(args.vehicles)
    held_back = []          # late events waiting to be released
    sent = 0
    batch = 0
    try:
        while args.batches == 0 or batch < args.batches:
            now_ms = int(time.time() * 1000)
            records = []
            for v in vehicles:
                step(v, args.interval)
                event_ts = now_ms - random.randint(2000, 20000)   # normal feed lag: 2-20 s
                event = to_event(v, event_ts, now_ms)
                problems = validate(event, VEHICLE_EVENT_FIELDS)
                if problems:
                    raise ValueError(f"fake event breaks the schema: {problems}")
                if random.random() < args.late:
                    held_back.append((now_ms + random.randint(60_000, 600_000), event))
                    continue
                records.append(event)
                if random.random() < args.dupes:
                    records.append(dict(event))   # same event_id: a true duplicate

            # Release late events whose delay has passed; their event_ts stays old.
            ready = [e for t, e in held_back if t <= now_ms]
            held_back = [(t, e) for t, e in held_back if t > now_ms]
            for e in ready:
                e["ingest_ts"] = now_ms
            records.extend(ready)

            # Write to a temp name, then rename, so Spark never reads a half-written file.
            name = f"batch_{now_ms}_{batch:06d}.json"
            tmp = os.path.join(args.out, "." + name + ".tmp")
            with open(tmp, "w") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")
            os.replace(tmp, os.path.join(args.out, name))

            sent += len(records)
            batch += 1
            print(f"batch {batch}: wrote {len(records)} events "
                  f"({len(ready)} late released) | total sent {sent}", flush=True)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    print(f"Stopped. Total events sent: {sent}")


if __name__ == "__main__":
    main()
