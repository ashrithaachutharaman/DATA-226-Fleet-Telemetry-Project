"""
Fake vehicle feed for testing the Spark job before Kafka is running (B7).

Writes batches of vehicle events, in the common schema from ingestion/schema.py,
as JSON-lines files into a folder. Spark's file source treats every new file
in that folder as new streaming data.

Run from the repo root:
    python streaming/fake_feed.py                      # 20 vehicles, a batch every 5 s, forever
    python streaming/fake_feed.py --batches 12         # stop after 12 batches (1 minute)
    python streaming/fake_feed.py --late 0.1 --dupes 0.05   # add messy data
    python streaming/fake_feed.py --batches 60 --bunch-every 8   # plant bus bunching (B12 testing)

Planted bunching: every N batches, two buses on the same route and direction are
pushed together (60 m apart) for a few batches, then separated again. Each planted
event is logged to data/planted_bunching.jsonl, so the detector can be checked
against a known answer with streaming/evaluate_bunching.py.
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


def offset(lat, lon, heading, meters):
    """Return the point `meters` ahead of (lat, lon) along `heading` (negative = behind)."""
    rad = math.radians(heading)
    new_lat = lat + (meters * math.cos(rad)) / 111_000
    new_lon = lon + (meters * math.sin(rad)) / (111_000 * math.cos(math.radians(lat)))
    return new_lat, new_lon


def make_vehicles(n, spacing_m=2000):
    """Place buses along their route, spacing_m apart, so they don't start out bunched."""
    vehicles = []
    for i in range(n):
        route_id, lat, lon, heading = ROUTES[i % len(ROUTES)]
        place_on_route = i // len(ROUTES)              # 0, 1, 2, ... for buses on the same route
        lat, lon = offset(lat, lon, heading, -place_on_route * spacing_m)
        vehicles.append({"vehicle_id": str(7000 + i), "route_id": route_id,
                         "trip_id": f"T{route_id}-{i}", "direction_id": i % 2,
                         "lat": lat, "lon": lon,
                         "heading": heading, "speed": random.uniform(3, 12),
                         # Each bus's tracker has its own steady delay, as in real feeds,
                         # so one bus's reports stay in time order.
                         "lag_ms": random.randint(2000, 18000)})
    return vehicles


def step(vehicle, seconds):
    """Move a vehicle forward along its heading; sometimes stop it."""
    if random.random() < 0.15:
        vehicle["speed"] = 0.0
    elif vehicle["speed"] == 0.0:
        vehicle["speed"] = random.uniform(3, 12)
    vehicle["lat"], vehicle["lon"] = offset(vehicle["lat"], vehicle["lon"],
                                            vehicle["heading"], vehicle["speed"] * seconds)


class BunchingPlanter:
    """Pushes two buses on the same route and direction together, and logs each event."""

    def __init__(self, vehicles, every, length, gap_m, log_path):
        self.vehicles, self.every, self.length, self.gap_m = vehicles, every, length, gap_m
        self.log_path = log_path
        self.active = None
        self.planted = 0

    def before_batch(self, batch, now_ms):
        if self.every <= 0:
            return
        if self.active is None and batch > 0 and batch % self.every == 0:
            self._start(now_ms)
        if self.active:
            a, b = self.active["a"], self.active["b"]
            # Keep bus B right behind bus A, moving at A's speed.
            b["lat"], b["lon"] = offset(a["lat"], a["lon"], a["heading"], -self.gap_m)
            b["speed"] = a["speed"]

    def after_batch(self, now_ms):
        if not self.active:
            return
        self.active["end_ts"] = now_ms
        self.active["batches_left"] -= 1
        if self.active["batches_left"] == 0:
            b = self.active["b"]
            # Separate them again: put B back where it was before the event.
            b["lat"], b["lon"] = self.active["b_home"]
            self._log(self.active)
            self.active = None

    def _start(self, now_ms):
        by_route = {}
        for v in self.vehicles:
            by_route.setdefault((v["route_id"], v["direction_id"]), []).append(v)
        options = [vs for vs in by_route.values() if len(vs) >= 2]
        if not options:
            return
        a, b = random.sample(random.choice(options), 2)
        self.active = {"a": a, "b": b, "b_home": (b["lat"], b["lon"]),
                       "batches_left": self.length,
                       "start_ts": now_ms - 20_000,      # covers the 2-20 s feed lag on event_ts
                       "end_ts": now_ms}

    def _log(self, event):
        self.planted += 1
        record = {"type": "planted_bunching",
                  "route_id": event["a"]["route_id"], "direction_id": event["a"]["direction_id"],
                  "vehicle_a": event["a"]["vehicle_id"], "vehicle_b": event["b"]["vehicle_id"],
                  "start_ts": event["start_ts"], "end_ts": event["end_ts"], "gap_m": self.gap_m}
        self.write(record)

    def write(self, record):
        os.makedirs(os.path.dirname(self.log_path) or ".", exist_ok=True)
        with open(self.log_path, "a") as f:
            f.write(json.dumps(record) + "\n")


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
    p.add_argument("--bunch-every", type=int, default=0,
                   help="plant a bunching event every N batches (0 = off)")
    p.add_argument("--bunch-length", type=int, default=4, help="batches each planted event lasts")
    p.add_argument("--bunch-gap", type=float, default=60.0, help="meters between the planted buses")
    p.add_argument("--planted-log", default="data/planted_bunching.jsonl")
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)
    vehicles = make_vehicles(args.vehicles)
    planter = BunchingPlanter(vehicles, args.bunch_every, args.bunch_length,
                              args.bunch_gap, args.planted_log)
    held_back = []          # late events waiting to be released
    sent = 0
    batch = 0
    try:
        while args.batches == 0 or batch < args.batches:
            now_ms = int(time.time() * 1000)
            records = []
            for v in vehicles:
                step(v, args.interval)
            planter.before_batch(batch, now_ms)
            for v in vehicles:
                event_ts = now_ms - v["lag_ms"] - random.randint(0, 1000)  # feed lag: 2-19 s
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
            planting = " | planting bunching" if planter.active else ""
            planter.after_batch(now_ms)
            print(f"batch {batch}: wrote {len(records)} events "
                  f"({len(ready)} late released) | total sent {sent}{planting}", flush=True)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    if args.bunch_every > 0:
        # Record when the feed stopped: windows near the end can't close without newer data.
        planter.write({"type": "feed_stopped", "ts": int(time.time() * 1000)})
        print(f"Planted bunching events: {planter.planted} (logged to {args.planted_log})")
    print(f"Stopped. Total events sent: {sent}")


if __name__ == "__main__":
    main()
