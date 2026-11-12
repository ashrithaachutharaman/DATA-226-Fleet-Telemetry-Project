"""AV / recorded-workload replay producer for the benchmark experiments.

Replays a trajectory file (Waymo/Argoverse export) or a recording of live feed events into
Kafka, at a chosen speed, with a chosen number of virtual vehicles, partition key, delivery
guarantee, and injected faults (duplicate, late, out-of-order events).

Inputs (pick one):
  --csv FILE      columns: vehicle_id,lat,lon,timestamp[,route_id]  (names configurable)
  --parquet FILE  e.g. an Argoverse 2 scenario file (see --id-cols/--x-col/--y-col/--time-col)
  --jsonl FILE    a recording made by ingestion/record_stream.py (live 511/BART events)

Examples:
  python ingestion/av_replay.py --csv trips.csv --source WAYMO --speed 10 --virtual-vehicles 1000
  python ingestion/av_replay.py --jsonl data/recording.jsonl --speed 20 --dup-rate 0.05 --late-rate 0.1
  python ingestion/av_replay.py --parquet scenario.parquet --source ARGO --id-cols scenario_id,track_id \
      --x-col position_x --y-col position_y --time-col timestep --time-scale 0.1 --anchor 37.77,-122.42

Events follow the B4 schema (ingestion/schema.py) exactly. What marks them as a replay is a Kafka
message HEADER, not an extra field: origin=replay, plus virtual_copy=1 for synthetic copies and
fault=late for injected late events. By default they go to the topic "replay-positions", NOT the live
"vehicle-positions" topic, and the dashboard hides them unless INCLUDE_REPLAY=1.
Event times are shifted to "now" (epoch milliseconds) so a replay can be processed like a live stream.
"""
import argparse
import csv
import json
import math
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from confluent_kafka import Producer
from producer_511 import (SCHEMA_VERSION, TRIP_UPDATE_EVENT_FIELDS, VEHICLE_EVENT_FIELDS,
                          make_trip_update_event_id, make_vehicle_event_id, validate)

ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
src = ap.add_mutually_exclusive_group(required=True)
src.add_argument("--csv")
src.add_argument("--parquet")
src.add_argument("--jsonl")
ap.add_argument("--source", default="AV", help="label for the events, e.g. WAYMO, ZOOX, ARGO")
ap.add_argument("--speed", type=float, default=1.0, help="1 = real time, 10 = ten times faster")
ap.add_argument("--virtual-vehicles", type=int, default=1, help="copies of each real vehicle (e.g. 1000 to 10000)")
ap.add_argument("--topic", default=None, help="default: replay-positions (keeps replays out of the live topic)")
ap.add_argument("--key", default="vehicle_id", choices=["vehicle_id", "route_id", "none"],
                help="Kafka partition key")
ap.add_argument("--delivery", default="at-least-once",
                choices=["at-most-once", "at-least-once", "exactly-once"],
                help="producer guarantee (exactly-once = idempotent producer)")
ap.add_argument("--dup-rate", type=float, default=0.0, help="fraction of events sent twice (same event_id)")
ap.add_argument("--late-rate", type=float, default=0.0, help="fraction of events stamped in the past")
ap.add_argument("--late-sec", type=int, default=120, help="how late those events are")
ap.add_argument("--shuffle", type=int, default=0, help="shuffle events inside windows of this many events (out of order)")
ap.add_argument("--seed", type=int, default=1)
ap.add_argument("--max-events", type=int, default=0, help="stop after this many events (0 = all)")
ap.add_argument("--max-gap", type=float, default=10.0,
                help="longest real-time pause between two events; longer gaps in the data are skipped over")
ap.add_argument("--report", default=None, help="write a JSON summary (throughput, counts) to this file")
ap.add_argument("--id-cols", default="vehicle_id", help="comma list of columns joined into the vehicle id")
ap.add_argument("--lat-col", default="lat")
ap.add_argument("--lon-col", default="lon")
ap.add_argument("--x-col", default=None, help="local x in meters (instead of lat/lon)")
ap.add_argument("--y-col", default=None, help="local y in meters")
ap.add_argument("--anchor", default="37.7749,-122.4194", help="lat,lon that local (0,0) is placed at")
ap.add_argument("--time-col", default="timestamp")
ap.add_argument("--time-scale", type=float, default=1.0, help="multiply time column to get seconds")
ap.add_argument("--route-col", default=None)
args = ap.parse_args()

rnd = random.Random(args.seed)
SRC = args.source.upper()


def local_to_latlon(x, y):
    a_lat, a_lon = (float(v) for v in args.anchor.split(","))
    return a_lat + y / 111320.0, a_lon + x / (111320.0 * math.cos(math.radians(a_lat)))


def load_tabular(rows):
    ids = [c.strip() for c in args.id_cols.split(",")]
    out = []
    for r in rows:
        vid = "-".join(str(r[c]) for c in ids)
        if args.x_col:
            lat, lon = local_to_latlon(float(r[args.x_col]), float(r[args.y_col]))
        else:
            lat, lon = float(r[args.lat_col]), float(r[args.lon_col])
        ts = float(r[args.time_col]) * args.time_scale                 # seconds
        route = str(r[args.route_col]) if args.route_col and r.get(args.route_col) not in (None, "") else "AV"
        out.append({"t": ts, "topic": None, "kind": "vehicle", "ev": {
            "source": SRC, "vehicle_id": vid, "route_id": route, "trip_id": None, "direction_id": None,
            "lat": lat, "lon": lon, "bearing": None, "speed_mps": None, "current_status": None, "stop_id": None}})
    return out


def load():
    if args.csv:
        with open(args.csv, newline="") as f:
            return load_tabular(csv.DictReader(f))
    if args.parquet:
        import pandas as pd
        return load_tabular(pd.read_parquet(args.parquet).to_dict("records"))
    out = []
    with open(args.jsonl) as f:
        for line in f:
            rec = json.loads(line)
            ev = rec["event"]
            if args.source != "AV":
                ev["source"] = SRC
            if not ev.get("event_ts"):
                continue          # cannot be placed on the timeline
            kind = "trip" if rec.get("topic") == "trip-updates" or "stop_sequence" in ev else "vehicle"
            out.append({"t": float(ev["event_ts"]) / 1000.0, "topic": rec.get("topic"), "kind": kind, "ev": ev})
    return out


items = load()
if not items:
    sys.exit("The input has no rows.")
items.sort(key=lambda i: i["t"])
t0 = items[0]["t"]

conf = {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"), "linger.ms": 5}
if args.delivery == "at-most-once":
    conf.update({"acks": "0", "retries": 0})
elif args.delivery == "at-least-once":
    conf.update({"acks": "all", "retries": 5})
else:
    conf.update({"acks": "all", "enable.idempotence": True})
producer = Producer(conf)

# one fixed small offset per virtual vehicle so the copies are spread out, not stacked
offsets = [(0.0, 0.0)] + [(rnd.uniform(-0.01, 0.01), rnd.uniform(-0.01, 0.01)) for _ in range(args.virtual_vehicles - 1)]

stats = {"sent": 0, "duplicates": 0, "late": 0, "invalid_skipped": 0, "shuffled_windows": 0}


def send(topic, ev, headers):
    key = None if args.key == "none" else ev.get(args.key)
    while True:
        try:
            producer.produce(topic, key=None if key is None else str(key), value=json.dumps(ev), headers=headers)
            break
        except BufferError:           # local queue full at very high rates: let it drain, then retry
            producer.poll(0.5)
    producer.poll(0)
    stats["sent"] += 1


def make_events(item, now_ms):
    base = item["ev"]
    fields = TRIP_UPDATE_EVENT_FIELDS if item["kind"] == "trip" else VEHICLE_EVENT_FIELDS
    for i in range(args.virtual_vehicles):
        ev = dict(base)
        headers = [("origin", b"replay")]
        if args.virtual_vehicles > 1:
            headers.append(("virtual_copy", b"1" if i > 0 else b"0"))
            if ev.get("vehicle_id") is not None:
                ev["vehicle_id"] = f"{base['vehicle_id']}-v{i}"
            if ev.get("lat") is not None:
                ev["lat"] = base["lat"] + offsets[i][0]
                ev["lon"] = base["lon"] + offsets[i][1]
        ts = now_ms
        if rnd.random() < args.late_rate:
            ts = now_ms - args.late_sec * 1000
            headers.append(("fault", b"late"))
            stats["late"] += 1
        ev["schema_version"], ev["event_ts"], ev["ingest_ts"] = SCHEMA_VERSION, ts, now_ms
        if item["kind"] == "trip":
            ev["event_id"] = make_trip_update_event_id(ev["source"], ev["trip_id"], ev.get("stop_sequence"), ts)
        else:
            ev["event_id"] = make_vehicle_event_id(ev["source"], ev["vehicle_id"], ts)
        if validate(ev, fields):
            stats["invalid_skipped"] += 1        # e.g. a recording made before the B4 schema
            continue
        yield ev, headers


window = []
start = real_start = time.time()


def flush_window():
    if not window:
        return
    if args.shuffle:
        rnd.shuffle(window)
        stats["shuffled_windows"] += 1
    for topic, ev, headers in window:
        send(topic, ev, headers)
        if rnd.random() < args.dup_rate:
            send(topic, ev, headers)    # same event_id: a true duplicate
            stats["duplicates"] += 1
    window.clear()


limit = args.max_events or float("inf")
for item in items:
    wait = (item["t"] - t0) / args.speed - (time.time() - start)
    if wait > args.max_gap:
        start -= wait - args.max_gap       # skip the idle gap instead of sleeping through it
        wait = args.max_gap
    if wait > 0:
        time.sleep(wait)
    topic = args.topic or ("replay-trips" if item["kind"] == "trip" else "replay-positions")
    for ev, headers in make_events(item, int(time.time() * 1000)):
        window.append((topic, ev, headers))
        if len(window) >= max(1, args.shuffle):
            flush_window()
    if stats["sent"] >= limit:
        break
flush_window()
producer.flush()
elapsed = time.time() - real_start
stats.update({"seconds": round(elapsed, 2), "events_per_sec": round(stats["sent"] / elapsed, 1) if elapsed else None,
              "speed": args.speed, "virtual_vehicles": args.virtual_vehicles, "key": args.key,
              "delivery": args.delivery, "dup_rate": args.dup_rate, "late_rate": args.late_rate, "shuffle": args.shuffle})
print(json.dumps(stats))
if args.report:
    with open(args.report, "w") as f:
        json.dump(stats, f, indent=2)
