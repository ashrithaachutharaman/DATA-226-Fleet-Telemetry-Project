"""Check that the LIVE Kafka topics hold only real data that follows the B4 schema.

    python ingestion/check_real.py            # reads what is already in Kafka (about 10 seconds)

It flags: simulated or replayed events in the live topics, source labels that have no live feed
(Waymo, Zoox, Argoverse), vehicle ids that are not hashed (B13), records that break the schema (wrong or missing fields, timestamps that
are not milliseconds), positions outside the Bay Area, and stale data.
"""
import argparse
import json
import os
import sys
import time
import re
import uuid
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from confluent_kafka import Consumer
from dotenv import load_dotenv
from ingestion.schema import TRIP_UPDATE_EVENT_FIELDS, VEHICLE_EVENT_FIELDS, validate

load_dotenv()
ap = argparse.ArgumentParser()
ap.add_argument("--seconds", type=float, default=10, help="stop after this long (or when Kafka runs dry)")
ap.add_argument("--stale-min", type=float, default=30, help="newest report older than this is called stale")
ap.add_argument("--topics", default="vehicle-positions,trip-updates")
args = ap.parse_args()

NOT_LIVE_SOURCES = {"SIM", "WAYMO", "ZOOX", "ARGO", "AV"}   # no public live feed exists for these
BBOX = (36.8, 38.6, -123.3, -121.0)                          # lat_min, lat_max, lon_min, lon_max
HASHED = re.compile(r"^[0-9a-f]{32}$")                         # a B13 token: 32 hex characters
MIN_MS = 1_577_836_800_000                                   # 2020-01-01 in ms: anything smaller is not milliseconds

c = Consumer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
              "group.id": f"check-{uuid.uuid4()}", "auto.offset.reset": "earliest"})
c.subscribe(args.topics.split(","))

stat = defaultdict(lambda: {"events": 0, "vehicles": set(), "newest": 0, "outside": 0, "not_live": 0,
                            "bad_schema": 0, "bad_time": 0, "raw_ids": 0})
first_problem = {}
total = 0
end = time.time() + args.seconds
empty = 0
while time.time() < end and empty < 3:
    m = c.poll(1.0)
    if m is None or m.error():
        empty += 1
        continue
    empty = 0
    try:
        e = json.loads(m.value())
    except Exception:
        continue
    total += 1
    src = e.get("source") or "unknown"
    s = stat[src]
    s["events"] += 1
    if e.get("vehicle_id"):
        s["vehicles"].add(e["vehicle_id"])
    headers = {k: v for k, v in (m.headers() or [])}
    if src in NOT_LIVE_SOURCES or headers.get("origin") in (b"replay", b"synthetic"):
        s["not_live"] += 1
    if e.get("vehicle_id") and src not in NOT_LIVE_SOURCES and not HASHED.match(str(e["vehicle_id"])):
        s["raw_ids"] += 1
    fields = TRIP_UPDATE_EVENT_FIELDS if "delay_sec" in e or "stop_sequence" in e else VEHICLE_EVENT_FIELDS
    problems = validate(e, fields)
    if problems:
        s["bad_schema"] += 1
        first_problem.setdefault(src, "; ".join(problems[:3]))
    ts = e.get("event_ts")
    if isinstance(ts, int):
        if ts < MIN_MS:
            s["bad_time"] += 1
        else:
            s["newest"] = max(s["newest"], ts)
    lat, lon = e.get("lat"), e.get("lon")
    if lat is not None and lon is not None and not (BBOX[0] <= lat <= BBOX[1] and BBOX[2] <= lon <= BBOX[3]):
        s["outside"] += 1

if not total:
    print("No events found in", args.topics, "- start ingestion/producer_all.py or producer_bart.py first.")
    raise SystemExit(2)

now = time.time()
problems_found = []
print(f"{'source':10s} {'events':>7s} {'vehicles':>9s} {'newest report':>14s}  verdict")
for src, s in sorted(stat.items(), key=lambda kv: -kv[1]["events"]):
    age = (now - s["newest"] / 1000) / 60 if s["newest"] else None
    notes = []
    if s["not_live"]:
        notes.append(f"NOT REAL: {s['not_live']} simulated/replayed/no-live-feed events")
        problems_found.append(src)
    if s["raw_ids"]:
        notes.append(f"PRIVACY: {s['raw_ids']} vehicle ids are not hashed (B13)")
        problems_found.append(src)
    if s["bad_schema"]:
        notes.append(f"SCHEMA: {s['bad_schema']} records break the B4 schema (e.g. {first_problem[src]})")
        problems_found.append(src)
    if s["bad_time"]:
        notes.append(f"TIME: {s['bad_time']} timestamps are not epoch milliseconds")
        problems_found.append(src)
    if s["outside"]:
        notes.append(f"{s['outside']} positions outside the Bay Area")
        problems_found.append(src)
    if age is not None and age > args.stale_min:
        notes.append(f"stale ({age:.0f} min old)")
    print(f"{src:10s} {s['events']:7d} {len(s['vehicles']):9d} "
          f"{('%.1f min ago' % age) if age is not None else 'n/a':>14s}  {'; '.join(notes) or 'real, live, schema OK'}")
print()
if problems_found:
    print("RESULT: NOT CLEAN. See the notes above.")
    print("Messages written before the schema change stay in Kafka. To wipe them: docker compose down -v && docker compose up -d")
    raise SystemExit(1)
print(f"RESULT: CLEAN. {total} events: all real, none simulated or replayed, all follow the B4 schema, vehicle ids hashed.")
