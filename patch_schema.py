"""Moves the producers onto the B4 common schema (millisecond timestamps, validate before publish)."""
import os
import sys

def patch(path, pairs, done):
    if not os.path.exists(path):
        print(path, "not found, skipped"); return
    s = open(path).read()
    if done in s:
        print(path, "already updated"); return
    for a, b in pairs:
        if s.count(a) != 1:
            sys.exit(f"{path}: could not find the expected text, so nothing was changed in this file. Tell Claude.")
        s = s.replace(a, b)
    open(path, "w").write(s); print(path, "updated")

patch("ingestion/producer_all.py", [
("from producer_511 import BASE, fetch_feed, publish, vehicle_events",
 "from producer_511 import BASE, VEHICLE_EVENT_FIELDS, fetch_feed, now_ms, publish, vehicle_events"),
('n = publish(producer, topic, vehicle_events(feed, agency, int(time.time())), "vehicle_id")',
 'n = publish(producer, topic, vehicle_events(feed, agency, now_ms()), "vehicle_id", VEHICLE_EVENT_FIELDS)'),
], "now_ms()")

patch("ingestion/producer_bart.py", [
("from producer_511 import publish, trip_delay_events",
 "from producer_511 import TRIP_UPDATE_EVENT_FIELDS, now_ms, publish, trip_delay_events"),
('n = publish(producer, topic, trip_delay_events(feed, "BA", int(time.time())), "trip_id")',
 'n = publish(producer, topic, trip_delay_events(feed, "BA", now_ms()), "trip_id", TRIP_UPDATE_EVENT_FIELDS)'),
], "now_ms()")

patch("ingestion/producer_vta.py", [
("from producer_511 import fetch_feed, vehicle_events",
 "from producer_511 import VEHICLE_EVENT_FIELDS, fetch_feed, now_ms, valid_events, vehicle_events"),
("    now = int(time.time())\n", "    now = now_ms()                                                   # milliseconds (B4)\n"),
("    for ev in vehicle_events(feed, args.agency, now):                # common schema",
 "    for ev in valid_events(vehicle_events(feed, args.agency, now), VEHICLE_EVENT_FIELDS):   # common schema, validated"),
], "valid_events")

patch("producer_check.sh", [
("START=$(date +%s)\n", "START=$(( $(date +%s) * 1000 ))   # milliseconds, like ingest_ts\n"),
('e.get("ingest_ts", 0) < start - 5:', 'e.get("ingest_ts", 0) < start - 5000:'),
], "* 1000 ))")

VIZ = "dashboard/viz_web.py" if os.path.exists("dashboard/viz_web.py") else "viz_web.py"
patch(VIZ, [
("from confluent_kafka import Consumer\n",
 "from confluent_kafka import Consumer\nfrom dotenv import load_dotenv\n\nload_dotenv()\n"),
('Consumer({"bootstrap.servers": "localhost:29092",',
 'Consumer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),'),
('if e.get("origin") == "replay" or msg.topic()',
 'if dict(msg.headers() or []).get("origin") == b"replay" or msg.topic()'),
('max(last_seen.get((src, vid), 0), e.get("event_ts") or int(time.time()))',
 'max(last_seen.get((src, vid), 0), (e.get("event_ts") or int(time.time() * 1000)) / 1000)'),
('s["last"] = max(s["last"], e.get("event_ts") or 0)',
 's["last"] = max(s["last"], (e.get("event_ts") or 0) / 1000)       # event_ts is milliseconds (B4)'),
('s["ages"].append(e["ingest_ts"] - e["event_ts"])',
 's["ages"].append((e["ingest_ts"] - e["event_ts"]) / 1000)'),
], "KAFKA_BOOTSTRAP_SERVERS")
