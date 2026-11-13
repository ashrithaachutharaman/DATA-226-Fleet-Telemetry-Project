"""Card: fetch ONE live feed (VTA), decode it, map to the common schema, publish to Kafka.

    python ingestion/producer_vta.py                 # poll VTA (511 agency SC) in a loop
    python ingestion/producer_vta.py --once          # one poll, then exit
    python ingestion/producer_vta.py --agency SC --interval 30

1. Fetch the GTFS-realtime vehicle-positions feed from 511 (protobuf)
2. Decode it with gtfs-realtime-bindings
3. Map each vehicle position to the common event schema (producer_511.vehicle_events)
4. Publish to topic vehicle-positions, keyed by vehicle_id
5. Poll on a loop with error handling and retries

511 allows 60 requests per hour per key, so 30 s polling needs two keys
(API_511_KEYS=key1,key2 in .env). With one key the safe interval is about 66 s; the script
raises a too-short interval to the safe value and says so.
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
from producer_511 import VEHICLE_EVENT_FIELDS, fetch_feed, get_salt, now_ms, valid_events, vehicle_events

load_dotenv()
ap = argparse.ArgumentParser()
ap.add_argument("--agency", default="SC", help="511 operator code for VTA (check: producer_all.py --list)")
ap.add_argument("--interval", type=float, default=30, help="seconds between polls")
ap.add_argument("--retries", type=int, default=3, help="attempts per poll before giving up until the next poll")
ap.add_argument("--once", action="store_true")
args = ap.parse_args()

raw = os.getenv("API_511_KEYS") or os.getenv("API_511_KEY") or ""
keys = [k.strip() for k in raw.split(",") if k.strip() and not k.strip().startswith(("PASTE", "your-"))]
if not keys:
    sys.exit("Set API_511_KEY (or API_511_KEYS=key1,key2) in .env")
try:
    get_salt()                      # B13: stop now if VEHICLE_ID_SALT is not set, instead of failing on every poll
except ValueError as e:
    sys.exit(str(e))
safe = 3600 / (60 * len(keys)) * 1.1
interval = max(args.interval, safe)
if interval > args.interval:
    print(f"{len(keys)} key(s): 511 allows 60 requests/hour per key, so polling every {interval:.0f}s "
          f"instead of {args.interval:.0f}s. Add a second key in API_511_KEYS to poll every ~33s.", flush=True)

topic = os.getenv("KAFKA_TOPIC_POSITIONS", "vehicle-positions")
producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
                     "acks": "all", "retries": 5, "linger.ms": 50})
failed_deliveries = []


def on_delivery(err, msg):
    if err is not None:
        failed_deliveries.append(str(err))


def poll_once(key):
    """Fetch, decode, map, publish. Returns the number of vehicles published."""
    now = now_ms()                                                   # milliseconds (B4)
    feed = fetch_feed("vehiclepositions", args.agency, key)          # fetch + protobuf decode
    n = 0
    for ev in valid_events(vehicle_events(feed, args.agency, now), VEHICLE_EVENT_FIELDS):   # common schema, validated
        producer.produce(topic, key=str(ev["vehicle_id"]), value=json.dumps(ev),
                         on_delivery=on_delivery)                    # keyed by vehicle_id
        producer.poll(0)
        n += 1
    producer.flush(10)
    return n


def poll_with_retries(key):
    delay = 2
    for attempt in range(1, args.retries + 1):
        try:
            return poll_once(key)
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if code in (401, 403):
                sys.exit(f"511 rejected the key (HTTP {code}). Check API_511_KEY in .env.")
            if code == 429:
                print("HTTP 429: 511 rate limit hit, waiting 120s", flush=True)
                time.sleep(120)
                return None
            print(f"HTTP {code} (attempt {attempt}/{args.retries})", flush=True)
        except (requests.ConnectionError, requests.Timeout) as e:
            print(f"network problem (attempt {attempt}/{args.retries}): {e!r}", flush=True)
        except Exception as e:                                       # bad data etc: do not crash the loop
            print(f"poll failed (attempt {attempt}/{args.retries}): {e!r}", flush=True)
        if attempt < args.retries:
            time.sleep(delay)
            delay *= 2                                               # 2s, 4s, 8s ...
    return None


polls = 0
while True:
    started = time.time()
    n = poll_with_retries(keys[polls % len(keys)])
    polls += 1
    if n is None:
        print(f"[{time.strftime('%H:%M:%S')}] {args.agency}: no data this poll, will try again", flush=True)
    else:
        bad = f", {len(failed_deliveries)} delivery errors" if failed_deliveries else ""
        print(f"[{time.strftime('%H:%M:%S')}] {args.agency}: published {n} vehicle positions to {topic}{bad}", flush=True)
        failed_deliveries.clear()
    if args.once:
        sys.exit(0 if n else 1)
    time.sleep(max(0, interval - (time.time() - started)))
