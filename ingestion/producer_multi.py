import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
from producer_511 import fetch_feed, publish, vehicle_events

load_dotenv()
ap = argparse.ArgumentParser()
ap.add_argument("--agencies", default="SF,SC", help="comma-separated 511 agency codes")
ap.add_argument("--interval", type=int, default=0, help="seconds between cycles (0 = automatic)")
ap.add_argument("--once", action="store_true")
args = ap.parse_args()

agencies = [a.strip() for a in args.agencies.split(",") if a.strip()]
key = os.getenv("API_511_KEY")
if not key or key.startswith("PASTE") or key.startswith("your-"):
    sys.exit("Set API_511_KEY in .env")
# 511 allows 60 requests/hour per key: one request per agency per cycle
interval = max(args.interval, 62 * len(agencies))

producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")})
topic = os.getenv("KAFKA_TOPIC_POSITIONS", "vehicle-positions")

while True:
    started = time.time()
    for agency in agencies:
        try:
            feed = fetch_feed("vehiclepositions", agency, key)
            n = publish(producer, topic, vehicle_events(feed, agency, int(time.time())), "vehicle_id")
            print(f"[{time.strftime('%H:%M:%S')}] {agency}: published {n}", flush=True)
        except requests.HTTPError as e:
            print(f"{agency}: HTTP error {e} (429 = rate limit)", flush=True)
        except Exception as e:
            print(f"{agency}: failed {e!r}", flush=True)
    if args.once:
        break
    time.sleep(max(0, interval - (time.time() - started)))
