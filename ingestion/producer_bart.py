import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from producer_511 import TRIP_UPDATE_EVENT_FIELDS, get_salt, now_ms, publish, trip_delay_events

URL = "http://api.bart.gov/gtfsrt/tripupdate.aspx"   # no key needed; BART has no vehicle-position feed

load_dotenv()
try:
    get_salt()                      # B13: stop now if VEHICLE_ID_SALT is not set, instead of failing on every poll
except ValueError as e:
    sys.exit(str(e))
ap = argparse.ArgumentParser()
ap.add_argument("--interval", type=int, default=30)
ap.add_argument("--once", action="store_true")
args = ap.parse_args()

producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")})
topic = os.getenv("KAFKA_TOPIC_TRIPS", "trip-updates")

while True:
    started = time.time()
    try:
        r = requests.get(URL, timeout=30)
        r.raise_for_status()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(r.content)
        n = publish(producer, topic, trip_delay_events(feed, "BA", now_ms()), "trip_id", TRIP_UPDATE_EVENT_FIELDS)
        print(f"[{time.strftime('%H:%M:%S')}] BART trip delays published: {n} "
              f"(from {len(feed.entity)} trips)", flush=True)
    except Exception as e:
        print(f"BART poll failed: {e!r}", flush=True)
    if args.once:
        break
    time.sleep(max(0, args.interval - (time.time() - started)))
