import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
from producer_511 import BASE, VEHICLE_EVENT_FIELDS, fetch_feed, get_salt, now_ms, publish, vehicle_events

load_dotenv()
ap = argparse.ArgumentParser(description="Poll vehicle positions for many 511 agencies")
ap.add_argument("--agencies", default="ALL",
                help="ALL (every operator 511 lists), or comma-separated codes like SF,SC,AC")
ap.add_argument("--exclude", default="RG", help="codes to skip when using ALL")
ap.add_argument("--once", action="store_true", help="one pass over the agencies, then exit")
ap.add_argument("--list", action="store_true", help="print 511's real operator codes and names, then exit")
args = ap.parse_args()

raw = os.getenv("API_511_KEYS") or os.getenv("API_511_KEY") or ""
keys = [k.strip() for k in raw.split(",") if k.strip() and not k.strip().startswith(("PASTE", "your-"))]
if not keys:
    sys.exit("Set API_511_KEY (or API_511_KEYS=key1,key2,key3 for teammates' keys) in .env")
try:
    get_salt()                      # B13: stop now if VEHICLE_ID_SALT is not set, instead of failing on every poll
except ValueError as e:
    sys.exit(str(e))
# 511 allows 60 requests/hour per key. Spread requests evenly across all keys.
gap = 3600 / (60 * len(keys)) * 1.1
print(f"{len(keys)} key(s): one request every {gap:.0f}s", flush=True)


def find_list(obj):
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for v in obj.values():
            found = find_list(v)
            if found:
                return found
    return []


def list_operators():
    r = requests.get(f"{BASE}/gtfsoperators", params={"api_key": keys[0]}, timeout=30)
    r.raise_for_status()
    data = json.loads(r.content.decode("utf-8-sig"))
    ops = {}
    for d in find_list(data):
        if isinstance(d, dict):
            oid = d.get("Id") or d.get("id") or d.get("OperatorId")
            if oid:
                ops[str(oid)] = d.get("Name") or d.get("name") or str(oid)
    if not ops:
        sys.exit("Could not read the operator list from 511. First 300 characters: " + r.text[:300])
    return ops


if args.list:
    for oid, name in list_operators().items():
        print(f"{oid:8s} {name}")
    sys.exit(0)

if args.agencies.upper() == "ALL":
    operators = list_operators()
    skip = {a.strip() for a in args.exclude.split(",")}
    agencies = [a for a in operators if a not in skip]
    os.makedirs("data", exist_ok=True)
    with open("data/agency_names.json", "w") as f:
        json.dump(operators, f)   # the dashboard uses this to show real agency names
    print(f"511 lists {len(operators)} operators; polling {len(agencies)}", flush=True)
else:
    agencies = [a.strip() for a in args.agencies.split(",") if a.strip()]
    try:   # save the real agency names so the dashboard can label each one
        os.makedirs("data", exist_ok=True)
        with open("data/agency_names.json", "w") as f:
            json.dump(list_operators(), f)
    except Exception as e:
        print(f"could not save agency names ({e!r}); the dashboard will show codes", flush=True)

producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")})
topic = os.getenv("KAFKA_TOPIC_POSITIONS", "vehicle-positions")

streak = {}      # agency -> consecutive polls with no vehicles
resume = {}      # agency -> first cycle it should be polled again
cycle = 0
req = 0
while True:
    total = 0
    for agency in agencies:
        if resume.get(agency, 0) > cycle:
            continue
        key = keys[req % len(keys)]
        req += 1
        started = time.time()
        try:
            feed = fetch_feed("vehiclepositions", agency, key)
            n = publish(producer, topic, vehicle_events(feed, agency, now_ms()), "vehicle_id", VEHICLE_EVENT_FIELDS)
            total += n
            print(f"[{time.strftime('%H:%M:%S')}] {agency}: published {n}", flush=True)
            bad = n == 0
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            print(f"{agency}: HTTP {code}", flush=True)
            if code == 429:
                time.sleep(120)
                bad = False
            else:
                bad = True
        except Exception as e:
            print(f"{agency}: failed {e!r}", flush=True)
            bad = False
        if bad:
            # no real-time vehicles from this agency: ask less often so it doesn't use up the limit
            streak[agency] = streak.get(agency, 0) + 1
            resume[agency] = cycle + min(2 ** streak[agency], 32)
        else:
            streak[agency] = 0
        if not (args.once and agency == agencies[-1]):
            time.sleep(max(0, gap - (time.time() - started)))
    cycle += 1
    print(f"--- pass {cycle} done: {total} vehicle reports published", flush=True)
    if args.once:
        break
