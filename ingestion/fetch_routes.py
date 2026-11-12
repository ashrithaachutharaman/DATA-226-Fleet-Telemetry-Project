"""Download official route lines (GTFS shapes) from 511 for the dashboard map.

    python ingestion/fetch_routes.py --agencies SF,SC,AC

Writes data/route_shapes.json. Each agency is one 511 request (they count toward the
60 requests/hour limit), and the files can be large, so this takes a moment.
"""
import argparse
import collections
import csv
import io
import json
import os
import sys
import time
import zipfile

import requests
from dotenv import load_dotenv

BASE = "https://api.511.org/transit"
OUT = "data/route_shapes.json"

load_dotenv()
ap = argparse.ArgumentParser()
ap.add_argument("--agencies", required=True, help="comma-separated 511 codes, e.g. SF,SC,AC")
ap.add_argument("--max-points", type=int, default=150, help="points kept per route line")
args = ap.parse_args()

raw = os.getenv("API_511_KEYS") or os.getenv("API_511_KEY") or ""
keys = [k.strip() for k in raw.split(",") if k.strip() and not k.strip().startswith(("PASTE", "your-"))]
if not keys:
    sys.exit("Set API_511_KEY in .env")
gap = 3600 / (60 * len(keys)) * 1.1

os.makedirs("data", exist_ok=True)
try:
    with open(OUT) as f:
        result = json.load(f)
except Exception:
    result = {}

agencies = [a.strip() for a in args.agencies.split(",") if a.strip()]
for i, agency in enumerate(agencies):
    key = keys[i % len(keys)]
    print(f"{agency}: downloading GTFS...", flush=True)
    try:
        r = requests.get(f"{BASE}/datafeeds", params={"api_key": key, "operator_id": agency}, timeout=180)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
    except Exception as e:
        print(f"{agency}: could not download ({e!r})", flush=True)
        continue
    files = {n.split("/")[-1]: n for n in z.namelist()}
    if "shapes.txt" not in files or "trips.txt" not in files:
        print(f"{agency}: this agency's GTFS has no shapes.txt, so no route lines", flush=True)
        continue

    def rows(name):
        with z.open(files[name]) as fh:
            yield from csv.DictReader(io.TextIOWrapper(fh, "utf-8-sig"))

    route_name = {}
    if "routes.txt" in files:
        for r_ in rows("routes.txt"):
            route_name[r_["route_id"]] = r_.get("route_short_name") or r_.get("route_long_name") or r_["route_id"]
    trips_on = collections.Counter()
    shape_route = {}
    for t in rows("trips.txt"):
        sid = t.get("shape_id")
        if sid:
            trips_on[sid] += 1
            shape_route[sid] = t["route_id"]
    by_route = collections.defaultdict(list)
    for sid, n in trips_on.items():
        by_route[shape_route[sid]].append((n, sid))
    keep = {sid for lst in by_route.values() for _, sid in sorted(lst, reverse=True)[:2]}  # 2 busiest shapes per route

    pts = collections.defaultdict(list)
    for s in rows("shapes.txt"):
        if s["shape_id"] in keep:
            pts[s["shape_id"]].append((float(s["shape_pt_sequence"]), float(s["shape_pt_lat"]), float(s["shape_pt_lon"])))
    lines = []
    for sid, p in pts.items():
        p.sort()
        step = max(1, len(p) // args.max_points)
        sampled = p[::step] + ([p[-1]] if (len(p) - 1) % step else [])
        rid = shape_route[sid]
        lines.append({"route": route_name.get(rid, rid), "pts": [[round(a, 5), round(b, 5)] for _, a, b in sampled]})
    result[agency] = lines
    print(f"{agency}: {len(lines)} route lines", flush=True)
    if i < len(agencies) - 1:
        time.sleep(gap)

with open(OUT, "w") as f:
    json.dump(result, f)
print(f"Saved {sum(len(v) for v in result.values())} route lines to {OUT}. Refresh the dashboard to see them.")
