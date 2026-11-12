"""
Check the bunching detector against events planted by fake_feed.py (B12 testing).

Matches each planted event (from data/planted_bunching.jsonl) to an alert with
the same route, direction and pair of buses, in a window that overlaps the
planted time. Reports recall: how many planted events were caught.

Run from the repo root, after stopping the detector:
    python streaming/evaluate_bunching.py

Planted events in the last --settle seconds before the feed stopped are reported
as "pending": their windows can only close when newer data pushes the watermark
past them, and the feed stopped sending data.
"""

import argparse
import json
import os

import pandas as pd


def to_ms(series):
    ts = pd.to_datetime(series, utc=True)
    return (ts - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(milliseconds=1)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--planted", default="data/planted_bunching.jsonl")
    p.add_argument("--alerts", default="data/alerts/bunching")
    p.add_argument("--settle", type=int, default=90,
                   help="seconds before feed stop that can't be judged yet (about window + watermark)")
    args = p.parse_args()

    planted, feed_stopped = [], None
    with open(args.planted) as f:
        for line in f:
            rec = json.loads(line)
            if rec["type"] == "planted_bunching":
                planted.append(rec)
            elif rec["type"] == "feed_stopped":
                feed_stopped = rec["ts"]

    if os.path.isdir(args.alerts):
        alerts = pd.read_parquet(args.alerts)
        alerts["start_ms"] = to_ms(alerts["window_start"])
        alerts["end_ms"] = to_ms(alerts["window_end"])
    else:
        alerts = pd.DataFrame(columns=["route_id", "direction_id", "vehicle_a", "vehicle_b",
                                       "start_ms", "end_ms", "distance_m"])

    caught, missed, pending = [], [], []
    matched_alerts = set()
    for ev in planted:
        pair = {ev["vehicle_a"], ev["vehicle_b"]}
        hits = alerts[(alerts["route_id"] == ev["route_id"])
                      & (alerts["direction_id"] == ev["direction_id"])
                      & (alerts["end_ms"] >= ev["start_ts"])
                      & (alerts["start_ms"] <= ev["end_ts"])]
        hits = hits[hits.apply(lambda r: {r["vehicle_a"], r["vehicle_b"]} == pair, axis=1)] \
            if len(hits) else hits
        if len(hits):
            caught.append(ev)
            matched_alerts.update(hits.index)
        elif feed_stopped and ev["end_ts"] > feed_stopped - args.settle * 1000:
            pending.append(ev)        # its window may simply not have closed yet
        else:
            missed.append(ev)

    judged = len(caught) + len(missed)
    print(f"Planted bunching events: {len(planted)}  (judged {judged}, pending {len(pending)})")
    print(f"Caught: {len(caught)}   Missed: {len(missed)}")
    if judged:
        print(f"Recall on planted events: {len(caught) / judged:.0%}")
    for ev in missed:
        print(f"  MISSED route {ev['route_id']} buses {ev['vehicle_a']} & {ev['vehicle_b']}")
    other = len(alerts) - len(matched_alerts)
    print(f"Total alerts: {len(alerts)}  |  matched to planted events: {len(matched_alerts)}  "
          f"|  other close pairs: {other}")
    if other:
        print("  Other close pairs may be real (random) bunching in the fake data; "
              "the exact ground-truth scan (B18) decides which are correct.")


if __name__ == "__main__":
    main()
