"""Shared functions for 511 GTFS-realtime ingestion (a library, not a program).

Run one of these instead:
    python ingestion/producer_all.py     MAIN entry point: live vehicle positions, one request per agency
    python ingestion/producer_bart.py    BART trip delays (BART has no vehicle-position feed)
    python ingestion/producer_vta.py     one-feed VTA producer (the card demo)

Every event follows the common schema in ingestion/schema.py (B4): timestamps are epoch
milliseconds, speed is speed_mps, status is current_status, and schema_version is set.
publish() runs validate() on each record and skips and logs bad ones.

B13: vehicle ids are replaced by a keyed hash (ingestion/hashing.py, salt = VEHICLE_ID_SALT in .env)
BEFORE the event is built, and event_id is built from the hashed id, so a raw id never reaches Kafka.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests
from google.transit import gtfs_realtime_pb2
from ingestion.hashing import hash_vehicle_id, load_salt  # noqa: E402
from ingestion.schema import (SCHEMA_VERSION, TRIP_UPDATE_EVENT_FIELDS,  # noqa: E402,F401
                              VEHICLE_EVENT_FIELDS, make_trip_update_event_id,
                              make_vehicle_event_id, validate)

BASE = "https://api.511.org/transit"
STATUS = {0: "INCOMING_AT", 1: "STOPPED_AT", 2: "IN_TRANSIT_TO"}


def now_ms() -> int:
    return int(time.time() * 1000)


_SALT = None


def get_salt() -> bytes:
    """The B13 salt from .env (VEHICLE_ID_SALT). Raises ValueError with a clear message if it is missing."""
    global _SALT
    if _SALT is None:
        _SALT = load_salt()
    return _SALT


def fetch_feed(endpoint: str, agency: str, key: str) -> gtfs_realtime_pb2.FeedMessage:
    r = requests.get(f"{BASE}/{endpoint}", params={"api_key": key, "agency": agency}, timeout=30)
    r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)
    return feed


def vehicle_events(feed, agency: str, ingest_ts: int, salt: bytes = None):
    """GTFS-rt vehicle positions -> VEHICLE_EVENT records. ingest_ts is in milliseconds.

    vehicle_id is hashed (B13) and event_id is built from the hashed id."""
    salt = salt or get_salt()
    for ent in feed.entity:
        if not ent.HasField("vehicle"):
            continue
        v = ent.vehicle
        if not v.HasField("position"):
            continue
        raw_id = v.vehicle.id or ent.id
        if not raw_id:
            continue                                                  # nothing to identify the vehicle by
        vehicle_id = hash_vehicle_id(raw_id, salt, source=agency)     # B13: the raw id stops here
        seconds = int(v.timestamp) or int(feed.header.timestamp)
        event_ts = seconds * 1000 if seconds else ingest_ts          # GTFS-rt is in seconds
        pos = v.position
        yield {
            "event_id": make_vehicle_event_id(agency, vehicle_id, event_ts),
            "schema_version": SCHEMA_VERSION,
            "source": agency,
            "vehicle_id": vehicle_id,
            "route_id": v.trip.route_id or None,
            "trip_id": v.trip.trip_id or None,
            "direction_id": v.trip.direction_id if v.trip.HasField("direction_id") else None,
            "event_ts": event_ts,
            "ingest_ts": ingest_ts,
            "lat": pos.latitude,
            "lon": pos.longitude,
            "bearing": pos.bearing if pos.HasField("bearing") else None,
            "speed_mps": pos.speed if pos.HasField("speed") else None,
            "current_status": STATUS.get(v.current_status) if v.HasField("current_status") else None,
            "stop_id": v.stop_id or None,
        }


def trip_delay_events(feed, agency: str, ingest_ts: int, salt: bytes = None):
    """GTFS-rt trip updates -> TRIP_UPDATE_EVENT records (one per stop with a delay).

    A vehicle_id, when the feed has one, is hashed (B13). BART usually has none."""
    salt = salt or get_salt()
    for ent in feed.entity:
        if not ent.HasField("trip_update"):
            continue
        tu = ent.trip_update
        if not tu.trip.trip_id:
            continue
        seconds = int(tu.timestamp) or int(feed.header.timestamp)
        event_ts = seconds * 1000 if seconds else ingest_ts
        vehicle_id = hash_vehicle_id(tu.vehicle.id, salt, source=agency) if tu.vehicle.id else None
        for stu in tu.stop_time_update:
            ev = stu.arrival if stu.HasField("arrival") else stu.departure
            if not ev.HasField("delay"):
                continue
            stop_sequence = stu.stop_sequence if stu.HasField("stop_sequence") else None
            yield {
                "event_id": make_trip_update_event_id(agency, tu.trip.trip_id, stop_sequence, event_ts),
                "schema_version": SCHEMA_VERSION,
                "source": agency,
                "trip_id": tu.trip.trip_id,
                "route_id": tu.trip.route_id or None,
                "vehicle_id": vehicle_id,
                "stop_id": stu.stop_id or None,
                "stop_sequence": stop_sequence,
                "delay_sec": ev.delay,
                "event_ts": event_ts,
                "ingest_ts": ingest_ts,
            }


def valid_events(events, fields):
    """Pass through records that match the schema; log and skip the rest."""
    bad = 0
    for e in events:
        problems = validate(e, fields)
        if problems:
            bad += 1
            if bad <= 3:
                print(f"skipping invalid record ({'; '.join(problems)})", flush=True)
            continue
        yield e
    if bad:
        print(f"skipped {bad} invalid records in this batch", flush=True)


def publish(producer, topic, events, key_field, fields):
    """Validate, then produce each event keyed by key_field. Returns how many were published."""
    n = 0
    for e in valid_events(events, fields):
        producer.produce(topic, key=str(e.get(key_field) or ""), value=json.dumps(e))
        n += 1
        producer.poll(0)
    producer.flush()
    return n
