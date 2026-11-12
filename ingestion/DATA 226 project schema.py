"""
Common event schemas for the fleet telemetry pipeline (backlog item B4).

Every producer (GTFS-realtime today, Waymo/Argoverse replay later) converts its
raw data into these flat records before publishing to Kafka. Spark, the
storage layers and the benchmarks all read this one definition, so the field
list lives here once and everything else imports it.

Two record types:
  VEHICLE_EVENT     one position report from one vehicle   (topic: vehicle-positions)
  TRIP_UPDATE_EVENT one predicted delay at one stop on a trip (topic: trip-updates)

Conventions
  - Timestamps are UTC epoch milliseconds (long). GTFS-rt sends seconds: multiply by 1000.
  - Distances in meters, speeds in meters per second, angles in degrees.
  - Optional fields are None/null when the feed does not provide them.
  - Bump SCHEMA_VERSION whenever a field is added, removed or changes meaning.
"""

SCHEMA_VERSION = 1

TOPIC_VEHICLE_POSITIONS = "vehicle-positions"
TOPIC_TRIP_UPDATES = "trip-updates"

# (name, type, nullable, description)
# Types are Spark SQL names: string, long, int, double.
VEHICLE_EVENT_FIELDS = [
    ("event_id",       "string", False, "Deterministic id: source:vehicle_id:event_ts. Same report polled twice -> same id, so duplicates can be detected (Bloom filter, B17)."),
    ("schema_version", "int",    False, "Version of this schema the record was written with."),
    ("source",         "string", False, "Data source / agency code, e.g. 'vta', 'muni', 'bart', later 'waymo'."),
    ("vehicle_id",     "string", False, "Vehicle identifier from the feed. Hashed at ingestion in B13 for privacy."),
    ("route_id",       "string", True,  "Route the vehicle is serving. Needed for bus bunching (group by route)."),
    ("trip_id",        "string", True,  "Scheduled trip the vehicle is running. Joins to trip updates."),
    ("direction_id",   "int",    True,  "0 or 1: direction of travel on the route. Bunching compares vehicles going the same way."),
    ("event_ts",       "long",   False, "When the vehicle reported this position (event time). Windows and watermarks use this."),
    ("ingest_ts",      "long",   False, "When our producer received it (processing time). ingest_ts - event_ts = feed lag."),
    ("lat",            "double", False, "Latitude, degrees (WGS84)."),
    ("lon",            "double", False, "Longitude, degrees (WGS84)."),
    ("bearing",        "double", True,  "Heading in degrees clockwise from true north. Often missing."),
    ("speed_mps",      "double", True,  "Speed in m/s if the feed sends it. Often missing; Spark derives speed from consecutive positions."),
    ("current_status", "string", True,  "INCOMING_AT, STOPPED_AT or IN_TRANSIT_TO. Helps separate stalls from normal stops."),
    ("stop_id",        "string", True,  "Stop the status refers to."),
]

TRIP_UPDATE_EVENT_FIELDS = [
    ("event_id",       "string", False, "Deterministic id: source:trip_id:stop_sequence:event_ts."),
    ("schema_version", "int",    False, "Version of this schema the record was written with."),
    ("source",         "string", False, "Data source / agency code."),
    ("trip_id",        "string", False, "Trip being updated."),
    ("route_id",       "string", True,  "Route of the trip."),
    ("vehicle_id",     "string", True,  "Vehicle running the trip, if the feed says. Hashed in B13."),
    ("stop_id",        "string", True,  "Stop this prediction is for."),
    ("stop_sequence",  "int",    True,  "Order of the stop within the trip (1, 2, 3, ...). Delay propagation compares consecutive stops."),
    ("delay_sec",      "int",    True,  "Predicted arrival delay in seconds vs schedule. Positive = late, negative = early."),
    ("event_ts",       "long",   False, "Timestamp of the trip update from the feed."),
    ("ingest_ts",      "long",   False, "When our producer received it."),
]


def make_vehicle_event_id(source, vehicle_id, event_ts):
    """Build the deterministic id for a vehicle event."""
    return f"{source}:{vehicle_id}:{event_ts}"


def make_trip_update_event_id(source, trip_id, stop_sequence, event_ts):
    """Build the deterministic id for a trip-update event."""
    return f"{source}:{trip_id}:{stop_sequence}:{event_ts}"


def validate(record, fields):
    """Return a list of problems with a record (empty list = valid).

    Producers call this before publishing, so bad records are caught at the
    edge instead of failing inside Spark.
    """
    python_types = {"string": str, "long": int, "int": int, "double": (int, float)}
    problems = []
    names = {f[0] for f in fields}
    for extra in set(record) - names:
        problems.append(f"unexpected field '{extra}'")
    for name, ftype, nullable, _ in fields:
        value = record.get(name)
        if value is None:
            if not nullable:
                problems.append(f"'{name}' is required")
            continue
        if isinstance(value, bool) or not isinstance(value, python_types[ftype]):
            problems.append(f"'{name}' should be {ftype}, got {type(value).__name__}")
    return problems


def spark_schema(fields):
    """Build a Spark StructType from a field list.

    pyspark is imported here, not at the top, so producers can use this file
    without installing Spark.
    """
    from pyspark.sql.types import (DoubleType, IntegerType, LongType,
                                   StringType, StructField, StructType)
    spark_types = {"string": StringType(), "long": LongType(),
                   "int": IntegerType(), "double": DoubleType()}
    return StructType([StructField(name, spark_types[ftype], nullable)
                       for name, ftype, nullable, _ in fields])


if __name__ == "__main__":
    # Quick self-test: python ingestion/schema.py
    import time

    now_ms = int(time.time() * 1000)
    example = {
        "event_id": make_vehicle_event_id("vta", "7012", now_ms - 15000),
        "schema_version": SCHEMA_VERSION,
        "source": "vta",
        "vehicle_id": "7012",
        "route_id": "22",
        "trip_id": "3156234",
        "direction_id": 0,
        "event_ts": now_ms - 15000,
        "ingest_ts": now_ms,
        "lat": 37.3352,
        "lon": -121.8811,
        "bearing": 270.0,
        "speed_mps": None,
        "current_status": "IN_TRANSIT_TO",
        "stop_id": "4523",
    }
    print("Example vehicle event:")
    for k, v in example.items():
        print(f"  {k:15} {v}")
    print("Problems:", validate(example, VEHICLE_EVENT_FIELDS) or "none")

    broken = dict(example, lat="37.33", vehicle_id=None)
    print("Problems in a broken record:", validate(broken, VEHICLE_EVENT_FIELDS))

    try:
        print(spark_schema(VEHICLE_EVENT_FIELDS).simpleString())
    except ImportError:
        print("(pyspark not installed; Spark schema check skipped)")
