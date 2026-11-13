"""Tests for B13 (run from the repo root: python -m pytest ingestion/test_hashing.py)."""

import hashlib
import hmac

import pytest

from ingestion.hashing import (
    DEFAULT_HEX_LENGTH,
    SALT_ENV_VAR,
    hash_vehicle_id,
    load_salt,
    pseudonymize_event,
)
from ingestion.schema import (
    SCHEMA_VERSION,
    TRIP_UPDATE_EVENT_FIELDS,
    VEHICLE_EVENT_FIELDS,
    make_trip_update_event_id,
    make_vehicle_event_id,
    validate,
)

SALT = b"test-salt-not-a-real-secret"

# Contains non-hex characters, so a hex token can never contain it by chance.
RAW_ID = "VTA-7012"


def vehicle_event(**overrides):
    """A record built to the B4 vehicle-event schema (passes validate())."""
    event_ts = 1700000000000
    event = {
        "event_id": make_vehicle_event_id("vta", RAW_ID, event_ts),
        "schema_version": SCHEMA_VERSION,
        "source": "vta",
        "vehicle_id": RAW_ID,
        "route_id": "22",
        "trip_id": "3156234",
        "direction_id": 0,
        "event_ts": event_ts,
        "ingest_ts": event_ts + 15000,
        "lat": 37.3352,
        "lon": -121.8811,
        "bearing": 270.0,
        "speed_mps": 8.5,
        "current_status": "IN_TRANSIT_TO",
        "stop_id": "4523",
    }
    event.update(overrides)
    return event


def trip_update_event(**overrides):
    """A record built to the B4 trip-update schema (passes validate())."""
    event_ts = 1700000000000
    event = {
        "event_id": make_trip_update_event_id("bart", "t-1", 3, event_ts),
        "schema_version": SCHEMA_VERSION,
        "source": "bart",
        "trip_id": "t-1",
        "route_id": "red",
        "vehicle_id": RAW_ID,
        "stop_id": "12TH",
        "stop_sequence": 3,
        "delay_sec": 60,
        "event_ts": event_ts,
        "ingest_ts": event_ts + 5000,
    }
    event.update(overrides)
    return event


def test_test_events_are_valid_against_schema():
    assert validate(vehicle_event(), VEHICLE_EVENT_FIELDS) == []
    assert validate(trip_update_event(), TRIP_UPDATE_EVENT_FIELDS) == []


def test_deterministic():
    assert hash_vehicle_id("1234", SALT) == hash_vehicle_id("1234", SALT)


def test_matches_independent_hmac_computation():
    expected = hmac.new(SALT, b"1234", hashlib.sha256).hexdigest()[:DEFAULT_HEX_LENGTH]
    assert hash_vehicle_id("1234", SALT) == expected


def test_different_ids_give_different_tokens():
    assert hash_vehicle_id("1234", SALT) != hash_vehicle_id("1235", SALT)


def test_different_salts_give_different_tokens():
    assert hash_vehicle_id("1234", SALT) != hash_vehicle_id("1234", b"another-salt")


def test_source_keeps_agencies_apart():
    assert hash_vehicle_id("1001", SALT, source="bart") != hash_vehicle_id(
        "1001", SALT, source="vta"
    )


def test_source_is_case_insensitive():
    assert hash_vehicle_id("1001", SALT, source="VTA") == hash_vehicle_id(
        "1001", SALT, source="vta"
    )


def test_separator_prevents_source_id_ambiguity():
    assert hash_vehicle_id("bc", SALT, source="a") != hash_vehicle_id(
        "c", SALT, source="ab"
    )


def test_integer_and_string_ids_match():
    assert hash_vehicle_id(1234, SALT) == hash_vehicle_id("1234", SALT)


@pytest.mark.parametrize("length", [8, 32, 64])
def test_hex_length(length):
    token = hash_vehicle_id("1234", SALT, hex_length=length)
    assert len(token) == length
    int(token, 16)  # valid hex


@pytest.mark.parametrize("length", [0, 65])
def test_bad_hex_length_rejected(length):
    with pytest.raises(ValueError):
        hash_vehicle_id("1234", SALT, hex_length=length)


@pytest.mark.parametrize("bad_id", [None, "", "   "])
def test_missing_vehicle_id_rejected(bad_id):
    with pytest.raises(ValueError):
        hash_vehicle_id(bad_id, SALT)


def test_empty_salt_rejected():
    with pytest.raises(ValueError):
        hash_vehicle_id("1234", b"")


def test_pseudonymize_vehicle_event_changes_only_vehicle_id_and_event_id():
    event = vehicle_event()
    original = dict(event)
    out = pseudonymize_event(event, SALT)

    assert event == original  # input not mutated
    assert out["vehicle_id"] == hash_vehicle_id(RAW_ID, SALT, source="vta")
    changed = {k for k in out if out[k] != original[k]}
    assert changed == {"vehicle_id", "event_id"}
    assert set(out) == set(original)


def test_pseudonymized_vehicle_event_still_matches_schema():
    out = pseudonymize_event(vehicle_event(), SALT)
    assert validate(out, VEHICLE_EVENT_FIELDS) == []


def test_raw_vehicle_id_appears_nowhere_in_output():
    out = pseudonymize_event(vehicle_event(), SALT)
    leaks = {k: v for k, v in out.items() if RAW_ID in str(v)}
    assert leaks == {}


def test_event_id_is_rebuilt_from_hashed_id():
    out = pseudonymize_event(vehicle_event(), SALT)
    assert out["event_id"] == make_vehicle_event_id(
        "vta", out["vehicle_id"], out["event_ts"]
    )


def test_event_id_stays_deterministic_for_duplicate_detection():
    first = pseudonymize_event(vehicle_event(), SALT)
    second = pseudonymize_event(vehicle_event(), SALT)
    assert first["event_id"] == second["event_id"]


def test_trip_update_with_vehicle_hashes_id_and_keeps_event_id():
    event = trip_update_event()
    out = pseudonymize_event(event, SALT)
    assert out["vehicle_id"] == hash_vehicle_id(RAW_ID, SALT, source="bart")
    assert out["event_id"] == event["event_id"]  # holds no vehicle ID
    assert RAW_ID not in str(out)
    assert validate(out, TRIP_UPDATE_EVENT_FIELDS) == []


def test_trip_update_with_none_vehicle_id_passes_through():
    event = trip_update_event(vehicle_id=None)
    out = pseudonymize_event(event, SALT)
    assert out == event
    assert out is not event
    assert validate(out, TRIP_UPDATE_EVENT_FIELDS) == []


def test_event_without_vehicle_id_field_passes_through():
    event = {"ts": 1}
    assert pseudonymize_event(event, SALT) == event


def test_empty_string_vehicle_id_in_event_rejected():
    with pytest.raises(ValueError):
        pseudonymize_event(vehicle_event(vehicle_id=""), SALT)


def test_pseudonymize_event_without_source_still_works():
    out = pseudonymize_event({"vehicle_id": "1234", "ts": 1}, SALT)
    assert out["vehicle_id"] == hash_vehicle_id("1234", SALT)


def test_load_salt_reads_env():
    assert load_salt({SALT_ENV_VAR: "abc123"}) == b"abc123"


@pytest.mark.parametrize("env", [{}, {SALT_ENV_VAR: ""}, {SALT_ENV_VAR: "   "}])
def test_load_salt_missing_raises(env):
    with pytest.raises(ValueError):
        load_salt(env)
