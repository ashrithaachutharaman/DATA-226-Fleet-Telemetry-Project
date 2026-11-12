"""Tests for B13 (run from the repo root: pytest ingestion/test_hashing.py)."""

import hashlib
import hmac

import pytest

from hashing import (
    DEFAULT_HEX_LENGTH,
    SALT_ENV_VAR,
    hash_vehicle_id,
    load_salt,
    pseudonymize_event,
)

SALT = b"test-salt-not-a-real-secret"


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
    assert hash_vehicle_id("1001", SALT, source="BART") != hash_vehicle_id(
        "1001", SALT, source="VTA"
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


def test_pseudonymize_event_replaces_only_vehicle_id():
    event = {
        "vehicle_id": "1234",
        "ts": 1700000000,
        "lat": 37.33,
        "lon": -121.89,
        "speed": 8.5,
        "route": "22",
        "trip": "t-1",
        "source": "VTA",
    }
    original = dict(event)
    out = pseudonymize_event(event, SALT)

    assert event == original  # input not mutated
    assert out["vehicle_id"] != "1234"
    assert "1234" not in out["vehicle_id"]
    assert out["vehicle_id"] == hash_vehicle_id("1234", SALT, source="VTA")
    assert {k: v for k, v in out.items() if k != "vehicle_id"} == {
        k: v for k, v in original.items() if k != "vehicle_id"
    }


def test_pseudonymize_event_without_source_still_works():
    out = pseudonymize_event({"vehicle_id": "1234", "ts": 1}, SALT)
    assert out["vehicle_id"] == hash_vehicle_id("1234", SALT)


def test_pseudonymize_event_requires_vehicle_id():
    with pytest.raises(KeyError):
        pseudonymize_event({"ts": 1}, SALT)


def test_load_salt_reads_env():
    assert load_salt({SALT_ENV_VAR: "abc123"}) == b"abc123"


@pytest.mark.parametrize("env", [{}, {SALT_ENV_VAR: ""}, {SALT_ENV_VAR: "   "}])
def test_load_salt_missing_raises(env):
    with pytest.raises(ValueError):
        load_salt(env)
