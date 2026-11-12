"""B13: pseudonymize vehicle IDs at ingestion.

Raw vehicle IDs are replaced with a keyed hash (HMAC-SHA256) before an event
is published to Kafka, so raw IDs never reach Kafka, MongoDB, the warehouse
or the Parquet archive.

Design notes
- HMAC with a secret salt, not a bare hash. Vehicle IDs are short and guessable,
  so an unkeyed SHA-256 could be reversed by hashing every candidate ID.
- Deterministic: the same vehicle (and source) always maps to the same token,
  so detectors, windows and joins that group by vehicle_id still work.
- `source` is mixed into the hash when given (lowercased, matching the schema's
  codes like vta, muni, bart), so two agencies that reuse the same raw ID do
  not collide into one token and "VTA" and "vta" give the same token.
- The raw ID also appears inside the vehicle event's `event_id`
  (source:vehicle_id:event_ts), so pseudonymize_event rebuilds that id from the
  hashed value. Otherwise the raw ID would leak through it.
- Trip updates may have no vehicle_id (BART often omits it). Those events pass
  through unchanged, since there is nothing to hash.
- Apply exactly once, at ingestion. Hashing an already-hashed event would
  produce a different token for the same vehicle.
- The salt lives in .env (VEHICLE_ID_SALT), never in the repo. Changing the
  salt changes every token, so keep one salt for the whole project.
- Field names `vehicle_id` and `source` come from the common vehicle-event
  schema (card B4).

Generate a salt with:
    python -c "import secrets; print(secrets.token_hex(32))"
"""

import hashlib
import hmac
import os
from typing import Any, Mapping, Optional

from ingestion.schema import make_vehicle_event_id

SALT_ENV_VAR = "VEHICLE_ID_SALT"

# 32 hex chars = 128 bits, plenty to avoid collisions at fleet scale and half
# the storage of a full SHA-256 hex digest. Pass hex_length=64 for the full digest.
DEFAULT_HEX_LENGTH = 32

_SEPARATOR = b"\x1f"  # ASCII unit separator, so ("a", "bc") != ("ab", "c")


def load_salt(env: Optional[Mapping[str, str]] = None) -> bytes:
    """Read the salt from the environment (loading .env if python-dotenv is installed).

    Raises ValueError if the salt is missing or empty. There is deliberately no
    default: a hardcoded fallback salt would silently give weak pseudonymization.
    """
    if env is None:
        try:
            from dotenv import load_dotenv

            load_dotenv()
        except ImportError:
            pass
        env = os.environ

    salt = env.get(SALT_ENV_VAR, "")
    if not salt.strip():
        raise ValueError(
            f"{SALT_ENV_VAR} is not set. Add it to your .env file "
            "(see .env.example for how to generate one)."
        )
    return salt.encode("utf-8")


def hash_vehicle_id(
    vehicle_id: Any,
    salt: bytes,
    source: Optional[str] = None,
    hex_length: int = DEFAULT_HEX_LENGTH,
) -> str:
    """Return the pseudonymous token for one vehicle ID."""
    if vehicle_id is None or str(vehicle_id).strip() == "":
        raise ValueError("vehicle_id is missing or empty")
    if not salt:
        raise ValueError("salt must be non-empty")
    if not 1 <= hex_length <= 64:
        raise ValueError("hex_length must be between 1 and 64")

    message = str(vehicle_id).encode("utf-8")
    if source is not None:
        message = str(source).lower().encode("utf-8") + _SEPARATOR + message

    digest = hmac.new(salt, message, hashlib.sha256).hexdigest()
    return digest[:hex_length]


def pseudonymize_event(
    event: Mapping[str, Any],
    salt: bytes,
    hex_length: int = DEFAULT_HEX_LENGTH,
) -> dict:
    """Return a copy of `event` with `vehicle_id` replaced by its token.

    The input is not modified. The event's `source` field, if present, is used
    to keep tokens from different agencies apart. For vehicle events, `event_id`
    is rebuilt from the hashed ID so the raw ID does not leak through it.

    Events with no vehicle_id (None or absent, e.g. some trip updates) are
    returned unchanged. An empty string still raises ValueError.
    """
    if event.get("vehicle_id") is None:
        return dict(event)  # nothing to hash

    hashed = dict(event)
    hashed["vehicle_id"] = hash_vehicle_id(
        event["vehicle_id"], salt, source=event.get("source"), hex_length=hex_length
    )

    # Vehicle events have no stop_sequence; trip updates do, and their event_id
    # (source:trip_id:stop_sequence:event_ts) holds no vehicle ID.
    if (
        "event_id" in hashed
        and "source" in hashed
        and "event_ts" in hashed
        and "stop_sequence" not in hashed
    ):
        hashed["event_id"] = make_vehicle_event_id(
            hashed["source"], hashed["vehicle_id"], hashed["event_ts"]
        )
    return hashed
