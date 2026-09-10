"""Validate the Day 3 Avro schema, rule matrix, and example fixtures."""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastavro import parse_schema
from fastavro.validation import validate

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_ROOT = PROJECT_ROOT / "contracts" / "events"
SCHEMA_PATH = CONTRACT_ROOT / "clickstream_event_v1.avsc"
RULES_PATH = CONTRACT_ROOT / "event_rules_v1.json"
VALID_FIXTURES = CONTRACT_ROOT / "examples" / "valid"
INVALID_FIXTURES = CONTRACT_ROOT / "examples" / "invalid"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_contract() -> tuple[dict[str, Any], dict[str, Any]]:
    schema = load_json(SCHEMA_PATH)
    rules = load_json(RULES_PATH)
    return schema, rules


def parse_utc_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("Timestamp must include UTC offset")
    return parsed.astimezone(timezone.utc)


def normalize_timestamps(
    event: dict[str, Any], fields: list[str]
) -> tuple[dict[str, Any], list[str]]:
    normalized = event.copy()
    errors: list[str] = []
    for field in fields:
        value = normalized.get(field)
        if value is None or isinstance(value, datetime):
            continue
        if not isinstance(value, str):
            errors.append(f"TIMESTAMP_INVALID:{field}")
            continue
        try:
            normalized[field] = parse_utc_timestamp(value)
        except ValueError:
            errors.append(f"TIMESTAMP_INVALID:{field}")
    return normalized, errors


def schema_event_names(schema: dict[str, Any]) -> set[str]:
    event_field = next(
        field for field in schema["fields"] if field["name"] == "event_name"
    )
    return set(event_field["type"]["symbols"])


def validate_event(
    event: dict[str, Any], schema: dict[str, Any], rules: dict[str, Any]
) -> list[str]:
    """Return stable error codes for structural and business contract failures."""
    known_fields = {field["name"] for field in schema["fields"]}
    if set(event) - known_fields:
        return ["AVRO_SCHEMA_INVALID"]

    normalized, timestamp_errors = normalize_timestamps(
        event, rules["timestamp_fields"]
    )
    if timestamp_errors:
        return sorted(set(timestamp_errors))

    parsed = parse_schema(schema)
    if not validate(normalized, parsed, raise_errors=False, strict=True):
        return ["AVRO_SCHEMA_INVALID"]

    errors: list[str] = []

    for field in rules["global_required_fields"]:
        if event.get(field) in (None, ""):
            errors.append(f"FIELD_REQUIRED:{field}")

    if not any(
        event.get(field) not in (None, "")
        for field in rules["identity_at_least_one_of"]
    ):
        errors.append("IDENTITY_REQUIRED")

    event_name = event["event_name"]
    for field in rules["event_required_fields"][event_name]:
        if event.get(field) in (None, ""):
            errors.append(f"FIELD_REQUIRED:{field}")

    for field, allowed in rules["allowed_values"].items():
        value = event.get(field)
        if value is not None and value not in allowed:
            errors.append(f"VALUE_NOT_ALLOWED:{field}")

    device_type = event.get("device_type")
    platform_name = event.get("platform")
    allowed_platforms = rules["platforms_by_device"].get(device_type, [])
    if allowed_platforms and platform_name not in allowed_platforms:
        errors.append("DEVICE_PLATFORM_MISMATCH")

    for field, pattern in rules["identifier_patterns"].items():
        value = event.get(field)
        if value not in (None, "") and not re.fullmatch(pattern, value):
            errors.append(f"IDENTIFIER_FORMAT_INVALID:{field}")

    if event.get("schema_version") != rules["schema_version"]:
        errors.append("SCHEMA_VERSION_UNSUPPORTED")

    event_time = normalized["event_time"]
    received_at = normalized["received_at"]
    max_skew = timedelta(seconds=rules["max_event_clock_skew_seconds"])
    if event_time > received_at + max_skew:
        errors.append("EVENT_TIME_TOO_FAR_IN_FUTURE")

    scheduled_start = normalized.get("scheduled_start_time")
    if (
        event_name == "appointment_booked"
        and scheduled_start
        and scheduled_start < event_time
    ):
        errors.append("SCHEDULED_START_BEFORE_BOOKING")

    return sorted(set(errors))


def validate_contract_files() -> list[str]:
    schema, rules = load_contract()
    failures: list[str] = []

    schema_names = schema_event_names(schema)
    rule_names = set(rules["event_required_fields"])
    if schema_names != rule_names:
        failures.append(
            f"Event-name mismatch: schema_only={sorted(schema_names - rule_names)}, "
            f"rules_only={sorted(rule_names - schema_names)}"
        )

    for path in sorted(VALID_FIXTURES.glob("*.json")):
        errors = validate_event(load_json(path), schema, rules)
        if errors:
            failures.append(
                f"{path.relative_to(PROJECT_ROOT)} expected valid, got {errors}"
            )

    for path in sorted(INVALID_FIXTURES.glob("*.json")):
        fixture = load_json(path)
        actual = validate_event(fixture["event"], schema, rules)
        expected = sorted(fixture["expected_error_codes"])
        if actual != expected:
            failures.append(
                f"{path.relative_to(PROJECT_ROOT)} expected {expected}, got {actual}"
            )

    return failures


def main() -> int:
    failures = validate_contract_files()
    if failures:
        print("Contract validation: FAIL")
        for failure in failures:
            print(f"- {failure}")
        return 1

    valid_count = len(list(VALID_FIXTURES.glob("*.json")))
    invalid_count = len(list(INVALID_FIXTURES.glob("*.json")))
    print(
        f"Contract validation: PASS ({valid_count} valid and {invalid_count} invalid fixtures)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
