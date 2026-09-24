import json
import struct
from datetime import datetime

import pytest

from generator.delivery import encode_delivery
from pipelines.event_validation import RegistryResolver, RegistryUnavailable, classify
from pipelines.silver_validation import validate_names
from scripts.validate_contracts import (
    VALID_FIXTURES,
    load_contract,
    load_json,
    validate_event,
)


def setup():
    schema, rules = load_contract()
    event = load_json(VALID_FIXTURES / "appointment_booked.json")
    return schema, rules, event


def wire(event, schema, rules):
    return encode_delivery({"event": event, "corrupt": False}, schema, rules, 42)


def test_valid_decoding_keeps_business_identity():
    schema, rules, event = setup()
    result = classify(wire(event, schema, rules), {42: schema}, schema, rules)
    assert result["route"] == "valid"
    assert result["error_codes"] == []
    assert json.loads(result["event_json"])["event_id"] == event["event_id"]


def test_multiple_errors_and_future_time_before_watermark():
    schema, rules, event = setup()
    event.update(
        appointment_id=None,
        device_type="desktop",
        platform="ios",
        event_time="2026-09-11T00:00:00Z",
    )
    result = classify(wire(event, schema, rules), {42: schema}, schema, rules)
    assert result["route"] == "quarantine"
    assert set(result["error_codes"]) == {
        "FIELD_REQUIRED:appointment_id",
        "DEVICE_PLATFORM_MISMATCH",
        "EVENT_TIME_TOO_FAR_IN_FUTURE",
    }


@pytest.mark.parametrize(
    "payload,code",
    [
        (None, "KAFKA_TOMBSTONE"),
        (b"", "FRAME_TRUNCATED"),
        (b"hello", "MAGIC_BYTE_INVALID"),
        (b"\x00" + struct.pack(">I", 42), "AVRO_DECODE_FAILED"),
    ],
)
def test_bad_wire(payload, code):
    schema, rules, _ = setup()
    assert classify(payload, {42: schema}, schema, rules)["error_codes"] == [code]


def test_unknown_unsupported_and_missing_prefetch_differ():
    schema, rules, event = setup()
    payload = wire(event, schema, rules)
    assert classify(payload, {42: None}, schema, rules)["error_codes"] == [
        "SCHEMA_ID_UNKNOWN"
    ]
    assert classify(payload, {42: {"type": "string"}}, schema, rules)[
        "error_codes"
    ] == ["WRITER_SCHEMA_UNSUPPORTED"]
    with pytest.raises(RegistryUnavailable):
        classify(payload, {}, schema, rules)
    assert classify(payload + b"extra", {42: schema}, schema, rules)["error_codes"] == [
        "AVRO_TRAILING_BYTES"
    ]


def test_late_missing_customer_and_duplicate_are_valid():
    schema, rules, event = setup()
    event["received_at"] = "2026-09-15T00:00:00Z"
    event["customer_id"] = "cus_99999999"
    payload = wire(event, schema, rules)
    assert classify(payload, {42: schema}, schema, rules)["route"] == "valid"
    assert classify(payload, {42: schema}, schema, rules) == classify(
        payload, {42: schema}, schema, rules
    )


def test_naive_datetime_rejected_by_shared_validator():
    schema, rules, event = setup()
    event["event_time"] = datetime(2026, 9, 10)  # noqa: DTZ001 - intentional invalid input
    assert validate_event(event, schema, rules) == ["TIMESTAMP_INVALID:event_time"]


def test_registry_errors_fail_batch_and_success_cached(monkeypatch):
    resolver = RegistryResolver("https://registry.example", "key", "secret")

    class Response:
        status_code = 503

        def json(self):
            return {"schema": json.dumps(setup()[0])}

    response = Response()
    calls = []

    def get(*args, **kwargs):
        calls.append(args)
        return response

    monkeypatch.setattr(resolver.session, "get", get)
    with pytest.raises(RegistryUnavailable):
        resolver.resolve(42)
    response.status_code = 200
    assert resolver.resolve(42) == setup()[0]
    assert resolver.resolve(42) == setup()[0]
    assert len(calls) == 2


def test_invalid_config():
    with pytest.raises(ValueError):
        validate_names("a.b.c", "a.b.c", "/tmp/x", "app")
