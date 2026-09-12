import struct
from io import BytesIO

import pytest
from fastavro import schemaless_reader

from generator.delivery import encode_delivery, partition_key, plan_deliveries
from generator.kafka_delivery import send_records
from scripts.validate_contracts import VALID_FIXTURES, load_contract, load_json


def inputs():
    schema, rules = load_contract()
    events = [load_json(p) for p in sorted(VALID_FIXTURES.glob("*.json"))]
    return events, schema, rules


def test_frame_roundtrip_and_duplicate_identity():
    events, schema, rules = inputs()
    records = plan_deliveries(events, schema, rules, duplicate_rate=1)
    assert len(records) == 2 * len(events)
    for event in events:
        pair = [r for r in records if r["event"]["event_id"] == event["event_id"]]
        payloads = [encode_delivery(r, schema, rules, 123) for r in pair]
        assert payloads[0] == payloads[1]
        assert payloads[0][:5] == b"\x00" + struct.pack(">I", 123)
        decoded = schemaless_reader(BytesIO(payloads[0][5:]), schema)
        assert decoded["event_id"] == event["event_id"]
        assert partition_key(event)


def test_faults_deterministic_missing_and_corruption():
    events, schema, rules = inputs()
    options = {
        "delay_rate": 1,
        "reorder_rate": 1,
        "missing_rate": 1,
        "corrupt_rate": 1,
        "customer_ids": ["cus_99999999"],
    }
    records = plan_deliveries(events, schema, rules, **options)
    assert records == plan_deliveries(events, schema, rules, **options)
    for record in records:
        assert record["event"]["customer_id"] != "cus_99999999"
        assert "delayed" in record["flags"]
        with pytest.raises((EOFError, ValueError, IndexError)):
            schemaless_reader(
                BytesIO(encode_delivery(record, schema, rules, 1)[5:]), schema
            )
    customers = {
        r["event"]["customer_id"] for r in records if r["event"]["customer_id"]
    }
    assert customers == {"cus_99999998"}


class Message:
    def topic(self):
        return "test"

    def partition(self):
        return 0

    def offset(self):
        return 12


class Producer:
    def __init__(self, fail=False, remaining=0):
        self.fail, self.remaining = fail, remaining

    def produce(self, topic, **kwargs):
        if self.fail:

            class Error:
                def code(self):
                    return 3

            kwargs["on_delivery"](Error(), None)
        elif not self.remaining:
            kwargs["on_delivery"](None, Message())

    def poll(self, timeout):
        pass

    def flush(self, timeout):
        return self.remaining


def test_delivery_success_requires_all_acknowledged():
    events, schema, rules = inputs()
    records = plan_deliveries(events, schema, rules)
    assert (
        send_records(Producer(), "test", records, schema, rules, 1, rate=100000)[
            "status"
        ]
        == "PASS"
    )
    for producer in (Producer(fail=True), Producer(remaining=1)):
        report = send_records(producer, "test", records, schema, rules, 1, rate=100000)
        assert report["status"] == "FAILED"
        assert report["acknowledged"] == 0


def test_invalid_rates():
    events, schema, rules = inputs()
    with pytest.raises(ValueError):
        plan_deliveries(events, schema, rules, delay_rate=2)


def test_registry_registration_uses_subject_policy_and_returned_id(monkeypatch):
    from generator.kafka_delivery import register_schema

    calls = []

    class Response:
        status_code = 200

        def json(self):
            return {"id": 987}

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def put(self, url, **kwargs):
            calls.append((url, kwargs))
            return Response()

        def post(self, url, **kwargs):
            calls.append((url, kwargs))
            return Response()

    monkeypatch.setattr("generator.kafka_delivery.requests.Session", Session)
    env = {
        "SCHEMA_REGISTRY_URL": "https://registry.example",
        "SCHEMA_REGISTRY_API_KEY": "test",
        "SCHEMA_REGISTRY_API_SECRET": "test",
    }
    _, schema, _ = inputs()
    assert register_schema(env, "events", schema) == 987
    assert calls[0][0].endswith("/config/events-value")
    assert calls[0][1]["json"] == {"compatibility": "BACKWARD_TRANSITIVE"}
    assert calls[1][0].endswith("/subjects/events-value/versions")
    assert calls[1][1]["json"]["schemaType"] == "AVRO"
