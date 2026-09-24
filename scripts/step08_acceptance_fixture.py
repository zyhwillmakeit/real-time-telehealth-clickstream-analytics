"""Build the six-case Step 8 fixture offline; send only with explicit --send.

Run from the repository root with: python -m scripts.step08_acceptance_fixture
"""
import argparse
import json
import os
from copy import deepcopy
from pathlib import Path

from generator.delivery import encode_delivery
from pipelines.event_validation import classify
from scripts.validate_contracts import VALID_FIXTURES, load_contract


def build_fixture():
    schema, rules = load_contract()
    base = json.loads((VALID_FIXTURES / "appointment_booked.json").read_text())
    cases = [
        ("corrupt_avro", 8001, {}, True, ["AVRO_DECODE_FAILED"]),
        ("multiple_errors", 8002, {"appointment_id": None, "device_type": "desktop", "platform": "ios"},
         False, ["FIELD_REQUIRED:appointment_id", "DEVICE_PLATFORM_MISMATCH"]),
        ("late_event", 8003, {"received_at": "2026-09-10T15:39:00.000000Z"}, False, []),
        ("missing_customer", 8004, {"customer_id": "cus_99999999"}, False, []),
        ("duplicate_a", 8005, {}, False, []),
        ("duplicate_b", 8005, {}, False, []),
    ]
    records, expected = [], []
    for label, number, changes, corrupt, errors in cases:
        event = deepcopy(base)
        event.update(event_id=f"evt_{number:032x}", **changes)
        record = {"delivery_id": label, "event": event, "corrupt": corrupt}
        want = {"delivery_id": label, "event_id": event["event_id"],
                "route": "quarantine" if errors else "valid", "error_codes": sorted(errors)}
        got = classify(encode_delivery(record, schema, rules, 1), {1: schema}, schema, rules)
        assert got["route"] == want["route"] and sorted(got["error_codes"]) == want["error_codes"]
        records.append(record)
        expected.append(want)
    return schema, rules, records, expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--send", action="store_true", help="Register schema and send to Kafka")
    parser.add_argument("--topic", default="telehealth-step08-negative-01")
    parser.add_argument("--output", type=Path, default=Path("data/step08-negative-01"))
    args = parser.parse_args()
    schema, rules, records, expected = build_fixture()
    if not args.send:
        print(json.dumps({"mode": "OFFLINE", "expected": expected}, indent=2))
        return
    # Exclusive directory prevents an accidental resend using the same run directory.
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "expected.json").write_text(json.dumps(expected, indent=2) + "\n")
    from dotenv import load_dotenv
    from generator.kafka_delivery import kafka_config, make_producer, register_schema, send_records
    load_dotenv(".env")
    env = dict(os.environ)
    schema_id = register_schema(env, args.topic, schema)
    report = send_records(make_producer(kafka_config(env)), args.topic, records, schema, rules, schema_id, rate=2)
    (args.output / "producer-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "acknowledged": report["acknowledged"],
                      "output_directory": str(args.output.resolve())}, indent=2))
    if report["status"] != "PASS":
        raise SystemExit("Partial/failed delivery: preserve the report and investigate before sending again")


if __name__ == "__main__":
    main()
