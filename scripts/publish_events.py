"""Plan locally by default; --send registers Avro and publishes to Kafka."""

import argparse
import json
import os
from collections import Counter
from pathlib import Path

from generator.customer_data import read_customer_csv, sha256_file
from generator.delivery import encode_delivery, plan_deliveries
from generator.kafka_delivery import (
    kafka_config,
    make_producer,
    register_schema,
    send_records,
)
from scripts.validate_contracts import load_contract

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=ROOT / "data/day-05/events.jsonl")
    parser.add_argument(
        "--customers",
        type=Path,
        default=ROOT / "evidence/day-04/customer_snapshot_v1.csv",
    )
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data/day-06")
    parser.add_argument("--seed", type=int, default=6)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--topic")
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--rate", type=float, default=100)
    parser.add_argument("--delay-seconds", type=float, default=2100)
    for name in ("duplicate", "delay", "reorder", "corrupt", "missing"):
        parser.add_argument("--" + name + "-rate", type=float, default=0)
    args = parser.parse_args()
    if args.rate <= 0 or (args.limit is not None and args.limit <= 0):
        parser.error("Rate and limit must be positive")
    # A report directory represents one attempt. Never overwrite prior delivery evidence.
    if args.output_dir.exists():
        parser.error("Output directory already exists; choose a new --output-dir")
    schema, rules = load_contract()
    events = [
        json.loads(line) for line in args.input.read_text().splitlines() if line.strip()
    ]
    if args.limit:
        events = events[: args.limit]
    records = plan_deliveries(
        events,
        schema,
        rules,
        seed=args.seed,
        customer_ids=[c.customer_id for c in read_customer_csv(args.customers)],
        duplicate_rate=args.duplicate_rate,
        delay_rate=args.delay_rate,
        reorder_rate=args.reorder_rate,
        corrupt_rate=args.corrupt_rate,
        missing_rate=args.missing_rate,
        delay_seconds=args.delay_seconds,
    )
    # Offline ID 1 is only for encoding validation, never sent as a Registry ID.
    for record in records:
        encode_delivery(record, schema, rules, 1)
    manifest = {
        "mode": "compressed_historical_replay",
        "seed": args.seed,
        "input_sha256": sha256_file(args.input),
        "customer_sha256": sha256_file(args.customers),
        "planned_deliveries": len(records),
        "expected_valid_unique_events": len(
            {r["event"]["event_id"] for r in records if not r["corrupt"]}
        ),
        "flags": dict(Counter(flag for r in records for flag in r["flags"])),
        "rates": {
            name: getattr(args, name + "_rate")
            for name in ("duplicate", "delay", "reorder", "corrupt", "missing")
        },
        "delay_seconds": args.delay_seconds,
        "expected_corrupt_deliveries": sum(r["corrupt"] for r in records),
        "status": "OFFLINE_VALIDATED",
    }
    args.output_dir.mkdir(parents=True)
    (args.output_dir / "plan.jsonl").write_text(
        "".join(json.dumps(r, sort_keys=True) + "\n" for r in records)
    )
    if args.send:
        from dotenv import load_dotenv

        load_dotenv(args.env_file, override=False)
        try:
            topic = args.topic or os.environ.get("KAFKA_TOPIC")
            if not topic:
                raise ValueError("Missing KAFKA_TOPIC")
            if (args.corrupt_rate or args.missing_rate) and not args.topic:
                raise ValueError(
                    "Fault injection requires explicit --topic for a test topic"
                )
            config = kafka_config(os.environ)
            producer = make_producer(config)
            metadata = producer.list_topics(topic=topic, timeout=15)
            if topic not in metadata.topics or metadata.topics[topic].error:
                raise ValueError("Target topic must already exist and be accessible")
            schema_id = register_schema(os.environ, topic, schema)
            manifest.update(topic=topic, schema_id=schema_id)
            report = send_records(
                producer, topic, records, schema, rules, schema_id, rate=args.rate
            )
            (args.output_dir / "delivery-report.json").write_text(
                json.dumps(report, indent=2) + "\n"
            )
            manifest["status"] = report["status"]
            manifest["acknowledged"] = report["acknowledged"]
        except Exception as exc:  # noqa: BLE001 - redact third-party failures
            # Third-party exception strings may contain endpoints or credentials.
            manifest.update(status="FAILED", error_type=type(exc).__name__)
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(manifest, indent=2))
    return 1 if manifest["status"] == "FAILED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
