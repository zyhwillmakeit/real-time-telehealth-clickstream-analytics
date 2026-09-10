"""Generate Day 5 JSONL events and expected results; no network access."""

import argparse
import json
from pathlib import Path

from generator.customer_data import parse_utc_timestamp, read_customer_csv, sha256_file
from generator.journeys import generate_journeys
from scripts.validate_contracts import load_contract, validate_event

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--customers",
        type=Path,
        default=ROOT / "evidence/day-04/customer_snapshot_v1.csv",
    )
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data/day-05")
    parser.add_argument("--count", type=int, default=700)
    parser.add_argument("--seed", type=int, default=20260912)
    parser.add_argument("--start-at", default="2026-09-12T00:00:00Z")
    parser.add_argument("--as-of", default="2026-09-15T12:00:00Z")
    args = parser.parse_args()
    events, manifest = generate_journeys(
        read_customer_csv(args.customers),
        count=args.count,
        seed=args.seed,
        start_at=parse_utc_timestamp(args.start_at),
        as_of=parse_utc_timestamp(args.as_of),
    )
    schema, rules = load_contract()
    for event in events:
        if errors := validate_event(event, schema, rules):
            raise ValueError(f"Invalid generated event: {event['event_id']}: {errors}")
    manifest["customer_source_sha256"] = sha256_file(args.customers)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / "events.jsonl"
    output.write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in events))
    manifest["events_sha256"] = sha256_file(output)
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    print(
        json.dumps(
            {
                k: v
                for k, v in manifest.items()
                if k not in ("journeys", "appointments")
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
