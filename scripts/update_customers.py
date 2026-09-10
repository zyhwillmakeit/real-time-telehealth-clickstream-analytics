"""Create a deterministic customer update snapshot and optionally load it."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

from generator.customer_data import (
    apply_deterministic_updates,
    build_manifest,
    parse_utc_timestamp,
    read_customer_csv,
    sha256_file,
    validate_customers,
    write_change_csv,
    write_customer_csv,
    write_manifest,
)
from generator.customer_postgres import load_customer_snapshot

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "evidence" / "day-04" / "customer_snapshot_v1.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "customer_snapshot_v2.csv"
DEFAULT_CHANGES = PROJECT_ROOT / "data" / "customer_changes_v2.csv"
DEFAULT_MANIFEST = PROJECT_ROOT / "data" / "customer_snapshot_v2.manifest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--changes-output", type=Path, default=DEFAULT_CHANGES)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--seed", type=int, default=20_260_911)
    parser.add_argument("--snapshot-id", default="customer_snapshot_v2")
    parser.add_argument("--effective-at", default="2026-09-11T12:00:00Z")
    parser.add_argument("--attribute-updates", type=int, default=300)
    parser.add_argument("--soft-deletes", type=int, default=50)
    parser.add_argument("--inserts", type=int, default=100)
    parser.add_argument("--load-postgres", action="store_true")
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    effective_at = parse_utc_timestamp(args.effective_at)
    source_customers = read_customer_csv(args.input)
    source_errors = validate_customers(source_customers)
    if source_errors:
        print(
            json.dumps({"status": "FAILED", "source_errors": source_errors}, indent=2)
        )
        return 1

    customers, changes = apply_deterministic_updates(
        source_customers,
        seed=args.seed,
        effective_at=effective_at,
        attribute_updates=args.attribute_updates,
        soft_deletes=args.soft_deletes,
        inserts=args.inserts,
    )
    expected_count = len(source_customers) + args.inserts
    errors = validate_customers(
        customers,
        expected_count=expected_count,
        snapshot_at=effective_at,
    )
    if errors:
        print(json.dumps({"status": "FAILED", "errors": errors}, indent=2))
        return 1

    write_customer_csv(args.output, customers)
    write_change_csv(args.changes_output, changes)
    change_counts = dict(
        sorted(Counter(change.operation for change in changes).items())
    )
    manifest = build_manifest(
        customers=customers,
        snapshot_path=args.output,
        snapshot_id=args.snapshot_id,
        snapshot_at=effective_at,
        generator_seed=args.seed,
        extra={
            "source_file": args.input.name,
            "source_sha256": sha256_file(args.input),
            "change_file": args.changes_output.name,
            "change_sha256": sha256_file(args.changes_output),
            "change_counts": change_counts,
        },
    )
    write_manifest(args.manifest, manifest)

    result: dict[str, object] = {
        "status": "PASS",
        "snapshot": str(args.output),
        "manifest": str(args.manifest),
        "row_count": len(customers),
        "change_counts": change_counts,
        "sha256": manifest["sha256"],
    }
    if args.load_postgres:
        load_dotenv(args.env_file, override=False)
        result["postgres"] = load_customer_snapshot(
            customers, os.environ, apply_ddl=False
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
