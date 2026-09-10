"""Generate the deterministic V1 customer seed snapshot and optionally load it."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from generator.customer_data import (
    build_manifest,
    generate_customers,
    parse_utc_timestamp,
    validate_customers,
    write_customer_csv,
    write_manifest,
)
from generator.customer_postgres import load_customer_snapshot

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_ROOT / "evidence" / "day-04" / "customer_snapshot_v1.csv"
DEFAULT_MANIFEST = (
    PROJECT_ROOT / "evidence" / "day-04" / "customer_snapshot_v1.manifest.json"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=20_260_910)
    parser.add_argument("--snapshot-id", default="customer_snapshot_v1")
    parser.add_argument("--snapshot-at", default="2026-09-10T23:59:59Z")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--load-postgres", action="store_true")
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshot_at = parse_utc_timestamp(args.snapshot_at)
    customers = generate_customers(
        count=args.count,
        seed=args.seed,
        snapshot_at=snapshot_at,
    )
    errors = validate_customers(
        customers,
        expected_count=args.count,
        snapshot_at=snapshot_at,
    )
    if errors:
        print(json.dumps({"status": "FAILED", "errors": errors}, indent=2))
        return 1

    write_customer_csv(args.output, customers)
    manifest = build_manifest(
        customers=customers,
        snapshot_path=args.output,
        snapshot_id=args.snapshot_id,
        snapshot_at=snapshot_at,
        generator_seed=args.seed,
    )
    write_manifest(args.manifest, manifest)

    result: dict[str, object] = {
        "status": "PASS",
        "snapshot": str(args.output),
        "manifest": str(args.manifest),
        "row_count": len(customers),
        "sha256": manifest["sha256"],
    }
    if args.load_postgres:
        load_dotenv(args.env_file, override=False)
        result["postgres"] = load_customer_snapshot(
            customers, os.environ, apply_ddl=True
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
