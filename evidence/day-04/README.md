# Day 4 Customer Snapshot Evidence

The V1 extract is deterministic acceptance evidence for the customer-data generator.

| Artifact | Purpose |
|---|---|
| `customer_snapshot_v1.csv` | Complete synthetic 10,000-customer source extract |
| `customer_snapshot_v1.manifest.json` | Contract version, seed, timestamp, checksum, row counts, and distributions |

Run `.venv/bin/python -m scripts.seed_customers` from the repository root to regenerate both files. The test suite checks the full extract against the V1 contract and verifies its SHA-256 checksum.

This reviewed fixture contains only stable synthetic identifiers and analytical categories. It contains no direct identifiers, contact data, clinical data, or free text.
