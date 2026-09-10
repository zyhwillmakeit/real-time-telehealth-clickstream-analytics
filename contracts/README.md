# Data Contracts

This directory contains the versioned source contracts that producers, streaming validation, tests, and downstream models share.

## Day 3 deliverables

| Artifact | Purpose |
|---|---|
| [`events/clickstream_event_v1.avsc`](events/clickstream_event_v1.avsc) | Avro wire schema for every clickstream event |
| [`events/event_rules_v1.json`](events/event_rules_v1.json) | Allowed values, identifier formats, and event-specific required fields |
| [`events/examples/valid`](events/examples/valid) | Human-readable valid examples used by contract tests |
| [`events/examples/invalid`](events/examples/invalid) | Invalid examples with expected error codes |
| [`sql/customer_master.sql`](sql/customer_master.sql) | PostgreSQL customer-master source DDL |
| [`../docs/data-contract.md`](../docs/data-contract.md) | Event dictionary, time semantics, IDs, compatibility, and ownership |

Metric formulas remain in [`docs/metrics.md`](../docs/metrics.md). The event contract defines what a source event means; metric contracts define how trusted events become analytical measures.

## Validate locally

The checked-in JSON examples use ISO-8601 timestamps for readability. The validator converts them to Avro `timestamp-micros` logical values before structural validation.

```bash
.venv/bin/python scripts/validate_contracts.py
.venv/bin/python -m pytest -q
```

Day 8 streaming validation should call the same business validator instead of maintaining a second rule set.
