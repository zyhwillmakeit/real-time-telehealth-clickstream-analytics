# Day 5: Causal journey simulation

Run from the repository root:

```bash
.venv/bin/python -m scripts.generate_journeys
.venv/bin/python -m pytest -q
```

The default run reads the Day 4 customer snapshot and simulates 700 booking
attempts, starting at 2026-09-12 00:00 UTC, one per minute. A fixed seed controls
customer selection, device, specialty, provider and appointment delay. Only
active, non-deleted customers created before simulation start are eligible.

Seven scenarios cycle: abandonment after profile, after slot browsing, or after
booking start; completed visit; video connection failure; no-show; started but
unfinished visit. These are acceptance scenarios, not measured population rates.
Customers may make multiple attempts, each with its own journey and session.
Pre-login events retain an anonymous ID; authenticated booking events use the
same customer keys as the batch source.

Booked appointments are scheduled 1–3 days later. Visit events retain appointment,
journey, provider, specialty and customer identity but use a new session. Normal
collection delay is two seconds. Events are sorted by collection time. Day 6
will introduce delivery retries, lateness and corruption separately.

Outputs in ignored `data/day-05/`:

- `events.jsonl`: V1-contract events, validated before writing. JSONL is a local
  representation; it is not Schema Registry wire framing.
- `manifest.json`: scenario ledger per journey and appointment, expected event
  counts, ordered funnel counts, source/output checksums and observation boundary.

The ledger is planned from scenarios before event serialization, providing a
reference for downstream reconciliation. It contains no customer-enrichment
claims; snapshot-at-processing attribution belongs to the later join pipeline.

The default observation boundary is 2026-09-15 12:00 UTC. Future events are
excluded. Appointments are mature only at scheduled time plus 24 hours, including
already completed appointments; otherwise they remain `observing`. This follows
the [metric contract](metrics.md). Extending `--as-of` reveals later events without
changing earlier event IDs or payloads. Use `--count`, `--seed`, `--start-at`,
`--as-of`, `--customers`, and `--output-dir` to reproduce other scenarios. Keep all
booking attempts before the observation boundary.

No broker or database is accessed. User-reported Kafka-to-Databricks smoke-test
success is separate from the full Day 2 multi-service gate, which remains pending.
