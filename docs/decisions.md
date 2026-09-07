# Architecture Decision Log

## ADR-001 — Streaming clickstream and batch customer snapshots

**Status:** Accepted on Day 1

Clickstream is continuous, high-frequency product telemetry with a near-real-time analytical requirement. Customer master data is small, slower-changing operational data with a one-hour freshness objective. The platform therefore streams click events and ingests full customer snapshots every 30 minutes. It does not query PostgreSQL for each event.

## ADR-002 — Raw delivery preservation before parsing

**Status:** Accepted on Day 1

Bronze stores the original delivery and Kafka metadata before schema or business validation. This preserves replay and audit evidence for malformed, unknown-schema, and late records.

## ADR-003 — Watermark deduplication plus durable event-key protection

**Status:** Accepted on Day 1

A 30-minute event-time watermark bounds streaming state and removes retries within the expected interval. The durable event table additionally uses an insert-only merge on `event_id` so a replay beyond the state window does not create duplicate business events.

## ADR-004 — Current customer dimension with recorded snapshot semantics

**Status:** Accepted on Day 1

V1 maintains a Type 1 current dimension and records the snapshot used for every enrichment. Historical matched events do not automatically change when customer attributes change. Missing customers may be repaired explicitly. The project does not claim exact event-time historical customer state.

## ADR-005 — Separate journey and appointment facts

**Status:** Accepted on Day 1

Booking conversion uses `booking_journey_id`; visit outcomes use `appointment_id`. This prevents cross-day appointment behavior from being divided by unrelated same-day booking counts and allows an explicit outcome observation window.

## ADR-006 — Daily reconciliation in addition to the low-latency path

**Status:** Accepted on Day 1

Watermarks intentionally trade completeness after a threshold for bounded streaming state. A scheduled reconciliation scans by ingestion time, finds valid events absent from durable Silver, repairs affected entities, and rebuilds their metrics.

## ADR-007 — dbt owns business semantics

**Status:** Accepted on Day 1

Spark owns parsing, event reliability, deduplication, quarantine, and customer enrichment. dbt owns journey facts, appointment maturity, metric formulas, analytical marts, tests, and documentation. This keeps changing business definitions out of the streaming reliability layer.

