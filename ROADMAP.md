# 30-Step Delivery Roadmap

Assumption: one developer, 4–5 hours per day. Each step ends with a reviewable output.

## Week 1 — Contracts and ingestion

- [x] **Step 1:** Define scope, business questions, metric contracts, architecture, acceptance objectives, repository layout, and this delivery plan.
- [ ] **Step 2:** Verify Kafka, Schema Registry, PostgreSQL, Databricks, object storage, and SQL Warehouse connectivity; lock the first tested dependency set.
- [x] **Step 3:** Define Avro event schema V1, field-level and event-level validation rules, timestamp conventions, journey identifiers, and customer DDL.
- [x] **Step 4:** Build deterministic customer seed/update scripts and the first complete customer extract; validate approximately 10,000 unique customers.
- [x] **Step 5:** Build the causal journey simulator with sessions, booking attempts, appointments, delayed virtual visits, and an expected-results manifest.
- [ ] **Step 6:** Connect the simulator to Kafka and Schema Registry; add configurable retry, delay, out-of-order, corrupt, and missing-customer scenarios.
  Local producer, Avro framing and fault planning implemented and tested; live Registry registration and Avro readback pending. See [Step 6](docs/day-06-kafka.md).
- [x] **Step 7:** Implement Kafka-to-Bronze raw ingestion with persistent checkpointing; prove stop/restart continuity.
  User-confirmed normal-stop/restart acceptance recorded 2026-09-23: all 40 reported offsets matched, zero missing/duplicate raw keys, B=30 rows and C=50 rows. Query ID persisted; run ID changed. See [acceptance evidence](evidence/day-07/README.md) and [runbook](docs/07-bronze.md). Abnormal process termination remains Step 23.

## Week 2 — Trusted Silver data

- [ ] **Step 8:** Implement parsing, structural and business validation, validated delivery output, and multi-reason quarantine records.
  Local implementation and offline tests complete; Databricks UDF/Delta and live routing acceptance pending. See [action guide](docs/08-validation.md).
- [ ] **Step 9:** Implement the 30-minute watermark, event-ID deduplication, and durable insert-only event merge.
- [ ] **Step 10:** Test controlled arrival order around the watermark and create the first reconciliation comparison.
- [ ] **Step 11:** Implement complete customer snapshot validation, Type 1 merge, tombstones, and 30-minute scheduling.
- [ ] **Step 12:** Implement versioned left enrichment with anonymous and `customer_not_found` outcomes.
- [ ] **Step 13:** Implement missing-customer repair and test idempotent recovery after partial multi-table writes.
- [ ] **Step 14:** Reconcile a deterministic fixture end to end and publish the trusted-Silver milestone evidence.

## Week 3 — Facts, marts, and dashboards

- [ ] **Step 15:** Initialize dbt sources, staging models, documentation, and core source/key tests on the SQL Warehouse.
- [ ] **Step 16:** Build `fct_booking_journey` with ordered stages, deduplicated steps, attribution, and instrumentation completeness.
- [ ] **Step 17:** Build `fct_appointment` with cross-session visit events, observation deadline, and maturity state.
- [ ] **Step 18:** Build the three business marts; verify filtered distinct counts and numerator/denominator rollups.
- [ ] **Step 19:** Implement affected-ID incremental rebuilds, safe cursor advancement, maturity updates, and a non-overlapping two-minute analytics job.
- [ ] **Step 20:** Build the journey analytics dashboard with KPIs, funnel, appointment outcomes, trends, filters, and data timestamps.
- [ ] **Step 21:** Build the platform-health dashboard with throughput, latency, freshness, backlog, quality, job, and reconciliation indicators.

## Week 4 — Recovery and production evidence

- [ ] **Step 22:** Complete ingestion-time reconciliation, late repair, customer repair, affected-model rebuild, and parameterized replay.
- [ ] **Step 23:** Test process termination, dependency outage, and partial write failure; document recovery time and final reconciliation.
- [ ] **Step 24:** Publish backward-compatible schema V2 and verify old/new coexistence and incompatible-change rejection.
- [ ] **Step 25:** Run a 100 events/s 60-minute sustained test, a 500 events/s 5-minute burst, and customer-freshness tests; record cost.
- [ ] **Step 26:** Optimize only measured bottlenecks and publish before/after evidence; report unmet targets honestly.
- [ ] **Step 27:** Add CI, configuration templates, access checks, secret scanning, shutdown instructions, and restart rehearsal.

## Final delivery

- [ ] **Step 28:** Finish runbook, data dictionary, dbt docs, decision log, and clean-room reproduction review.
- [ ] **Step 29:** Fix acceptance gaps and record a 5–8 minute demonstration of normal flow, quarantine, reconciliation, and recovery.
- [ ] **Step 30:** Tag the release, publish the evidence index and final acceptance table, update resume claims from measured results, and stop unused paid resources.

## Milestone gates

| Gate | Required result |
|---|---|
| Step 7 — passed (user-confirmed) | Kafka deliveries reach Bronze and survive normal stop/restart with the same checkpoint; 40 report offsets matched without missing or duplicate raw keys |
| Step 14 | Deterministic raw input reconciles to unique Silver events and explainable quarantine output |
| Step 21 | Business and platform-health dashboards form an end-to-end demonstrable loop |
| Step 28 | A reviewer can reproduce, operate, fail, recover, and inspect the platform from the documentation |

If a gate slips, reduce chart count, visual polish, additional aggregate tables, and deployment automation before removing contracts, deduplication, quarantine, late-data repair, cross-day metric semantics, or recovery testing.
