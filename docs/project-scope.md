# Project Scope

## Problem statement

Telehealth product teams need near-real-time visibility into the path from provider discovery to appointment booking and virtual-visit completion. Clickstream events are continuous and imperfect: clients retry, devices go offline, records arrive out of order, and producers evolve. Customer attributes change more slowly and are maintained in an operational database.

This project designs a production-oriented analytical platform that combines these two data patterns without querying the operational database for every event. Its core engineering question is:

> How can unreliable behavioral events and periodically refreshed customer attributes become trustworthy, explainable, and recoverable analytics data?

## Consumers

### Product and operations

Product managers, growth analysts, and telehealth operations analysts need booking funnel, visit outcome, customer segment, acquisition channel, specialty, and device performance.

### Data engineering

Data engineers need event throughput, source and analytical freshness, processing latency, invalid and duplicate rates, customer-match rate, streaming backlog, checkpoint recovery, and reconciliation status.

## V1 scope

- A stateful Python simulator for web and mobile booking journeys
- One versioned Kafka business-event topic backed by Schema Registry
- Raw event preservation in Delta Bronze
- Validation, quarantine, event-time watermarking, and durable deduplication
- Scheduled full snapshots of a small PostgreSQL customer master table
- A current customer dimension and versioned enrichment semantics
- dbt booking-journey and appointment facts
- Three business marts that support all required filters
- Business analytics and platform-health dashboards
- Daily reconciliation, parameterized replay, and controlled failure recovery
- Dependency locking, configuration templates, automated tests, and evidence reports

## Explicit non-goals

- A complete telehealth application
- Real patient, clinical, diagnosis, payment, or protected health information
- EHR, FHIR, claims, payments, and provider scheduling integrations
- Appointment cancellation and rescheduling semantics
- Change data capture from PostgreSQL
- Stream-to-stream joins or multiple business-event topics
- Machine learning, Kubernetes, Airflow, Snowflake, or Redshift
- Multi-cloud deployment
- Exact historical customer attributes at event time

## Assumptions

- One developer works 4–5 hours per day for 30 days.
- The developer has working Python, SQL, and basic Spark knowledge.
- A Databricks workspace with Kafka access, persistent object storage, Jobs, and SQL Warehouse is available.
- PostgreSQL is reachable from Databricks; a database bound to a laptop's `localhost` is not sufficient for cloud validation.
- The V1 customer table contains about 10,000 synthetic records and is small enough for scheduled full snapshots.
- A successful appointment can have at most one simulated virtual visit.
- A booked appointment is scheduled 0–7 days after booking.
- Appointment outcomes are observed until 24 hours after the scheduled start.
- All analytical timestamps and cohorts use UTC.

## Business event path

```text
specialty_searched
  → provider_profile_viewed
  → appointment_slot_viewed
  → booking_started
  → appointment_booked
  → checkin_started
  → waiting_room_entered
  → video_visit_started
  → visit_completed
```

Branches include abandonment at each stage and `video_connection_failed` after entering the virtual-visit path.

## Acceptance dataset

The controlled acceptance run will contain approximately 1–3 million events, adjusted to available compute, and a generator manifest containing expected raw deliveries, unique valid events, invalid records, appointments, journey stages, and visit outcomes.

It must include normal events and separately controllable injections for:

- identical retries within and beyond the watermark window
- the same `event_id` with conflicting content
- malformed payloads and event-specific missing fields
- 5–25 minute late deliveries and 90-minute late deliveries
- out-of-order events
- missing and newly created customers
- timestamps more than five minutes in the future
- mixed schema V1 and backward-compatible V2 messages

An event can exhibit more than one quality condition. Quality percentages are therefore not assumed to sum to 100%.

## Definition of done

The project is complete when a reviewer can trace an event from Kafka delivery through Bronze, validation, deduplication, customer enrichment, facts, marts, and dashboard; observe invalid and duplicate behavior; recover a beyond-watermark valid event through reconciliation; restart a failed stream without double-counting; and reproduce the result from documented configuration without access to secret values.

