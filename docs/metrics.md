# Metric Contracts

Metric definitions are versioned business contracts. Gold models retain numerator and denominator columns so filtered rates are recomputed from counts instead of averaging percentages.

## Cohorts and observation windows

- Booking-funnel cohort date: UTC date of the journey's first valid `provider_profile_viewed` event.
- Appointment cohort date: UTC date of `appointment_booked`.
- Completed-visit volume date: UTC date of `visit_completed`.
- Appointment observation deadline: `scheduled_start_time + 24 hours`.
- An appointment enters outcome-rate denominators only after its observation deadline.
- Booked appointments inside the window are labeled `observing`, not incomplete.

## Business metrics

| Metric | Grain and formula | Key safeguards |
|---|---|---|
| Provider-view journeys | Distinct booking journeys with a valid provider view | Repeated page views count once |
| Booking conversion rate | Ordered journeys reaching `appointment_booked` ÷ journeys with a provider view in the same cohort | Requires valid stage ordering; does not compare unrelated calendar-day totals |
| Step conversion rate | Journeys reaching the next ordered stage ÷ journeys reaching the current stage | Uses one journey and one attribution record per funnel |
| Step drop-off rate | `1 - step conversion rate` | Returns NULL for a zero denominator |
| Booking-to-observed-completion rate | Mature appointments completed in the observation window ÷ mature booked appointments | Excludes appointments still under observation |
| Started-visit completion rate | Mature started appointments completed within 24 hours of start ÷ mature started appointments | Separates booking from visit-start performance |
| Completed visits | Distinct appointment IDs with a valid completion event in the selected event-time range | At most one simulated visit per appointment in V1 |
| Active customers | Distinct non-null customer IDs with qualifying behavior after all selected filters | Anonymous users are reported separately; daily distinct values are not added for longer periods |
| Incomplete instrumentation | Journey/appointment entities with a downstream event but a missing required upstream stage ÷ eligible entities | Missing events are measured, not invented |

### Funnel ordering

The primary booking funnel is:

```text
provider_profile_viewed
  → booking_started
  → appointment_booked
```

`appointment_slot_viewed` can be displayed as a diagnostic stage, but the primary headline funnel remains stable even if product instrumentation changes around slot browsing. A later stage counts only when its event time is at or after the preceding valid stage for the same journey.

### Attribution

- Booking funnel dimensions use the first valid provider-view event in the journey.
- Appointment outcome dimensions use the appointment-booked event.
- Customer dimensions use the snapshot recorded on that attribution event.
- This keeps numerator and denominator in the same specialty, device, membership, channel, and region segment when later events use a different device or customer attributes change.

## Platform metrics

| Metric | Definition |
|---|---|
| Input throughput | Kafka deliveries ingested per closed five-minute ingestion window |
| Source delivery latency | `received_at - event_time`, reported with percentiles and negative values isolated |
| Silver processing latency | `processed_at - received_at` for valid events |
| Gold availability latency | Successful Gold publish time minus `received_at` for normal valid events |
| Event freshness | Current time minus latest valid event time, interpreted alongside an input heartbeat |
| Customer freshness | Current time minus the latest published customer snapshot extraction time |
| Duplicate delivery rate | Deliveries whose event ID has already appeared ÷ all parseable deliveries in the same closed ingestion range |
| Invalid event rate | Quarantined deliveries ÷ all deliveries in the same closed ingestion range |
| Late-delivery rate | Valid deliveries exceeding the project delivery-latency threshold ÷ valid deliveries |
| Watermark drop count | Engine-reported rows dropped by watermark, kept separate from the business late-delivery rate |
| Customer-not-found rate | Valid events with a non-null customer ID and no customer match ÷ valid events with a non-null customer ID |
| Reconciliation gap | Expected valid unique events from the manifest minus durable unique events at a fixed input boundary |
| Recovery time | Failure injection timestamp to successful catch-up at the agreed input boundary |

Duplicate, invalid, late, and customer-missing conditions can overlap and are not expected to add to 100%.

## Initial targets

| Target | Measurement contract |
|---|---|
| Gold availability p95 < 5 minutes | Warm-compute, 60-minute sustained run; measure `received_at` to successful Gold publish and report exclusions separately |
| Customer freshness p95 < 60 minutes | Make timestamped source updates and trace each to the published dimension |
| 30-minute real-time lateness tolerance | Controlled event-time and arrival-order tests on both sides of the threshold |
| Zero duplicate downstream event IDs | Acceptance data after retry, restart, and replay scenarios |
| 100% invalid-record traceability | Each injected invalid delivery maps to raw evidence and quarantine error codes |
| Recovery and catch-up < 10 minutes | Controlled stream interruption with final count and offset reconciliation |
| Eventual completeness | Post-reconciliation unique events and entity results match the fixed generator manifest |

The targets become achievements only after the evidence report records load, duration, compute specification, p50/p95, failures, backlog, and cost.

