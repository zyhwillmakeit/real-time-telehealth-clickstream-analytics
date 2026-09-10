# Clickstream and Customer Data Contract V1

## Ownership and format

The simulated product client owns business-event meaning and identifier continuity. The collection boundary assigns `received_at` and publishes Avro records to the `telehealth-clickstream` topic. Schema Registry owns structural compatibility; the versioned rule matrix owns field-level and event-level business validity.

The wire format is Avro `com.telehealth.analytics.clickstream.v1.TelehealthClickstreamEvent`. All examples are synthetic and exclude clinical, diagnosis, payment, message-content, address, or protected health information.

## Event dictionary

| Event | Meaning | Required business identifiers |
|---|---|---|
| `specialty_searched` | User submits a specialty search | session; anonymous or customer identity |
| `provider_profile_viewed` | User opens a provider profile and starts an attributable booking journey | session, booking journey, provider |
| `appointment_slot_viewed` | User views an available appointment slot | session, booking journey, provider |
| `booking_started` | Authenticated user begins booking | customer, session, booking journey, provider |
| `appointment_booked` | Booking succeeds and creates an appointment | customer, session, booking journey, appointment, provider, scheduled start |
| `checkin_started` | Customer starts pre-visit check-in | customer, booking journey, appointment, provider, scheduled start |
| `waiting_room_entered` | Customer enters the virtual waiting room | customer, booking journey, appointment, provider, scheduled start |
| `video_visit_started` | Product observes the virtual visit start | customer, booking journey, appointment, provider, scheduled start |
| `video_connection_failed` | Product observes a video connection failure | customer, booking journey, appointment, provider, scheduled start |
| `visit_completed` | Product observes a completed virtual visit | customer, booking journey, appointment, provider, scheduled start |

Every event also requires `event_id`, `event_name`, `event_time`, `received_at`, `session_id`, `device_type`, `platform`, and `schema_version`. `specialty` is required by all V1 event types. Event-specific requirements are machine-readable in `contracts/events/event_rules_v1.json`.

## Field dictionary

| Field | Avro type | Meaning and rule |
|---|---|---|
| `event_id` | string | Immutable business event ID; identical client retries reuse the ID and business payload |
| `event_name` | enum | One of the ten contracted product interactions |
| `event_time` | timestamp-micros | UTC time the user action happened; analytical ordering and watermark column |
| `received_at` | timestamp-micros | UTC time the collection boundary accepted the event for Kafka delivery |
| `anonymous_id` | null/string | Stable pre-login browser/app identity; at least this or customer_id is required |
| `customer_id` | null/string | Synthetic customer-master key; required from booking start onward |
| `session_id` | string | One contiguous product visit; a later virtual visit normally has a new session |
| `booking_journey_id` | null/string | One booking attempt, beginning at the attributable provider view |
| `appointment_id` | null/string | One successful appointment; connects booking and later visit sessions |
| `scheduled_start_time` | null/timestamp-micros | Planned appointment time and basis of the outcome observation deadline |
| `provider_id` | null/string | Synthetic selected provider ID |
| `device_type` | string | `desktop`, `mobile`, or `tablet` |
| `platform` | string | `web`, `ios`, or `android`; must be compatible with device type |
| `specialty` | null/string | One of the four contracted specialty values; business-required for every V1 event |
| `schema_version` | int | Payload contract version; exactly 1 for this schema |

## Identifier conventions

| Identifier | Format | Lifecycle |
|---|---|---|
| event | `evt_` + 32 lowercase hexadecimal characters | New for each business action; unchanged on retry |
| anonymous | `anon_` + 32 lowercase hexadecimal characters | Stable until simulated browser/app identity reset |
| customer | `cus_` + eight digits | Stable PostgreSQL primary key |
| session | `ses_` + 32 lowercase hexadecimal characters | New after the configured inactivity boundary or new app visit |
| booking journey | `bkj_` + 32 lowercase hexadecimal characters | New for each booking attempt, including a retry after abandonment |
| appointment | `apt_` + 32 lowercase hexadecimal characters | Created only when booking succeeds |
| provider | `prv_` + six digits | Stable synthetic provider reference |
| raw delivery | `topic:partition:offset` | Assigned in Bronze; identifies delivery, not business uniqueness |

Kafka partition keys prefer `booking_journey_id`; appointment-only traffic uses `appointment_id`. Ordering is guaranteed only within a Kafka partition. Analytical order is reconstructed from identifiers and `event_time`.

## Time conventions

- All timestamps are UTC with microsecond precision. Human-readable fixtures use ISO-8601 with `Z`; Avro serializes them as `timestamp-micros`.
- `event_time` records business occurrence and is immutable on retry.
- `received_at` records collection, not Spark processing. A delayed/retried event keeps its original business time and receives the collection time appropriate to the simulated delivery attempt.
- `ingested_at` is added by the Bronze ingestion job and is absent from the producer contract.
- `processed_at` is added by validation/curation and is absent from the producer contract.
- Kafka record timestamp and offsets remain transport metadata and do not replace business timestamps.
- Events more than five minutes ahead of `received_at` fail with `EVENT_TIME_TOO_FAR_IN_FUTURE` before watermarking.
- For `appointment_booked`, `scheduled_start_time` cannot precede `event_time`.
- Negative or unexpectedly large delivery latency remains observable; late-but-valid records are not structurally invalid.

## Validation order and error codes

Validation fails closed in this order:

1. Decode and validate the Avro record and reject unknown fields.
2. Parse timestamps and require UTC.
3. Check global and event-specific required fields.
4. Validate allowed values and device/platform compatibility.
5. Validate identifier formats and schema version.
6. Apply temporal rules.

| Error code | Meaning |
|---|---|
| `AVRO_SCHEMA_INVALID` | Record cannot satisfy the structural V1 schema or contains unknown fields |
| `TIMESTAMP_INVALID:<field>` | Timestamp is not a valid UTC value |
| `FIELD_REQUIRED:<field>` | A common or event-specific field is missing or blank |
| `IDENTITY_REQUIRED` | Both anonymous_id and customer_id are absent |
| `VALUE_NOT_ALLOWED:<field>` | A controlled field contains an unknown value |
| `DEVICE_PLATFORM_MISMATCH` | Platform is inconsistent with the stated device type |
| `IDENTIFIER_FORMAT_INVALID:<field>` | An identifier does not follow its prefix and character contract |
| `SCHEMA_VERSION_UNSUPPORTED` | Payload version is not V1 |
| `EVENT_TIME_TOO_FAR_IN_FUTURE` | Event time is more than five minutes beyond collection time |
| `SCHEDULED_START_BEFORE_BOOKING` | A booking claims a scheduled time before the booking event |

A record may have multiple business errors. Quarantine should preserve the complete error-code array, contract version, and raw delivery reference.

## Journey invariants

Individual event validation does not invent missing history. Downstream entity models separately check:

- funnel stages are counted only in valid time order for one booking journey;
- one booking journey produces at most one successful appointment in V1;
- one appointment belongs to exactly one customer, provider, specialty, and booking journey;
- visit events cannot be treated as proof that missing upstream instrumentation occurred;
- one appointment produces at most one completed visit in V1.

Violations become instrumentation or entity-integrity metrics rather than silently rewritten events.

## Customer master contract

`source.customer_master` contains one current row per synthetic customer. The primary key uses the same `cus_########` pattern as clickstream. Controlled values are lowercase snake case:

- regions: `midwest`, `northeast`, `south`, `west`
- membership plans: `basic`, `premium`
- acquisition channels: `employer`, `organic`, `paid_search`, `referral`
- account statuses: `active`, `closed`, `suspended`

`updated_at` supports snapshot freshness and audit. `is_deleted` is a logical deletion marker; it prevents absence from an incomplete snapshot from being mistaken for a deletion. The source DDL contains no clinical attributes.

## Schema evolution policy

The Schema Registry subject will use `BACKWARD_TRANSITIVE` compatibility. V1 field names, meanings, and identifier semantics are stable. A future additive field must be nullable or have an Avro default. Producers do not emit a new schema version until the compatibility test passes and consumers capable of reading it are deployed. V2 implementation and mixed-version evidence remain a Day 24 task.

