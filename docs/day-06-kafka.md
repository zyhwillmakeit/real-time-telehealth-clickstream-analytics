# Day 6: Avro delivery and fault scenarios

Local implementation is ready. Live Schema Registry registration and Avro delivery
remain pending manual execution. The earlier JSON smoke test does not prove Avro
or Registry connectivity. Do not mark the full Day 6 gate complete yet.

## Offline reproduction

From the repository root, with the virtual environment active:

```bash
python -m scripts.generate_journeys
python -m scripts.publish_events --output-dir data/day-06-preview
python -m scripts.publish_events --duplicate-rate 0.1 --delay-rate 0.1 --reorder-rate 0.2 --corrupt-rate 0.02 --missing-rate 0.05 --output-dir data/day-06-fault-preview
python -m pytest -q
```

Without `--send`, the command does not read `.env` or access the network.
Every output directory must be new, preserving evidence for each attempt.
`plan.jsonl` records each intended delivery, payload, flags and simulated send time.
`manifest.json` records input checksums, rates, expected unique valid events and
corrupt delivery counts. The offline encoding check uses a dummy schema ID which
is never used by the live sender.

## Manual connection

1. Create a Kafka topic `telehealth-clickstream` in the connected cluster. Keep the
   JSON smoke topic separate. The producer disables automatic topic creation.
2. Enable Schema Registry in the relevant Confluent environment. Add its endpoint
   and its own API credentials to the existing root `.env`:

```dotenv
SCHEMA_REGISTRY_URL=https://YOUR-REGISTRY-ENDPOINT
SCHEMA_REGISTRY_API_KEY=YOUR-REGISTRY-KEY
SCHEMA_REGISTRY_API_SECRET=YOUR-REGISTRY-SECRET
KAFKA_TOPIC=telehealth-clickstream
```

Keep existing Kafka credentials. Kafka keys and Registry keys serve different
resources. The Kafka principal needs metadata/read-description and write access;
the Registry principal needs schema registration and subject configuration access.

3. Send a small clean sample:

```bash
python -m scripts.publish_events --send --limit 20 --rate 10 --output-dir data/day-06-live-01
```

This checks topic metadata, configures only `<topic>-value` to
`BACKWARD_TRANSITIVE`, registers the V1 schema, and sends framed Avro bytes using
the returned schema ID. Keys are UTF-8 journey IDs with appointment, customer,
anonymous and session fallbacks. The wire value is magic byte 0, a four-byte
big-endian schema ID, then an Avro datum. No Avro container-file header is used.

`manifest.json` must say `PASS`, with `acknowledged=20`.
`delivery-report.json` contains an acknowledgement and Kafka partition/offset for
each delivery. Queue exhaustion, callback failure or flush timeout produces FAILED.
A failed report may have partial successes; inspect it before rerunning. Retries
across separate invocations can redeliver events. Downstream event-ID deduplication
is still required despite producer transport idempotence.

4. In Databricks, reuse the working Kafka reader against the new topic. First
inspect `topic`, `partition`, `offset` and `length(value)`. The value is binary
Avro and must not be interpreted with `CAST(value AS STRING)` or `from_json`.
Read the first five bytes as the framing header and decode the remaining bytes
with the schema identified by the header (or use Registry-aware Avro decoding).
Confirm one decoded event ID against `delivery-report.json`.

Only after registration, acknowledgements and decoded event verification should
the live gate be marked complete. Full Bronze ingestion/checkpoints are Day 7.

## Fault semantics

| Option | Meaning |
|---|---|
| `--duplicate-rate` | Chance of an additional identical payload with the same event ID; broker retries remain independently idempotent |
| `--delay-rate` | Chance to shift collection time by `--delay-seconds` (default 2100 seconds) |
| `--reorder-rate` | Chance of adding up to 300 seconds of arrival jitter; actual inversion depends on nearby events |
| `--corrupt-rate` | Chance to send a header with a truncated Avro body; these cannot be counted as valid events |
| `--missing-rate` | Fraction sampled at customer level; remap all that customer's authenticated events to one ID absent from the supplied customer snapshot |

Faults can overlap; flag counts are not disjoint. A duplicate of a corrupt event
is also corrupt. Missing customers remain valid events and should survive a left
join with unmatched status. The Day 5 ledger describes the original clean data;
use the Day 6 plan for mutated payloads, losses and reconciliation expectations.

For corruption or missing-customer live runs, create a dedicated test topic and
pass it explicitly with `--topic telehealth-clickstream-faults`. Broker-side schema
validation, if enabled, may reject corrupt messages; report that as a delivery
failure rather than claiming quarantine coverage.

Events are sent in simulated arrival order at `--rate` records/second, not at
their historical wall-clock intervals. Stored timestamps remain simulated.
This is a compressed historical replay for functional correctness, not a live
latency benchmark. No message timestamp override is set. A simulated delay alone
does not prove a watermark drop; that requires later streaming tests.

References: [Registry API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html),
[Confluent serialization](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html).
