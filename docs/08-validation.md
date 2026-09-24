# Step 8 — validation and quarantine action guide

Local implementation is complete. Spark/Delta and live Registry execution remain
to be validated in Databricks. No cloud resources or Git state were modified.

## Direction

Bronze raw deliveries → Avro decode and contract validation → one classified Delta
table → `validated_deliveries` and `quarantine_events` views.

Each output retains `raw_record_id`, transport metadata, rule version and processing
time. Quarantine retains raw bytes plus the full array of applicable business
errors. Structural failures stop semantic validation when the payload cannot be
interpreted safely. Invalid framing, truncated Avro, unknown schema IDs,
unsupported writer schemas and trailing bytes have distinct error codes.

The physical table is `silver.classified_deliveries`. The two named views are
logical outputs backed by the same atomic commit, not separate physical tables.
The valid view exposes typed V1 event fields; the quarantine view preserves the
decoded JSON where available. This avoids partially writing one of two routes.
Later streaming jobs may read the physical Delta table and filter `route='valid'`.

## What to do in Databricks

1. Sync the repository and open `notebooks/08_validate_deliveries.py`. Use a
   compatible classic Databricks Runtime with fastavro 1.12.2 and requests 2.34.2
   installed on driver/workers. Keep `pipelines` and `scripts` packages importable
   on workers. The local virtual environment is not copied into Databricks.
2. Point `source` at the successful Step 7 Bronze table. Create/use a writable
   Silver schema. Grant read access to Bronze, table/view creation in Silver,
   and access to the new S02 checkpoint. Do not reuse the S01 checkpoint.
3. Configure Registry HTTPS URL and secret names. These are Registry credentials,
   not Kafka credentials. Step 8 reads Delta, so it does not consume Kafka directly.
4. Run AvailableNow. Inspect the classified table and both views. Compare exact
   `raw_record_id` sets within the processed Bronze boundary: valid + quarantined
   must cover every input delivery once, and neither view may overlap the other.
5. Use a separate test schema/source for negative cases: truncated Avro, invalid
   business fields, multiple errors, tombstones and unknown IDs. Step 6 corruption
   scenarios can feed a separate Step 7 test stream. Keep production acceptance
   evidence untouched. Confirm late events and missing customers remain valid.
6. Rerun with unchanged source, checkpoint and app ID; output counts must not grow.
   Then add input and confirm only new deliveries appear. Save counts, missing/
   duplicate key checks, error-code distributions and query progress in a Volume.

After these checks pass, record cloud acceptance and proceed to Step 9 watermark
and event-ID deduplication. Repeated event IDs at different Kafka offsets are
deliberately retained here. No customer join occurs in Step 8.

## Operational semantics

- Registry lookup happens on the driver once per distinct schema ID; successful
  responses are cached. Credentials never enter the worker decoding closure.
- Network errors, authentication failures, rate limiting and unexpected Registry
  responses fail the batch for retry. Only explicit Registry error 40403 means
  an unknown schema ID. It is not negatively cached across batches.
- At present only the exact checked-in V1 writer schema is accepted. Even a
  compatible new version is quarantined as unsupported until explicitly supported
  (Step 24). Schema references are not resolved in this V1 implementation.
- At most 100 distinct header IDs per microbatch are resolved. Exceeding this
  guard stops the batch for investigation; no records are silently discarded.
- `txnAppId` and `txnVersion=batchId` protect repeated writes of the same batch.
  Keep the app ID stable for this checkpoint and unique to this pipeline. Never
  reset a checkpoint while keeping its app ID: batches could be skipped. A new
  checkpoint/app ID replay into an existing table can duplicate records; use an
  isolated target for deliberate replay. Rule changes require a reviewed versioned
  pipeline/replay plan. Only one writer should own each output schema's views.
- View creation is retryable after a committed batch, but the two view DDL commands
  themselves are not atomic. The underlying routed rows are committed together.
- Idempotency is scoped to the same checkpoint/batch, not arbitrary replay across
  checkpoints. Abrupt cloud failure recovery remains unverified locally.

## Local verification

Run `python -m pytest -q`. Tests cover Avro round trips, multiple business errors,
future timestamps, corrupt framing, unsupported and unknown schemas, Registry
outages, and preservation of late, duplicate and missing-customer events. Offline
tests do not assert that real Spark UDF execution, Delta commits or restart
recovery passed; those are the cloud checks above.

Reference: [Databricks idempotent foreachBatch writes](https://docs.databricks.com/aws/en/structured-streaming/delta-lake#use-foreachbatch-for-idempotent-table-writes).
