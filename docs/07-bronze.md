# 7. Kafka to raw Bronze and restart acceptance

## Status and boundaries

Local implementation and offline tests are complete. Step 6 live Kafka/Avro validation has also been completed, including Schema Registry registration, producer acknowledgements, Databricks Avro decoding, and event-ID reconciliation against the delivery report.

Step 7 Databricks execution, Delta commits, and checkpoint continuity have not yet been fully verified. The Day 7 gate remains pending until the cloud validation steps below pass.

S01 saves every Kafka delivery with unchanged binary key/value and headers,
topic, partition, offset, Kafka timestamp/type, ingestion timestamp and
`raw_record_id=topic:partition:offset`. A best-effort `schema_id` is extracted from
the Confluent header only when magic byte 0 and five bytes are present. This is
not validation: even a corrupt body is preserved. Null values (Kafka tombstones),
non-Avro values and unknown schema IDs are retained. Step 8 owns decoding/quarantine.

The sink is append-only Delta with a persistent checkpoint. There is no business
event deduplication, watermark, foreachBatch merge or data-loss suppression.
Sending the same event again creates a new Kafka offset and a new Bronze row;
restarting the same query must not append a second row for the same offset.

## Prepare Databricks

1. Sync/import this repository into a Databricks Git folder, preserving `pipelines/`
   and `notebooks/` as siblings. Git actions remain user-managed.
2. Use the compute already tested against Kafka, with Spark/Kafka/Delta support.
   `available_now` is the default. On serverless, use a supported job execution;
   continuous `processing_time` requires compatible classic compute. Record the
   actual Runtime version and compute type with your evidence.
3. Use an existing writable catalog. Create a Bronze schema and a checkpoint
   Volume (or use an existing durable cloud-storage location). For example,
   substituting your actual catalog:

```sql
CREATE SCHEMA IF NOT EXISTS telehealth_dev.bronze;
CREATE SCHEMA IF NOT EXISTS telehealth_dev.ops;
CREATE VOLUME IF NOT EXISTS telehealth_dev.ops.stream_checkpoints;
```

The execution identity needs catalog/schema USE, CREATE TABLE and target
SELECT/MODIFY privileges, plus READ/WRITE VOLUME or equivalent storage access.
Do not use local `/tmp` or a newly randomized directory as the checkpoint.

4. Put the Kafka reader credentials in a Databricks secret scope. Local `.env`
   is not uploaded or read by this notebook. Grant the Kafka identity read and
   describe access to the topic. Schema Registry credentials are not needed by S01.
5. Open `notebooks/day07_bronze.py`. Configure widgets:

| Widget | Value |
|---|---|
| bootstrap_servers | Same broker address used in the successful smoke test |
| topic | telehealth-clickstream |
| table | telehealth_dev.bronze.clickstream_raw |
| checkpoint | /Volumes/telehealth_dev/ops/stream_checkpoints/s01/v1 |
| secret_scope | Your existing scope |
| api_key_secret / api_secret_secret | Secret names, not secret values |
| mode | available_now |
| max_offsets | 10000 |

Run only one instance for this checkpoint/table. The notebook persists a binding
alongside the checkpoint to detect changes to source/topic/sink. Never reuse a
checkpoint after changing those bindings or recreating a topic/table. Do not
delete a checkpoint to resolve an ordinary restart issue. Starting offsets apply
only to a new query; existing checkpoint offsets determine restart position.

## Three-run acceptance

Use this test with other producers paused so unchanged-count checks are meaningful.
Choose new local output directories for every publish attempt.

### Run A: ingest and finish

Send 20 acknowledged messages locally (requires completed Day 6 configuration):

```bash
python -m scripts.publish_events --send --limit 20 --rate 10 --output-dir data/day-07-send-a
```

Run the notebook in `available_now`. It must finish without a query exception.
Save printed query ID, run ID and last progress. Note that last progress describes
only the last batch, not total ingestion. Record the Delta table version:

```python
spark.sql(f"DESCRIBE HISTORY {config.table}").select("version", "timestamp", "operation").show(5, False)
```

Upload `data/day-07-send-a/delivery-report.json` to a readable UC Volume path.
Run this additional notebook cell, replacing the report path:

```python
from pipelines.bronze_evidence import verify_bronze
report_a = json.loads(Path("/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME/report-a.json").read_text())
check_a = verify_bronze(spark, config.table, [report_a])
assert check_a["status"] == "PASS", check_a
count_a = check_a["table_rows"]
query_id_a = str(query.id)
print(json.dumps(check_a, indent=2))
```

The matcher checks exact acknowledged offsets; it does not assume Kafka offsets
are consecutive. Existing historical messages in the topic may also be ingested.

### Run B: no new input, same checkpoint

Rerun the startup and wait cells with exactly the same configuration. The previous
AvailableNow query has already stopped. This is a stop/restart test; an abrupt
process-kill recovery test belongs to Day 23.

```python
check_b = verify_bronze(spark, config.table, [report_a])
assert check_b["status"] == "PASS", check_b
assert check_b["table_rows"] == count_a
assert str(query.id) == query_id_a
print(json.dumps(check_b, indent=2))
```

The query ID should persist while run ID changes. Save both. If Notebook variables
were cleared, reload the saved A evidence rather than taking a fresh baseline.

### Run C: new offsets, same checkpoint

While the stream is stopped, send another acknowledged set:

```bash
python -m scripts.publish_events --send --limit 20 --rate 10 --output-dir data/day-07-send-b
```

Reusing event IDs here is deliberate: Bronze must preserve both deliveries at
different offsets. Upload the second delivery report as `report-b.json`, then
restart the notebook using the SAME checkpoint and target.

```python
report_b = json.loads(Path("/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME/report-b.json").read_text())
check_c = verify_bronze(spark, config.table, [report_a, report_b])
assert check_c["status"] == "PASS", check_c
assert check_c["table_rows"] == count_a + report_b["acknowledged"]
assert str(query.id) == query_id_a
print(json.dumps(check_c, indent=2))
```

Save the two producer reports, A/B/C check results, query/run IDs, table versions,
runtime and checkpoint path in your evidence directory. Mark the Day 7 gate
complete only after all checks pass. Do not commit secret values.

For continuous ingestion on compatible classic compute, select `processing_time`.
Use `query.stop()` before restarting; never start two simultaneous writers using
one checkpoint. `query.lastProgress` may be empty until a batch finishes. The
notebook does not auto-stop an ongoing continuous query.

## Failure interpretation

- Unknown topic/authentication/network error: reuse Day 6 connection troubleshooting.
- Schema/Volume permission error: fix Databricks storage and table grants.
- Missing offsets / data-loss error: preserve evidence and investigate retention or
  topic recreation; do not set failOnDataLoss=false to make acceptance appear green.
- Duplicate raw keys: check concurrent jobs, changed checkpoints, prior manual
  table appends or topic recreation. A new checkpoint on a populated table can replay rows.
- Missing report offsets: confirm correct cluster/topic, query completion and retention.

Offline tests cover configuration, credential escaping and exact-offset evidence
comparison. They do not run Spark, Kafka or Delta and cannot prove exactly-once
commits or cloud recovery. Local dependencies intentionally do not install another
Spark distribution alongside the Databricks Runtime.

References: [checkpoints](https://docs.databricks.com/aws/en/structured-streaming/checkpoints),
[Kafka connector](https://docs.databricks.com/aws/en/connect/streaming/kafka),
[Delta streaming](https://docs.databricks.com/aws/en/structured-streaming/delta-lake).
