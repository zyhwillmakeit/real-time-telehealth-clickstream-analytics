# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Step 8 — Negative and boundary acceptance
# MAGIC 
# MAGIC Run this notebook on a compatible classic Databricks Runtime with the repository available to Python workers. It consumes an **existing six-message test batch**; it does not send Kafka messages.
# MAGIC 
# MAGIC The six-case run and normal 50-row restart were confirmed by the project owner. This cleaned notebook is a reproducible runbook, not an export of those execution outputs.
# MAGIC 
# MAGIC | Case | Deliveries | Expected |
# MAGIC |---|---:|---|
# MAGIC | corrupt_avro | 1 | quarantine: AVRO_DECODE_FAILED |
# MAGIC | multiple_errors | 1 | quarantine: FIELD_REQUIRED:appointment_id + DEVICE_PLATFORM_MISMATCH |
# MAGIC | late_event | 1 | valid (35 minutes late) |
# MAGIC | missing_customer | 1 | valid (synthetic customer ID; no lookup at this step) |
# MAGIC | duplicate_a / duplicate_b | 2 | valid, identical event_id, distinct Kafka offsets |
# MAGIC 
# MAGIC Expected: **6 deliveries = 4 valid + 2 quarantined**. Step 8 does not apply watermarks, customer joins or event-ID deduplication.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - Create the dedicated topic `telehealth-step08-negative-01` and send the fixed six-message fixture once using the local producer procedure.
# MAGIC - Upload `expected.json` and the successful `producer-report.json` into `/Volumes/workspace/ops/validation_evidence/step08-negative-01/`.
# MAGIC - Configure Kafka and Schema Registry secrets in scope `telehealth`.
# MAGIC - Install `fastavro==1.12.2` and `requests==2.34.2` on the compute. If using `%pip install`, call `dbutils.library.restartPython()` **before** running the cells below.
# MAGIC - Use a separate Silver schema: the pipeline owns fixed-name views within that schema.
# MAGIC 
# MAGIC Do not resend the fixture, delete checkpoints, or reuse these app IDs for a different checkpoint. Keep Step 7 evidence untouched. A completely new run requires a new topic, tables, checkpoints, app ID and evidence directory.
# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Configure widgets
# MAGIC Run this cell, then fill `repo_root` and `registry_url` in the widget bar. Registry URL is an HTTPS endpoint, not a credential. Defaults point to the previously accepted isolated test resources.
# COMMAND ----------
for name, default in {
    "repo_root": "",
    "registry_url": "",
    "secret_scope": "telehealth",
    "topic": "telehealth-step08-negative-01",
    "source": "workspace.bronze_step08_test01.clickstream_raw",
    "target": "workspace.silver_step08_test01.classified_deliveries",
    "bronze_checkpoint": "/Volumes/workspace/ops/stream_checkpoints/step08-negative-01/s01",
    "silver_checkpoint": "/Volumes/workspace/ops/stream_checkpoints/step08-negative-01/s02",
    "app_id": "telehealth-step08-negative-01-s02",
    "evidence_dir": "/Volumes/workspace/ops/validation_evidence/step08-negative-01",
}.items():
    dbutils.widgets.text(name, default)
# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Check compute and load configuration
# MAGIC If `SELECT 1` hangs, resolve the compute/notebook session before continuing. No Kafka connection is attempted by this check.
# COMMAND ----------
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

spark.sql("SELECT 1 AS connection_test").show()
root = Path(dbutils.widgets.get("repo_root"))
assert (root / "pipelines").is_dir(), "Set repo_root to your Workspace repository root"
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from pipelines.bronze_kafka import BronzeConfig, start_bronze
from pipelines.event_validation import RegistryResolver, RULE_VERSION
from pipelines.silver_validation import start_validation, validate_names

get = dbutils.widgets.get
topic, source, target = [get(n).strip() for n in ("topic", "source", "target")]
bronze_checkpoint = get("bronze_checkpoint").rstrip("/")
silver_checkpoint = get("silver_checkpoint").rstrip("/")
app_id = get("app_id").strip()
scope = get("secret_scope").strip()
registry_url = get("registry_url").strip()
evidence_dir = Path(get("evidence_dir"))
validate_names(source, target, silver_checkpoint, app_id)
assert registry_url.startswith("https://"), "Fill registry_url with the Registry HTTPS endpoint"
assert str(evidence_dir).startswith("/Volumes/")
assert bronze_checkpoint != silver_checkpoint
source_schema = source.rsplit(".", 1)[0]
target_schema = target.rsplit(".", 1)[0]
assert source_schema != target_schema, "Use separate Bronze and Silver schemas"
spark.conf.set("spark.sql.session.timeZone", "UTC")
print("Configuration loaded; no credentials printed")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Load independent producer evidence
# MAGIC Fail before ingestion if the batch is incomplete or its expected cases differ. The local report supplies actual Kafka offsets; it is not reconstructed from the output table.
# COMMAND ----------
expected = json.loads((evidence_dir / "expected.json").read_text())
report = json.loads((evidence_dir / "producer-report.json").read_text())
case_contract = {
    "corrupt_avro": ("quarantine", ["AVRO_DECODE_FAILED"]),
    "multiple_errors": ("quarantine", ["DEVICE_PLATFORM_MISMATCH", "FIELD_REQUIRED:appointment_id"]),
    "late_event": ("valid", []),
    "missing_customer": ("valid", []),
    "duplicate_a": ("valid", []),
    "duplicate_b": ("valid", []),
}
expected_by_label = {r["delivery_id"]: r for r in expected}
assert len(expected) == len(expected_by_label) == 6
assert set(expected_by_label) == set(case_contract)
for label, (route, errors) in case_contract.items():
    assert expected_by_label[label]["route"] == route
    assert sorted(expected_by_label[label]["error_codes"]) == sorted(errors)
assert expected_by_label["duplicate_a"]["event_id"] == expected_by_label["duplicate_b"]["event_id"]
assert report["status"] == "PASS" and report["planned"] == report["acknowledged"] == 6
assert report["remaining"] == 0 and not report["submission_error"]
deliveries = report["deliveries"]
assert len(deliveries) == 6
assert all(r["status"] == "ACKNOWLEDGED" and r["topic"] == topic for r in deliveries)
assert {r["delivery_id"] for r in deliveries} == set(case_contract)

def kafka_key(row):
    return row["topic"], int(row["partition"]), int(row["offset"])

sent_keys = {kafka_key(r) for r in deliveries}
assert len(sent_keys) == 6
print("Producer evidence validated: six unique acknowledged Kafka offsets")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Create schemas and protect checkpoint bindings
# MAGIC Bindings prevent accidental changes when resuming this run. Do not overwrite an existing binding to bypass a mismatch.
# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {source_schema}").collect()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}").collect()

def bind_checkpoint(checkpoint, binding):
    directory = checkpoint.rstrip("/") + "_binding"
    dbutils.fs.mkdirs(directory)
    path = directory + "/source-sink.json"
    if any(e.name == "source-sink.json" for e in dbutils.fs.ls(directory)):
        assert json.loads(dbutils.fs.head(path)) == binding, "Checkpoint binding changed"
    else:
        dbutils.fs.put(path, json.dumps(binding, sort_keys=True), overwrite=False)

bind_checkpoint(bronze_checkpoint, {"topic": topic, "target": source})
bind_checkpoint(silver_checkpoint, {
    "source": source, "target": target, "app_id": app_id, "rule_version": RULE_VERSION,
})
# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Ingest Kafka into Bronze
# MAGIC AvailableNow finishes after processing the available input. Existing checkpoints resume normally. This isolated topic/table must contain exactly the six reported deliveries.
# COMMAND ----------
bronze_config = BronzeConfig(
    bootstrap_servers=dbutils.secrets.get(scope, "kafka-bootstrap-servers"),
    topic=topic, table=source, checkpoint=bronze_checkpoint, mode="available_now",
)
bronze_query = start_bronze(
    spark, bronze_config,
    dbutils.secrets.get(scope, "kafka-api-key"),
    dbutils.secrets.get(scope, "kafka-api-secret"),
)
bronze_query.awaitTermination()
assert spark.table(source).count() == 6, "Stop: expected exactly six Bronze deliveries"
print("Bronze ingestion completed")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Decode and classify Bronze deliveries
# COMMAND ----------
resolver = RegistryResolver(
    registry_url,
    dbutils.secrets.get(scope, "schema-registry-api-key"),
    dbutils.secrets.get(scope, "schema-registry-api-secret"),
)
query = start_validation(spark, source, target, silver_checkpoint, app_id, resolver)
query.awaitTermination()
first_query_id, first_run_id = str(query.id), str(query.runId)
display(spark.sql(f"SELECT route, COUNT(*) AS deliveries FROM {target} GROUP BY route"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Verify exact coverage, routes, payloads and views
# MAGIC Collect is bounded to seven rows: this is a six-record acceptance fixture, not a production table audit. Any extra row fails the test. The missing-customer case verifies preservation of its ID; absence from a particular customer snapshot requires a separate lookup.
# COMMAND ----------
def verify_acceptance():
    bronze = [r.asDict() for r in spark.table(source).select(
        "topic", "partition", "offset", "raw_record_id"
    ).limit(7).collect()]
    rows = [r.asDict() for r in spark.table(target).select(
        "topic", "partition", "offset", "raw_record_id", "route", "error_codes", "event_json"
    ).limit(7).collect()]
    assert len(bronze) == len(rows) == 6
    assert {kafka_key(r) for r in bronze} == sent_keys
    actual = {kafka_key(r): r for r in rows}
    assert len(actual) == 6 and set(actual) == sent_keys
    for collection in (bronze, rows):
        assert len({r["raw_record_id"] for r in collection}) == 6
        assert all(r["raw_record_id"] == ":".join(map(str, kafka_key(r))) for r in collection)
    assert Counter(r["route"] for r in rows) == {"valid": 4, "quarantine": 2}
    results, events = [], {}
    for delivery in deliveries:
        label = delivery["delivery_id"]
        want, got = expected_by_label[label], actual[kafka_key(delivery)]
        assert got["route"] == want["route"], label
        assert sorted(got["error_codes"]) == sorted(want["error_codes"]), label
        assert delivery["event_id"] == want["event_id"], label
        if label == "corrupt_avro":
            assert got["event_json"] is None
        else:
            event = json.loads(got["event_json"])
            assert event["event_id"] == want["event_id"], label
            events[label] = event
        results.append({
            "delivery_id": label, "topic": delivery["topic"],
            "partition": delivery["partition"], "offset": delivery["offset"],
            "route": got["route"], "error_codes": got["error_codes"], "status": "PASS",
        })
    late = events["late_event"]
    parse = lambda value: datetime.fromisoformat(value.replace("Z", "+00:00"))
    assert (parse(late["received_at"]) - parse(late["event_time"])).total_seconds() == 2100
    assert events["missing_customer"]["customer_id"] == "cus_99999999"
    assert events["multiple_errors"]["appointment_id"] is None
    assert events["multiple_errors"]["device_type"] == "desktop"
    assert events["multiple_errors"]["platform"] == "ios"
    assert events["duplicate_a"] == events["duplicate_b"]
    valid = spark.table(target_schema + ".validated_deliveries").select("raw_record_id").limit(7).collect()
    quarantine = spark.table(target_schema + ".quarantine_events").select("raw_record_id").limit(7).collect()
    assert len(valid) == 4 and len(quarantine) == 2
    for view_rows, route in ((valid, "valid"), (quarantine, "quarantine")):
        assert {r.raw_record_id for r in view_rows} == {r["raw_record_id"] for r in rows if r["route"] == route}
    return results

case_results = verify_acceptance()
print(json.dumps(case_results, indent=2))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Resume the same query and verify again
# MAGIC No producer resend and no checkpoint/app-ID change. This tests normal completion and restart, not process termination.
# COMMAND ----------
before_count = spark.table(target).count()
retry_query = start_validation(spark, source, target, silver_checkpoint, app_id, resolver)
retry_query.awaitTermination()
after_count = spark.table(target).count()
restart_result = {
    "before": before_count, "after": after_count,
    "same_query_id": str(retry_query.id) == first_query_id,
    "different_run_id": str(retry_query.runId) != first_run_id,
}
assert before_count == after_count == 6
assert restart_result["same_query_id"] and restart_result["different_run_id"]
case_results = verify_acceptance()
print(restart_result)
# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Save fresh evidence without overwriting the original
# MAGIC Every successful notebook run writes a uniquely named report next to the original `acceptance.json`. Keep the original producer report and expectations. Publish only reviewed, redacted evidence to GitHub; this notebook has no saved outputs or credentials.
# COMMAND ----------
# Recheck immediately before writing evidence.
case_results = verify_acceptance()
assert restart_result["before"] == restart_result["after"] == 6
assert restart_result["same_query_id"] and restart_result["different_run_id"]
now = datetime.now(timezone.utc)
acceptance = {
    "step": 8, "status": "PASS", "scope": "six-delivery negative and boundary acceptance",
    "checked_at": now.isoformat(), "topic": topic, "source": source, "target": target,
    "bronze_checkpoint": bronze_checkpoint, "silver_checkpoint": silver_checkpoint,
    "app_id": app_id, "rule_version": RULE_VERSION,
    "bronze_rows": 6, "classified_rows": 6, "valid_rows": 4, "quarantine_rows": 2,
    "missing": 0, "unexpected": 0, "duplicate_keys": 0, "invalid_routes": 0,
    "restart": restart_result, "query_id": first_query_id,
    "first_run_id": first_run_id, "retry_run_id": str(retry_query.runId),
    "cases": case_results,
    "not_verified": ["customer snapshot absence", "tombstone live routing", "unknown schema ID live routing",
                     "new input after classification restart", "abrupt termination recovery"],
}
name = "acceptance-" + now.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8] + ".json"
path = evidence_dir / name
with path.open("x", encoding="utf-8") as handle:
    json.dump(acceptance, handle, indent=2)
print(f"PASS: evidence saved to {path}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Completion boundary
# MAGIC This notebook establishes exact offset coverage, expected routes/errors, view consistency, boundary-event preservation and idempotent normal restart for the six-message fixture. Together with the owner-confirmed normal 50-row run, this satisfies the core Step 8 milestone and permits Step 9 work.
# MAGIC 
# MAGIC The broader runbook still lists live tombstone/unknown-schema cases and new input after S02 restart. Those are **pending**, not implied by this result. Process-termination recovery belongs to Step 23. This notebook does not measure latency or validate a customer join.
