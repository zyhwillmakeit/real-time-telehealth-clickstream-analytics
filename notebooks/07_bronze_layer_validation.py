# Databricks notebook source

# MAGIC %md
# MAGIC # Day 7 — Bronze Layer Validation
# MAGIC Validate Kafka-to-Delta ingestion and graceful restart continuity.
# MAGIC
# MAGIC **Run one selected stage at a time.** Default `REVIEW` reads saved evidence only.
# MAGIC Run the parameter cell first, configure the widgets, then run all remaining cells.
# MAGIC Every run restores its own imports, configuration and persisted baselines; no
# MAGIC variables from a previous Python session are required.
# MAGIC
# MAGIC - **A:** Ingest the first batch and save its baseline.
# MAGIC - **B:** With producers paused, restart and verify that the row count is unchanged.
# MAGIC - **C:** After B passes, publish another fully acknowledged batch, upload its report,
# MAGIC   then restart and verify all old and new offsets.
# MAGIC - **REVIEW:** Inspect saved A/B/C results without starting a query.
# MAGIC
# MAGIC Keep the same source, target, checkpoint and evidence directory throughout a test.
# MAGIC Only one writer may use this checkpoint. Do not delete checkpoint files or rewrite
# MAGIC bindings to resolve configuration differences. This notebook does not test abrupt
# MAGIC process termination or event-level deduplication.

# COMMAND ----------

dbutils.widgets.dropdown("stage", "REVIEW", ["REVIEW", "A", "B", "C"])
for name, default in {
    "repo_root": "",
    "bootstrap_servers": "",
    "topic": "telehealth_analytics",
    "table": "workspace.bronze.clickstream_raw_day07_check02",
    "checkpoint": "/Volumes/workspace/ops/stream_checkpoints/s01/day07_check02",
    "secret_scope": "",
    "api_key_secret": "kafka-api-key",
    "api_secret_secret": "kafka-api-secret",
    "max_offsets": "10000",
    "report_a": "/Volumes/workspace/ops/validation_evidence/delivery-report.json",
    "report_b": "/Volumes/workspace/ops/validation_evidence/report-b.json",
    "evidence_dir": "/Volumes/workspace/ops/validation_evidence/day07-check-03",
}.items():
    dbutils.widgets.text(name, default)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC Keep this notebook in the repository with the sibling `pipelines/` package.
# MAGIC If automatic discovery fails, set `repo_root` to the workspace repository path.
# MAGIC Databricks supplies Spark, Kafka and Delta. Credentials come from Secrets only.
# MAGIC The bootstrap widget requires the actual broker address, not a secret name.
# MAGIC
# MAGIC The defaults identify the user's completed acceptance environment. Select REVIEW
# MAGIC to inspect it. For a new test, choose a new evidence directory. Reuse the existing
# MAGIC checkpoint with its existing table, or create a new empty table AND a new checkpoint.
# MAGIC Never attach a fresh checkpoint to a populated target as an ordinary restart.
# MAGIC
# MAGIC If necessary, create these objects separately with an authorized identity:
# MAGIC ```sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.ops;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.ops.stream_checkpoints;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.ops.validation_evidence;
# MAGIC ```
# MAGIC Do not upload `.env`. Use compute already verified with Kafka and AvailableNow.

# COMMAND ----------

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

stage = dbutils.widgets.get("stage")
if stage not in {"REVIEW", "A", "B", "C"}:
    raise ValueError("Unknown validation stage")


def volume_file(value):
    path = Path(value)
    if not value.startswith("/Volumes/") or len(path.parts) < 6 or ".." in path.parts:
        raise ValueError("Use a path inside /Volumes/catalog/schema/volume/")
    return path


evidence_dir = volume_file(dbutils.widgets.get("evidence_dir"))


def load_result(name):
    data = json.loads((evidence_dir / f"{name}.json").read_text())
    # Accept the legacy evidence layout produced by the original notebook.
    if data.get("check", {}).get("status") != "PASS":
        raise ValueError(f"Stage {name} has no successful validation result")
    return data


def save_result(name, data):
    evidence_dir.mkdir(parents=True, exist_ok=True)
    with (evidence_dir / f"{name}.json").open("x", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, default=str)


if stage == "REVIEW":
    for name in ("A", "B", "C"):
        path = evidence_dir / f"{name}.json"
        if not path.exists():
            print(f"Stage {name}: no saved evidence")
            continue
        result = json.loads(path.read_text())
        print(
            json.dumps(
                {
                    "stage": name,
                    "check": result.get("check"),
                    "query_id": result.get("query_id"),
                    "run_id": result.get("run_id"),
                },
                indent=2,
            )
        )
    print("Review only. No ingestion query was started.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restore configuration and validate inputs
# MAGIC All reports are checked before starting ingestion. A FAILED producer report is
# MAGIC not repaired by editing its status or counts. A timeout can have an uncertain
# MAGIC broker outcome; preserve the failed report and reconcile before another test.

# COMMAND ----------

if stage != "REVIEW":
    if (evidence_dir / f"{stage}.json").exists():
        raise RuntimeError(
            f"Stage {stage} evidence exists. Select REVIEW or the next stage."
        )
    override = dbutils.widgets.get("repo_root").strip()
    candidates = [Path(override)] if override else [Path.cwd(), *Path.cwd().parents]
    repo = next(
        (p for p in candidates if (p / "pipelines/bronze_kafka.py").is_file()), None
    )
    if repo is None:
        raise RuntimeError("Set repo_root to the repository containing pipelines/")
    sys.path.insert(0, str(repo))
    from pipelines.bronze_kafka import BronzeConfig, start_bronze
    from pipelines.bronze_evidence import expected_keys, verify_bronze

    config = BronzeConfig(
        bootstrap_servers=dbutils.widgets.get("bootstrap_servers").strip(),
        topic=dbutils.widgets.get("topic").strip(),
        table=dbutils.widgets.get("table").strip(),
        checkpoint=dbutils.widgets.get("checkpoint").strip(),
        mode="available_now",
        max_offsets=int(dbutils.widgets.get("max_offsets")),
    )
    baseline_a = baseline_b = None
    if stage == "A":
        report_a = json.loads(volume_file(dbutils.widgets.get("report_a")).read_text())
    else:
        baseline_a = load_result("A")
        for field in ("table", "checkpoint", "topic"):
            if getattr(config, field) != baseline_a[field]:
                raise ValueError(f"Configuration differs from stage A: {field}")
        report_a = baseline_a["report_a"]
        if stage == "C":
            baseline_b = load_result("B")
            if baseline_b["query_id"] != baseline_a["query_id"]:
                raise ValueError(
                    "Stages A and B belong to different checkpoint queries"
                )
    reports = [report_a]
    if stage == "C":
        report_b = json.loads(volume_file(dbutils.widgets.get("report_b")).read_text())
        reports.append(report_b)
    for index, report in enumerate(reports):
        print(
            {
                "report": index + 1,
                "status": report.get("status"),
                "planned": report.get("planned"),
                "acknowledged": report.get("acknowledged"),
                "delivery_count": len(report.get("deliveries", [])),
            }
        )
    expected_keys(reports)
    if any(
        row["topic"] != config.topic
        for report in reports
        for row in report["deliveries"]
    ):
        raise ValueError("Producer report topic differs from the ingestion topic")
    if any(q.isActive for q in spark.streams.active):
        raise RuntimeError(
            "Stop active queries before starting this isolated acceptance run"
        )

    binding_dir = config.checkpoint.rstrip("/") + "_binding"
    binding_file = binding_dir + "/source-sink.json"
    binding = {
        "bootstrap_servers": config.bootstrap_servers,
        "topic": config.topic,
        "table": config.table,
        "pipeline_version": 1,
    }
    dbutils.fs.mkdirs(binding_dir)
    if any(e.name == "source-sink.json" for e in dbutils.fs.ls(binding_dir)):
        if json.loads(dbutils.fs.head(binding_file)) != binding:
            raise ValueError(
                "Checkpoint binding differs. Investigate; do not overwrite it."
            )
    else:
        # A missing binding is only initialized for a fresh checkpoint and empty target.
        parent, folder = config.checkpoint.rstrip("/").rsplit("/", 1)
        if any(e.name.rstrip("/") == folder for e in dbutils.fs.ls(parent)):
            raise ValueError(
                "Checkpoint exists without a binding. Review its provenance first."
            )
        if (
            spark.catalog.tableExists(config.table)
            and spark.table(config.table).limit(1).count()
        ):
            raise ValueError(
                "Fresh checkpoint with populated target would replay data. Use a new empty target."
            )
        dbutils.fs.put(
            binding_file, json.dumps(binding, sort_keys=True), overwrite=False
        )
    print({"stage": stage, "table": config.table, "checkpoint": config.checkpoint})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute exactly one AvailableNow run
# MAGIC This cell performs a real restart for B/C and waits for completion before validation.
# MAGIC The source/sink configuration and checkpoint remain unchanged between stages.

# COMMAND ----------

if stage != "REVIEW":
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    scope = dbutils.widgets.get("secret_scope")
    query = start_bronze(
        spark,
        config,
        dbutils.secrets.get(scope, dbutils.widgets.get("api_key_secret")),
        dbutils.secrets.get(scope, dbutils.widgets.get("api_secret_secret")),
    )
    query.awaitTermination()
    print({"query_id": str(query.id), "run_id": str(query.runId)})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify and save evidence
# MAGIC A records the actual table size; it need not equal the first report's size.
# MAGIC B requires the same size. C requires B's size plus the new acknowledged deliveries.
# MAGIC Only passing evidence is saved as A/B/C.json, with exclusive creation. Failed checks
# MAGIC remain visible in the output and must be investigated before continuing.

# COMMAND ----------

if stage != "REVIEW":
    check = verify_bronze(spark, config.table, reports)
    print(json.dumps(check, indent=2))
    if check["status"] != "PASS":
        raise RuntimeError("Missing or duplicate raw keys. Acceptance failed.")
    if stage != "A":
        previous = baseline_a if stage == "B" else baseline_b
        expected_rows = previous["table_rows"] + (
            report_b["acknowledged"] if stage == "C" else 0
        )
        if check["table_rows"] != expected_rows:
            raise RuntimeError(
                f"Expected {expected_rows} rows, got {check['table_rows']}. Check other writers or partial sends."
            )
        if str(query.id) != baseline_a["query_id"]:
            raise RuntimeError(
                "Query ID changed; checkpoint continuity was not demonstrated"
            )
        if str(query.runId) in {baseline_a["run_id"], previous["run_id"]}:
            raise RuntimeError(
                "Run ID did not change; a new execution was not demonstrated"
            )

    commit = (
        spark.sql(f"DESCRIBE HISTORY {config.table}")
        .select("version", "timestamp", "operation")
        .first()
    )
    progress = query.lastProgress
    result = {
        "stage": stage,
        "table": config.table,
        "checkpoint": config.checkpoint,
        "topic": config.topic,
        "query_id": str(query.id),
        "run_id": str(query.runId),
        "table_rows": check["table_rows"],
        "check": check,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "delta_commit": commit.asDict() if commit else None,
        "last_batch": None
        if not progress
        else {
            "batch_id": progress["batchId"],
            "input_rows": progress["numInputRows"],
            "sources": [
                {"startOffset": s.get("startOffset"), "endOffset": s.get("endOffset")}
                for s in progress.get("sources", [])
            ],
        },
    }
    if stage == "A":
        result["report_a"] = report_a
    elif stage == "C":
        result["report_b"] = report_b
    save_result(stage, result)
    print(f"Stage {stage} passed. Evidence saved to {evidence_dir / (stage + '.json')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next action and interpretation
# MAGIC - After A: keep producers paused, select B, and rerun the notebook.
# MAGIC - After B: publish another batch and require PASS with all deliveries acknowledged.
# MAGIC   Upload its **delivery-report.json**, select C, and rerun the notebook.
# MAGIC - After C: keep all three results and both successful producer reports. Select REVIEW
# MAGIC   on subsequent visits; do not repeat the producer merely to inspect results.
# MAGIC
# MAGIC A Python session restart clears memory, not Delta tables or Volume evidence.
# MAGIC Rerun this notebook for the selected stage to restore dependencies automatically.
# MAGIC Never treat empty duplicate-query output as an empty Bronze table.
# MAGIC
# MAGIC The user reported a completed run with 40 matched report offsets, 50 total rows and
# MAGIC zero duplicate or missing raw keys. The additional 10 rows were included in the
# MAGIC 30-row baseline after an earlier partial producer success. Those numbers describe
# MAGIC that run only and are not hardcoded acceptance criteria. This cleaned notebook
# MAGIC must be validated in the cloud when used for a new run; local checks do not prove
# MAGIC Spark/Delta recovery.
