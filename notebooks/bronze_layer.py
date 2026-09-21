# Databricks notebook source
# MAGIC %md
# MAGIC Run from a Databricks Git folder containing this repository, on the compute
# MAGIC already tested with Kafka. Configure widgets, then run all. No pip install
# MAGIC is needed; the Databricks Runtime supplies Spark/Kafka/Delta.

# COMMAND ----------
import json
import sys
from pathlib import Path

# Notebook resides directly under <repository>/notebooks/.
repo = Path.cwd()
if not (repo / "pipelines").is_dir():
    repo = repo.parent
if not (repo / "pipelines").is_dir():
    raise RuntimeError("Open this notebook within the repository Git folder")
sys.path.insert(0, str(repo))

from pipelines.bronze_kafka import BronzeConfig, start_bronze

for name, default in {
    "bootstrap_servers": "",
    "topic": "telehealth-clickstream",
    "table": "telehealth_dev.bronze.clickstream_raw",
    "checkpoint": "",
    "secret_scope": "",
    "api_key_secret": "kafka-api-key",
    "api_secret_secret": "kafka-api-secret",
    "mode": "available_now",
    "max_offsets": "10000",
}.items():
    dbutils.widgets.text(name, default)

# COMMAND ----------
config = BronzeConfig(
    bootstrap_servers=dbutils.widgets.get("bootstrap_servers"),
    topic=dbutils.widgets.get("topic"),
    table=dbutils.widgets.get("table"),
    checkpoint=dbutils.widgets.get("checkpoint"),
    mode=dbutils.widgets.get("mode"),
    max_offsets=int(dbutils.widgets.get("max_offsets")),
)
# Persist a source/sink binding alongside the checkpoint. Missing permissions or
# mismatched bindings fail closed. Never share this directory between queries.
binding_dir = config.checkpoint.rstrip("/") + "_binding"
dbutils.fs.mkdirs(binding_dir)
binding_file = binding_dir + "/source-sink.json"
binding = {
    "bootstrap_servers": config.bootstrap_servers,
    "topic": config.topic,
    "table": config.table,
    "pipeline_version": 1,
}
entries = dbutils.fs.ls(binding_dir)
if any(e.name == "source-sink.json" for e in entries):
    if json.loads(dbutils.fs.head(binding_file)) != binding:
        raise ValueError("Checkpoint source/sink binding differs; review configuration")
else:
    dbutils.fs.put(binding_file, json.dumps(binding, sort_keys=True), overwrite=False)

query = start_bronze(
    spark,
    config,
    dbutils.secrets.get(
        dbutils.widgets.get("secret_scope"), dbutils.widgets.get("api_key_secret")
    ),
    dbutils.secrets.get(
        dbutils.widgets.get("secret_scope"), dbutils.widgets.get("api_secret_secret")
    ),
)
print({"query_id": str(query.id), "run_id": str(query.runId), "mode": config.mode})

# COMMAND ----------
if config.mode == "available_now":
    query.awaitTermination()
    print("AvailableNow completed successfully")
else:
    print("Query running. Stop explicitly with query.stop() before restarting.")

# COMMAND ----------
# Capture bounded progress evidence without credentials or full source descriptions.
progress = query.lastProgress
if progress:
    print(
        json.dumps(
            {
                "query_id": str(query.id),
                "run_id": str(query.runId),
                "batch_id": progress["batchId"],
                "input_rows": progress["numInputRows"],
                "sources": [
                    {
                        "startOffset": s.get("startOffset"),
                        "endOffset": s.get("endOffset"),
                    }
                    for s in progress.get("sources", [])
                ],
            },
            indent=2,
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC Follow docs/day-07-bronze.md for report upload, offset matching and
# MAGIC stop/restart checks. Do not delete the checkpoint or recreate the target.
