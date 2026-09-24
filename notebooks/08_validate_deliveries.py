# Databricks notebook source
# MAGIC %md
# MAGIC Step 8: use a compatible classic Databricks Runtime cluster.
# MAGIC Install fastavro==1.12.2 and requests==2.34.2 as cluster libraries first.
# MAGIC Keep the repository pipelines/ and scripts/ importable on Python workers.
# COMMAND ----------
import json
import sys
from pathlib import Path

for name, default in {
    "repo_root": "",
    "source": "workspace.bronze.clickstream_raw_day07_check02",
    "target": "workspace.silver.classified_deliveries",
    "checkpoint": "/Volumes/workspace/ops/stream_checkpoints/s02/v1",
    "app_id": "telehealth-s02-v1",
    "registry_url": "",
    "secret_scope": "",
    "registry_key_secret": "schema-registry-api-key",
    "registry_secret_secret": "schema-registry-api-secret",
}.items():
    dbutils.widgets.text(name, default)

# COMMAND ----------
root = Path(dbutils.widgets.get("repo_root") or Path.cwd())
if not (root / "pipelines").is_dir():
    root = root.parent
if not (root / "pipelines").is_dir():
    raise ValueError("Set repo_root to the workspace repository root")
sys.path.insert(0, str(root))
from pipelines.event_validation import RegistryResolver
from pipelines.silver_validation import start_validation, validate_names

source, target, checkpoint, app_id = [
    dbutils.widgets.get(n) for n in ("source", "target", "checkpoint", "app_id")
]
validate_names(source, target, checkpoint, app_id)
binding = {
    "source": source,
    "target": target,
    "app_id": app_id,
    "rule_version": "1.0.0",
}
directory = checkpoint.rstrip("/") + "_binding"
dbutils.fs.mkdirs(directory)
path = directory + "/source-sink.json"
if any(e.name == "source-sink.json" for e in dbutils.fs.ls(directory)):
    if json.loads(dbutils.fs.head(path)) != binding:
        raise ValueError("Checkpoint binding changed; review configuration")
else:
    dbutils.fs.put(path, json.dumps(binding, sort_keys=True), overwrite=False)
resolver = RegistryResolver(
    dbutils.widgets.get("registry_url"),
    dbutils.secrets.get(
        dbutils.widgets.get("secret_scope"), dbutils.widgets.get("registry_key_secret")
    ),
    dbutils.secrets.get(
        dbutils.widgets.get("secret_scope"),
        dbutils.widgets.get("registry_secret_secret"),
    ),
)
spark.conf.set("spark.sql.session.timeZone", "UTC")
query = start_validation(spark, source, target, checkpoint, app_id, resolver)
query.awaitTermination()
print({"query_id": str(query.id), "run_id": str(query.runId), "status": "COMPLETED"})

# COMMAND ----------
display(spark.sql(f"SELECT route, count(*) AS deliveries FROM {target} GROUP BY route"))
