"""One atomic classified-delivery table, two views, no watermark or event dedup."""

import re

from pipelines.event_validation import classify, header_id
from scripts.validate_contracts import load_contract

EVENT_DDL = """event_id STRING, event_name STRING, event_time TIMESTAMP,
received_at TIMESTAMP, anonymous_id STRING, customer_id STRING, session_id STRING,
booking_journey_id STRING, appointment_id STRING, scheduled_start_time TIMESTAMP,
provider_id STRING, device_type STRING, platform STRING, specialty STRING, schema_version INT"""


def validate_names(source, target, checkpoint, app_id):
    for table in (source, target):
        if not re.fullmatch(r"[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*", table):
            raise ValueError("Use simple three-part table names")
    if (
        source == target
        or not checkpoint.startswith(("/Volumes/", "s3://", "abfss://", "gs://"))
        or not app_id
    ):
        raise ValueError(
            "Separate source/target, durable checkpoint and stable app ID required"
        )
    if any(p in (".", "..") for p in checkpoint.split("/")):
        raise ValueError("Relative checkpoint components forbidden")


def make_batch_handler(target, app_id, resolver, max_schema_ids=100):
    contract, rules = load_contract()

    def process(batch, batch_id):
        from pyspark.sql import functions as F

        session = batch.sparkSession
        # Use payload header, not the Bronze best-effort schema_id hint.
        extract = F.udf(header_id, "long")
        ids = (
            batch.select(extract("value").alias("id"))
            .where("id IS NOT NULL")
            .distinct()
            .limit(max_schema_ids + 1)
            .collect()
        )
        if len(ids) > max_schema_ids:
            raise ValueError("Too many schema IDs; batch stopped for investigation")
        schemas = {row.id: resolver.resolve(row.id) for row in ids}
        # Credentials and HTTP session stay on driver. Workers receive schemas only.
        decode = F.udf(
            lambda value: classify(value, schemas, contract, rules),
            "schema_id LONG, rule_version STRING, route STRING, error_codes ARRAY<STRING>, event_json STRING",
        )
        classified = (
            batch.withColumn("_validation", decode("value"))
            .drop("schema_id")
            .select("*", "_validation.*")
            .drop("_validation")
            .withColumn("processed_at", F.current_timestamp())
        )
        (
            classified.write.format("delta")
            .mode("append")
            .option("txnAppId", app_id)
            .option("txnVersion", batch_id)
            .saveAsTable(target)
        )
        schema = target.rsplit(".", 1)[0]
        # Views share a single commit: partial routing into two sinks is impossible.
        session.sql(f"""CREATE OR REPLACE VIEW {schema}.validated_deliveries AS
            SELECT raw_record_id, topic, partition, offset, schema_id, rule_version,
                   ingested_at, processed_at, parsed.* FROM (
                SELECT *, from_json(event_json, '{EVENT_DDL}') AS parsed
                FROM {target} WHERE route = 'valid')""")
        session.sql(f"""CREATE OR REPLACE VIEW {schema}.quarantine_events AS
            SELECT * FROM {target} WHERE route = 'quarantine' """)

    return process


def start_validation(spark, source, target, checkpoint, app_id, resolver):
    validate_names(source, target, checkpoint, app_id)
    return (
        spark.readStream.table(source)
        .writeStream.option("checkpointLocation", checkpoint)
        .queryName("s02_" + target.replace(".", "_"))
        .foreachBatch(make_batch_handler(target, app_id, resolver))
        .trigger(availableNow=True)
        .start()
    )
