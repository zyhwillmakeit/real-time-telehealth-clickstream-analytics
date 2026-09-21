"""S01: preserve Kafka deliveries in an append-only Delta table.

PySpark is provided by Databricks and imported only inside Spark functions.
"""

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class BronzeConfig:
    bootstrap_servers: str
    topic: str
    table: str
    checkpoint: str
    mode: str = "available_now"
    max_offsets: int = 10000

    def __post_init__(self):
        if not self.bootstrap_servers or not re.fullmatch(
            r"[A-Za-z0-9._-]+", self.topic
        ):
            raise ValueError("A bootstrap server and one explicit topic are required")
        if not re.fullmatch(
            r"[A-Za-z_][\w]*\.[A-Za-z_][\w]*\.[A-Za-z_][\w]*", self.table
        ):
            raise ValueError(
                "Table must be catalog.schema.table using simple identifiers"
            )
        if not self.checkpoint.startswith(("/Volumes/", "s3://", "abfss://", "gs://")):
            raise ValueError("Use a durable UC Volume or cloud-storage checkpoint path")
        if any(part in ("..", ".") for part in self.checkpoint.split("/")):
            raise ValueError("Checkpoint cannot contain relative path components")
        if (
            self.mode not in ("available_now", "processing_time")
            or self.max_offsets <= 0
        ):
            raise ValueError("Invalid trigger mode or max_offsets")


def jaas_config(api_key, api_secret):
    if not api_key or not api_secret:
        raise ValueError("Kafka credentials must be nonempty")

    def escape(value):
        if any(c in value for c in "\r\n\x00"):
            raise ValueError("Credential contains unsupported control characters")
        return value.replace("\\", "\\\\").replace('"', '\\"')

    return (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{escape(api_key)}" password="{escape(api_secret)}";'
    )


def reader_options(config, api_key, api_secret):
    return {
        "kafka.bootstrap.servers": config.bootstrap_servers,
        "subscribe": config.topic,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas_config(api_key, api_secret),
        "startingOffsets": "earliest",
        "failOnDataLoss": "true",
        "includeHeaders": "true",
        "maxOffsetsPerTrigger": str(config.max_offsets),
        "kafka.allow.auto.create.topics": "false",
    }


def bronze_rows(raw):
    from pyspark.sql import functions as F

    # A framing hint only: valid magic byte/header does not prove valid Avro.
    schema_id = F.expr("""CASE WHEN length(value) >= 5
        AND hex(substring(value, 1, 1)) = '00'
        THEN CAST(conv(hex(substring(value, 2, 4)), 16, 10) AS BIGINT)
        ELSE NULL END""")
    return raw.select(
        F.concat_ws(
            ":",
            F.col("topic"),
            F.col("partition").cast("string"),
            F.col("offset").cast("string"),
        ).alias("raw_record_id"),
        "topic",
        "partition",
        "offset",
        "key",
        "value",
        "headers",
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        schema_id.alias("schema_id"),
        F.current_timestamp().alias("ingested_at"),
    )


def start_bronze(spark, config, api_key, api_secret):
    # Schema must already exist. The table owns raw deliveries, not business events.
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {config.table} (
        raw_record_id STRING, topic STRING, partition INT, offset BIGINT,
        key BINARY, value BINARY, headers ARRAY<STRUCT<key: STRING, value: BINARY>>,
        kafka_timestamp TIMESTAMP, kafka_timestamp_type INT, schema_id BIGINT,
        ingested_at TIMESTAMP
    ) USING DELTA TBLPROPERTIES ('delta.appendOnly' = 'true')""")
    detail = spark.sql(f"DESCRIBE DETAIL {config.table}").first()
    if (
        detail["format"] != "delta"
        or detail["properties"].get("delta.appendOnly") != "true"
    ):
        raise ValueError("Target must be an append-only Delta table")
    raw = (
        spark.readStream.format("kafka")
        .options(**reader_options(config, api_key, api_secret))
        .load()
    )
    writer = (
        bronze_rows(raw)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint)
        .queryName("s01_" + config.table.replace(".", "_"))
    )
    if config.mode == "available_now":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="10 seconds")
    return writer.toTable(config.table)
