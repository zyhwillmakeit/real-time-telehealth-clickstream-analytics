"""Bounded producer delivery and authenticated Registry registration."""

import json
import logging
import time
from urllib.parse import quote

import requests

from generator.delivery import encode_delivery, partition_key


def kafka_config(env):
    required = ("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET")
    if missing := [key for key in required if not env.get(key)]:
        raise ValueError("Missing settings: " + ", ".join(missing))
    if env.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL") != "SASL_SSL":
        raise ValueError("This cloud producer supports SASL_SSL only")
    return {
        "bootstrap.servers": env["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": env.get("KAFKA_SASL_MECHANISM", "PLAIN"),
        "sasl.username": env["KAFKA_API_KEY"],
        "sasl.password": env["KAFKA_API_SECRET"],
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": 30000,
        "allow.auto.create.topics": False,
    }


def register_schema(env, topic, schema):
    required = (
        "SCHEMA_REGISTRY_URL",
        "SCHEMA_REGISTRY_API_KEY",
        "SCHEMA_REGISTRY_API_SECRET",
    )
    if missing := [key for key in required if not env.get(key)]:
        raise ValueError("Missing settings: " + ", ".join(missing))
    url = env["SCHEMA_REGISTRY_URL"].rstrip("/")
    if not url.startswith("https://"):
        raise ValueError("Schema Registry must use HTTPS")
    subject = quote(topic + "-value", safe="")
    with requests.Session() as session:
        session.auth = (
            env["SCHEMA_REGISTRY_API_KEY"],
            env["SCHEMA_REGISTRY_API_SECRET"],
        )
        # Explicit subject policy, never a global Registry policy change.
        response = session.put(
            url + "/config/" + subject,
            json={"compatibility": "BACKWARD_TRANSITIVE"},
            timeout=15,
            allow_redirects=False,
        )
        if response.status_code != 200:
            raise RuntimeError(f"Registry configuration HTTP {response.status_code}")
        response = session.post(
            url + "/subjects/" + subject + "/versions",
            json={"schemaType": "AVRO", "schema": json.dumps(schema)},
            timeout=15,
            allow_redirects=False,
        )
        if response.status_code != 200:
            raise RuntimeError(f"Registry registration HTTP {response.status_code}")
        schema_id = response.json()["id"]
        if type(schema_id) is not int or not 0 < schema_id < 2**32:
            raise ValueError("Registry returned invalid schema ID")
        return schema_id


def send_records(producer, topic, records, schema, rules, schema_id, *, rate=100):
    if rate <= 0:
        raise ValueError("Rate must be positive")
    results = []

    def callback(record):
        def delivered(error, message):
            row = {
                "delivery_id": record["delivery_id"],
                "event_id": record["event"]["event_id"],
                "status": "FAILED" if error else "ACKNOWLEDGED",
            }
            if error:
                row["error_code"] = error.code()
            else:
                row.update(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                )
            results.append(row)

        return delivered

    submission_error = None
    started = time.monotonic()
    try:
        for i, record in enumerate(records):
            wait = started + i / rate - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            deadline = time.monotonic() + 30
            while True:
                try:
                    producer.produce(
                        topic,
                        key=partition_key(record["event"]).encode(),
                        value=encode_delivery(record, schema, rules, schema_id),
                        headers={"delivery_id": record["delivery_id"].encode()},
                        on_delivery=callback(record),
                    )
                    break
                except BufferError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Producer queue stayed full") from None
                    producer.poll(0.1)
            producer.poll(0)
    except Exception as exc:  # noqa: BLE001 - preserve partial delivery evidence
        submission_error = type(exc).__name__
    remaining = producer.flush(35)
    acknowledged = sum(r["status"] == "ACKNOWLEDGED" for r in results)
    return {
        "status": "PASS"
        if acknowledged == len(records) and not remaining and not submission_error
        else "FAILED",
        "planned": len(records),
        "acknowledged": acknowledged,
        "remaining": remaining,
        "submission_error": submission_error,
        "deliveries": results,
    }


def make_producer(config):
    from confluent_kafka import Producer

    logger = logging.getLogger("redacted-kafka")
    logger.propagate = False
    logger.addHandler(logging.NullHandler())
    return Producer(config, logger=logger)
