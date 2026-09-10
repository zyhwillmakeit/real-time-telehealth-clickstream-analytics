"""Run redacted, read-only connectivity checks for the Day 2 environment gate."""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from dotenv import load_dotenv

SERVICES = (
    "kafka",
    "schema_registry",
    "postgres",
    "databricks_workspace",
    "sql_warehouse",
    "object_storage",
)


@dataclass(frozen=True)
class Result:
    service: str
    status: str
    check: str
    detail: str


def required_values(env: dict[str, str], keys: tuple[str, ...]) -> list[str]:
    """Return missing key names without exposing values."""
    return [key for key in keys if not env.get(key)]


def blocked(service: str, keys: list[str]) -> Result:
    return Result(service, "BLOCKED", "configuration", f"Missing: {', '.join(keys)}")


def failed(service: str, exc: Exception) -> Result:
    # Exception text can echo URLs, usernames, or driver connection strings.
    return Result(service, "FAILED", "read-only query", exc.__class__.__name__)


def check_kafka(env: dict[str, str]) -> Result:
    keys = ["KAFKA_BOOTSTRAP_SERVERS"]
    protocol = env.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    if protocol != "PLAINTEXT":
        keys.extend(["KAFKA_API_KEY", "KAFKA_API_SECRET"])
    if missing := required_values(env, tuple(keys)):
        return blocked("kafka", missing)

    try:
        from confluent_kafka.admin import AdminClient

        config = {
            "bootstrap.servers": env["KAFKA_BOOTSTRAP_SERVERS"],
            "security.protocol": protocol,
        }
        if protocol != "PLAINTEXT":
            config.update(
                {
                    "sasl.mechanism": env.get("KAFKA_SASL_MECHANISM", "PLAIN"),
                    "sasl.username": env["KAFKA_API_KEY"],
                    "sasl.password": env["KAFKA_API_SECRET"],
                }
            )
        metadata = AdminClient(config).list_topics(timeout=10)
        topic = env.get("KAFKA_TOPIC", "telehealth-clickstream")
        detail = "Broker metadata read; topic exists" if topic in metadata.topics else "Broker metadata read; topic not created yet"
        return Result("kafka", "PASS", "broker metadata", detail)
    except Exception as exc:  # noqa: BLE001 - normalize third-party driver failures
        return failed("kafka", exc)


def check_schema_registry(env: dict[str, str]) -> Result:
    if missing := required_values(env, ("SCHEMA_REGISTRY_URL",)):
        return blocked("schema_registry", missing)

    try:
        import requests

        auth = None
        if env.get("SCHEMA_REGISTRY_API_KEY") or env.get("SCHEMA_REGISTRY_API_SECRET"):
            missing = required_values(env, ("SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET"))
            if missing:
                return blocked("schema_registry", missing)
            auth = (env["SCHEMA_REGISTRY_API_KEY"], env["SCHEMA_REGISTRY_API_SECRET"])
        response = requests.get(f"{env['SCHEMA_REGISTRY_URL'].rstrip('/')}/subjects", auth=auth, timeout=10)
        response.raise_for_status()
        subjects = response.json()
        if not isinstance(subjects, list):
            raise TypeError("Unexpected registry response type")
        return Result("schema_registry", "PASS", "GET /subjects", f"Read {len(subjects)} subject(s)")
    except Exception as exc:  # noqa: BLE001 - normalize third-party driver failures
        return failed("schema_registry", exc)


def check_postgres(env: dict[str, str]) -> Result:
    keys = ("POSTGRES_HOST", "POSTGRES_DATABASE", "POSTGRES_USER", "POSTGRES_PASSWORD")
    if missing := required_values(env, keys):
        return blocked("postgres", missing)

    try:
        import psycopg

        with psycopg.connect(
            host=env["POSTGRES_HOST"],
            port=int(env.get("POSTGRES_PORT", "5432")),
            dbname=env["POSTGRES_DATABASE"],
            user=env["POSTGRES_USER"],
            password=env["POSTGRES_PASSWORD"],
            sslmode=env.get("POSTGRES_SSLMODE", "require"),
            connect_timeout=10,
        ) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            if cursor.fetchone() != (1,):
                raise ValueError("Unexpected SELECT 1 result")
        return Result("postgres", "PASS", "SELECT 1", "Read-only query succeeded")
    except Exception as exc:  # noqa: BLE001 - normalize third-party driver failures
        return failed("postgres", exc)


def check_databricks_workspace(env: dict[str, str]) -> Result:
    keys = ("DATABRICKS_HOST", "DATABRICKS_TOKEN")
    if missing := required_values(env, keys):
        return blocked("databricks_workspace", missing)

    try:
        from databricks.sdk import WorkspaceClient

        current_user = WorkspaceClient(host=env["DATABRICKS_HOST"], token=env["DATABRICKS_TOKEN"]).current_user.me()
        if not current_user.id:
            raise ValueError("Workspace returned no current-user ID")
        return Result("databricks_workspace", "PASS", "current user", "Workspace API authentication succeeded")
    except Exception as exc:  # noqa: BLE001 - normalize third-party driver failures
        return failed("databricks_workspace", exc)


def check_sql_warehouse(env: dict[str, str]) -> Result:
    keys = ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")
    if missing := required_values(env, keys):
        return blocked("sql_warehouse", missing)

    try:
        from databricks import sql

        server_hostname = env["DATABRICKS_HOST"].removeprefix("https://").removeprefix("http://").rstrip("/")
        with sql.connect(
            server_hostname=server_hostname,
            http_path=env["DATABRICKS_HTTP_PATH"],
            access_token=env["DATABRICKS_TOKEN"],
        ) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            if cursor.fetchone()[0] != 1:
                raise ValueError("Unexpected SELECT 1 result")
        return Result("sql_warehouse", "PASS", "SELECT 1", "Warehouse query succeeded")
    except Exception as exc:  # noqa: BLE001 - normalize third-party driver failures
        return failed("sql_warehouse", exc)


def check_object_storage(env: dict[str, str]) -> Result:
    keys = ("AWS_REGION", "S3_BUCKET")
    if missing := required_values(env, keys):
        return blocked("object_storage", missing)

    try:
        import boto3

        client = boto3.client(
            "s3",
            region_name=env["AWS_REGION"],
            endpoint_url=env.get("S3_ENDPOINT_URL") or None,
        )
        client.head_bucket(Bucket=env["S3_BUCKET"])
        return Result("object_storage", "PASS", "HeadBucket", "Bucket is reachable with current credential chain")
    except Exception as exc:  # noqa: BLE001 - normalize third-party driver failures
        return failed("object_storage", exc)


CHECKS: dict[str, Callable[[dict[str, str]], Result]] = {
    "kafka": check_kafka,
    "schema_registry": check_schema_registry,
    "postgres": check_postgres,
    "databricks_workspace": check_databricks_workspace,
    "sql_warehouse": check_sql_warehouse,
    "object_storage": check_object_storage,
}


def dependency_versions() -> dict[str, str]:
    names = (
        "confluent-kafka",
        "fastavro",
        "psycopg",
        "databricks-sdk",
        "databricks-sql-connector",
        "dbt-core",
        "dbt-databricks",
        "boto3",
    )
    values: dict[str, str] = {}
    for name in names:
        try:
            values[name] = version(name)
        except PackageNotFoundError:
            values[name] = "MISSING"
    return values


def exit_code(results: list[Result]) -> int:
    if any(result.status == "FAILED" for result in results):
        return 1
    if any(result.status == "BLOCKED" for result in results):
        return 2
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env", help="Path to an ignored dotenv file")
    parser.add_argument("--service", choices=("all", *SERVICES), default="all")
    parser.add_argument("--output", help="Optional JSON report path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv(args.env_file, override=False)
    env = dict(os.environ)
    selected = SERVICES if args.service == "all" else (args.service,)
    results = [CHECKS[name](env) for name in selected]
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "python": platform.python_version(),
        "dependencies": dependency_versions(),
        "results": [asdict(result) for result in results],
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    if args.output:
        Path(args.output).write_text(f"{rendered}\n", encoding="utf-8")
    return exit_code(results)


if __name__ == "__main__":
    sys.exit(main())
