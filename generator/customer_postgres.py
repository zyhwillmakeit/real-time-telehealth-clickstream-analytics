"""Transactional PostgreSQL loading for complete customer snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from generator.customer_data import Customer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DDL_PATH = PROJECT_ROOT / "contracts" / "sql" / "customer_master.sql"
REQUIRED_POSTGRES_SETTINGS = (
    "POSTGRES_HOST",
    "POSTGRES_DATABASE",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
)

UPSERT_SQL = """
INSERT INTO source.customer_master (
    customer_id,
    signup_date,
    region,
    membership_plan,
    acquisition_channel,
    account_status,
    created_at,
    updated_at,
    is_deleted
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (customer_id) DO UPDATE SET
    signup_date = EXCLUDED.signup_date,
    region = EXCLUDED.region,
    membership_plan = EXCLUDED.membership_plan,
    acquisition_channel = EXCLUDED.acquisition_channel,
    account_status = EXCLUDED.account_status,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    is_deleted = EXCLUDED.is_deleted
WHERE source.customer_master.updated_at <= EXCLUDED.updated_at
"""


def postgres_config(env: Mapping[str, str]) -> dict[str, object]:
    missing = [key for key in REQUIRED_POSTGRES_SETTINGS if not env.get(key)]
    if missing:
        raise ValueError(f"Missing PostgreSQL settings: {', '.join(missing)}")
    return {
        "host": env["POSTGRES_HOST"],
        "port": int(env.get("POSTGRES_PORT", "5432")),
        "dbname": env["POSTGRES_DATABASE"],
        "user": env["POSTGRES_USER"],
        "password": env["POSTGRES_PASSWORD"],
        "sslmode": env.get("POSTGRES_SSLMODE", "require"),
        "connect_timeout": 10,
    }


def _database_row(customer: Customer) -> tuple[object, ...]:
    return (
        customer.customer_id,
        customer.signup_date,
        customer.region,
        customer.membership_plan,
        customer.acquisition_channel,
        customer.account_status,
        customer.created_at,
        customer.updated_at,
        customer.is_deleted,
    )


def load_customer_snapshot(
    customers: list[Customer],
    env: Mapping[str, str],
    *,
    apply_ddl: bool,
) -> dict[str, int]:
    """Upsert one full snapshot atomically and return redacted table counts."""
    import psycopg

    with psycopg.connect(**postgres_config(env)) as connection:
        if apply_ddl:
            connection.execute(DDL_PATH.read_text(encoding="utf-8"), prepare=False)
        with connection.cursor() as cursor:
            cursor.executemany(UPSERT_SQL, map(_database_row, customers))
            cursor.execute(
                """
                SELECT
                    COUNT(*)::INTEGER,
                    COUNT(*) FILTER (WHERE NOT is_deleted)::INTEGER,
                    COUNT(*) FILTER (WHERE is_deleted)::INTEGER
                FROM source.customer_master
                """
            )
            total, active, deleted = cursor.fetchone()
    return {
        "table_rows": total,
        "non_deleted_rows": active,
        "deleted_rows": deleted,
    }
