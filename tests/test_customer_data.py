import json
from collections import Counter
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from generator.customer_data import (
    ACCOUNT_STATUSES,
    ACQUISITION_CHANNELS,
    MEMBERSHIP_PLANS,
    REGIONS,
    apply_deterministic_updates,
    generate_customers,
    read_customer_csv,
    sha256_file,
    validate_customers,
    write_customer_csv,
)
from generator.customer_postgres import postgres_config

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_PATH = PROJECT_ROOT / "evidence" / "day-04" / "customer_snapshot_v1.csv"
MANIFEST_PATH = (
    PROJECT_ROOT / "evidence" / "day-04" / "customer_snapshot_v1.manifest.json"
)
SNAPSHOT_AT = datetime(2026, 9, 10, 23, 59, 59, tzinfo=timezone.utc)


def test_customer_generation_is_deterministic() -> None:
    first = generate_customers(count=250, seed=1234, snapshot_at=SNAPSHOT_AT)
    second = generate_customers(count=250, seed=1234, snapshot_at=SNAPSHOT_AT)
    different = generate_customers(count=250, seed=4321, snapshot_at=SNAPSHOT_AT)

    assert first == second
    assert first != different
    assert validate_customers(first, expected_count=250, snapshot_at=SNAPSHOT_AT) == []


def test_checked_in_snapshot_has_10000_valid_unique_customers() -> None:
    customers = read_customer_csv(SNAPSHOT_PATH)
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    assert (
        validate_customers(customers, expected_count=10_000, snapshot_at=SNAPSHOT_AT)
        == []
    )
    assert len({customer.customer_id for customer in customers}) == 10_000
    assert customers[0].customer_id == "cus_00000001"
    assert customers[-1].customer_id == "cus_00010000"
    assert manifest["row_count"] == 10_000
    assert manifest["unique_customer_count"] == 10_000
    assert manifest["non_deleted_row_count"] == 10_000
    assert manifest["deleted_row_count"] == 0
    assert manifest["sha256"] == sha256_file(SNAPSHOT_PATH)
    assert set(manifest["distributions"]["region"]) == set(REGIONS)
    assert set(manifest["distributions"]["membership_plan"]) == set(MEMBERSHIP_PLANS)
    assert set(manifest["distributions"]["acquisition_channel"]) == set(
        ACQUISITION_CHANNELS
    )
    assert set(manifest["distributions"]["account_status"]) == set(ACCOUNT_STATUSES)


def test_customer_csv_round_trip(tmp_path: Path) -> None:
    expected = generate_customers(count=25, seed=99, snapshot_at=SNAPSHOT_AT)
    output = tmp_path / "customers.csv"

    write_customer_csv(output, expected)

    assert read_customer_csv(output) == expected


def test_update_batch_is_deterministic_and_auditable() -> None:
    source = generate_customers(count=100, seed=1234, snapshot_at=SNAPSHOT_AT)
    effective_at = datetime(2026, 9, 11, 12, tzinfo=timezone.utc)
    arguments = {
        "seed": 5678,
        "effective_at": effective_at,
        "attribute_updates": 10,
        "soft_deletes": 5,
        "inserts": 3,
    }

    first_customers, first_changes = apply_deterministic_updates(source, **arguments)
    second_customers, second_changes = apply_deterministic_updates(source, **arguments)

    assert first_customers == second_customers
    assert first_changes == second_changes
    assert len(first_customers) == 103
    assert (
        validate_customers(
            first_customers, expected_count=103, snapshot_at=effective_at
        )
        == []
    )
    assert Counter(change.operation for change in first_changes) == {
        "update": 10,
        "soft_delete": 5,
        "insert": 3,
    }
    changed_ids = [change.customer_id for change in first_changes]
    assert len(changed_ids) == len(set(changed_ids))

    source_by_id = {customer.customer_id: customer for customer in source}
    result_by_id = {customer.customer_id: customer for customer in first_customers}
    for change in first_changes:
        if change.operation == "insert":
            continue
        before = source_by_id[change.customer_id]
        after = result_by_id[change.customer_id]
        assert after.customer_id == before.customer_id
        assert after.signup_date == before.signup_date
        assert after.created_at == before.created_at
        assert after.updated_at == effective_at


def test_validator_rejects_duplicate_and_out_of_domain_rows() -> None:
    customers = generate_customers(count=2, seed=1234, snapshot_at=SNAPSHOT_AT)
    invalid = replace(customers[0], region="central")

    errors = validate_customers([invalid, invalid], expected_count=2)

    assert "DUPLICATE_CUSTOMER_ID:cus_00000001" in errors
    assert "VALUE_NOT_ALLOWED:cus_00000001:region" in errors


def test_postgres_configuration_errors_name_keys_without_values() -> None:
    env = {"POSTGRES_HOST": "db.internal", "POSTGRES_PASSWORD": "secret-value"}

    try:
        postgres_config(env)
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected incomplete PostgreSQL configuration to fail")

    assert "POSTGRES_DATABASE" in message
    assert "POSTGRES_USER" in message
    assert "db.internal" not in message
    assert "secret-value" not in message
