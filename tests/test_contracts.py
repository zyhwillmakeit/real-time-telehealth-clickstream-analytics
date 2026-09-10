from pathlib import Path

from scripts.validate_contracts import (
    INVALID_FIXTURES,
    VALID_FIXTURES,
    load_contract,
    load_json,
    schema_event_names,
    validate_contract_files,
    validate_event,
)


def test_schema_and_rules_cover_the_same_event_names() -> None:
    schema, rules = load_contract()

    assert schema_event_names(schema) == set(rules["event_required_fields"])
    assert len(schema_event_names(schema)) == 10


def test_all_checked_in_contract_fixtures_match_expected_results() -> None:
    assert validate_contract_files() == []
    assert len(list(VALID_FIXTURES.glob("*.json"))) == 3
    assert len(list(INVALID_FIXTURES.glob("*.json"))) == 7


def test_retry_preserves_business_event_identity() -> None:
    schema, rules = load_contract()
    event = load_json(VALID_FIXTURES / "appointment_booked.json")
    retry = event.copy()

    assert retry["event_id"] == event["event_id"]
    assert validate_event(retry, schema, rules) == []


def test_unknown_fields_fail_closed() -> None:
    schema, rules = load_contract()
    event = load_json(VALID_FIXTURES / "provider_profile_viewed.json")
    event["diagnosis"] = "must-not-enter-analytics-contract"

    assert validate_event(event, schema, rules) == ["AVRO_SCHEMA_INVALID"]


def test_customer_ddl_contains_source_controls() -> None:
    ddl = Path("contracts/sql/customer_master.sql").read_text(encoding="utf-8")

    assert "PRIMARY KEY" in ddl
    assert "is_deleted BOOLEAN NOT NULL" in ddl
    assert "updated_at TIMESTAMPTZ NOT NULL" in ddl
    assert "clinical" in ddl
