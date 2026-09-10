from collections import Counter
from datetime import timedelta

import pytest

from generator.customer_data import generate_customers, parse_utc_timestamp
from generator.journeys import generate_journeys
from scripts.validate_contracts import load_contract, validate_event

START = parse_utc_timestamp("2026-09-12T00:00:00Z")


def run(as_of=START + timedelta(days=5), count=70):
    customers = generate_customers(count=100, seed=1, snapshot_at=START)
    return generate_journeys(
        customers, count=count, seed=42, start_at=START, as_of=as_of
    )


def test_determinism_and_contract():
    events, manifest = run()
    assert (events, manifest) == run()
    schema, rules = load_contract()
    assert all(not validate_event(e, schema, rules) for e in events)
    assert len({e["event_id"] for e in events}) == len(events)
    assert Counter(e["event_name"] for e in events) == manifest["expected_event_counts"]
    assert {e["event_name"] for e in events} == set(rules["event_required_fields"])
    assert manifest["booking_funnel"] == {
        "provider_view": 70,
        "booking_started": 50,
        "appointment_booked": 40,
    }
    assert manifest["mature_appointments"] == 40
    assert manifest["mature_completed_appointments"] == 10


def test_causal_order_identity_and_cross_day_sessions():
    events, manifest = run()
    for appointment in manifest["appointments"]:
        rows = [
            e for e in events if e["appointment_id"] == appointment["appointment_id"]
        ]
        booking = rows[0]
        assert booking["event_name"] == "appointment_booked"
        for visit in rows[1:]:
            assert visit["session_id"] != booking["session_id"]
            assert visit["event_time"][:10] > booking["event_time"][:10]
            for field in (
                "customer_id",
                "provider_id",
                "specialty",
                "booking_journey_id",
                "scheduled_start_time",
            ):
                assert visit[field] == booking[field]
    for journey in manifest["journeys"]:
        names = [
            e["event_name"]
            for e in events
            if e["booking_journey_id"] == journey["booking_journey_id"]
        ]
        assert names[0] == "provider_profile_viewed"
        if "appointment_booked" in names:
            assert names.index("booking_started") < names.index("appointment_booked")


def test_cutoff_excludes_future_and_preserves_identity():
    early, early_manifest = run(START + timedelta(hours=12))
    late, _ = run()
    assert early_manifest["observing_appointments"] == 40
    assert early_manifest["mature_appointments"] == 0
    assert not any(e["event_name"] == "visit_completed" for e in early)
    by_id = {e["event_id"]: e for e in late}
    assert all(e == by_id[e["event_id"]] for e in early)
    assert all(e["received_at"] <= early_manifest["as_of"] for e in early)


def test_invalid_inputs():
    with pytest.raises(ValueError):
        run(count=0)
    with pytest.raises(ValueError):
        run(as_of=START)
    with pytest.raises(ValueError):
        generate_journeys(
            [], count=7, seed=1, start_at=START, as_of=START + timedelta(days=5)
        )
