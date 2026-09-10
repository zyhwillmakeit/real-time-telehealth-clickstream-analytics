"""Causal synthetic journeys with an independent scenario ledger."""

import hashlib
import random
from collections import Counter
from datetime import timedelta

from generator.customer_data import format_utc_timestamp as stamp
from generator.customer_data import require_utc, validate_customers

SCENARIOS = (
    "profile_abandoned",
    "slot_abandoned",
    "booking_abandoned",
    "completed",
    "connection_failed",
    "no_show",
    "started_unfinished",
)
BOOKING = (
    "specialty_searched",
    "provider_profile_viewed",
    "appointment_slot_viewed",
    "booking_started",
    "appointment_booked",
)
VISIT = ("checkin_started", "waiting_room_entered", "video_visit_started")


def identifier(prefix, seed, *parts):
    payload = ":".join(map(str, (seed, *parts)))
    return prefix + "_" + hashlib.sha256(payload.encode()).hexdigest()[:32]


def generate_journeys(customers, *, count, seed, start_at, as_of):
    """Plan outcomes first; emit only events collected by the observation boundary.

    Scenarios cycle for guaranteed coverage. Randomness controls customer selection,
    context and appointment delay, not business outcome prevalence.
    """
    start_at, as_of = require_utc(start_at), require_utc(as_of)
    if count <= 0 or as_of < start_at + timedelta(minutes=count - 1, seconds=242):
        raise ValueError("Positive count and as_of after all booking attempts required")
    errors = validate_customers(customers)
    if errors:
        raise ValueError(f"Invalid customer source: {errors[:3]}")
    eligible = [
        c
        for c in customers
        if not c.is_deleted
        and c.account_status == "active"
        and c.created_at <= start_at
    ]
    if not eligible:
        raise ValueError("No active customers exist at simulation start")
    rng = random.Random(seed)
    events, journeys, appointments = [], [], []
    expected_counts = Counter()
    for i in range(count):
        customer = rng.choice(eligible)
        scenario = SCENARIOS[i % len(SCENARIOS)]
        journey_id = identifier("bkj", seed, i)
        session = identifier("ses", seed, i, "booking")
        visit_session = identifier("ses", seed, i, "visit")
        anonymous = identifier("anon", seed, customer.customer_id)
        at = start_at + timedelta(minutes=i)
        booked = scenario not in SCENARIOS[:3]
        appointment_id = identifier("apt", seed, i) if booked else None
        scheduled = at + timedelta(days=rng.randint(1, 3), hours=1) if booked else None
        length = {
            "profile_abandoned": 2,
            "slot_abandoned": 3,
            "booking_abandoned": 4,
        }.get(scenario, 5)
        # Each entry is an intended business action, before transport simulation.
        plan = [
            (name, at + timedelta(minutes=j), session)
            for j, name in enumerate(BOOKING[:length])
        ]
        if booked and scenario != "no_show":
            plan.extend(
                [
                    (VISIT[0], scheduled - timedelta(minutes=5), visit_session),
                    (VISIT[1], scheduled - timedelta(minutes=2), visit_session),
                ]
            )
            if scenario == "connection_failed":
                plan.append(("video_connection_failed", scheduled, visit_session))
            else:
                plan.append((VISIT[2], scheduled, visit_session))
                if scenario == "completed":
                    plan.append(
                        (
                            "visit_completed",
                            scheduled + timedelta(minutes=30),
                            visit_session,
                        )
                    )
        device = rng.choice(("desktop", "mobile", "tablet"))
        platform = (
            "web" if device == "desktop" else rng.choice(("web", "ios", "android"))
        )
        provider = f"prv_{rng.randint(1, 500):06d}"
        specialty = rng.choice(
            ("dermatology", "mental_health", "pediatrics", "primary_care")
        )
        visible = [
            (name, time, ses)
            for name, time, ses in plan
            if time + timedelta(seconds=2) <= as_of
        ]
        expected_counts.update(name for name, _, _ in visible)
        journeys.append(
            {
                "booking_journey_id": journey_id,
                "customer_id": customer.customer_id,
                "scenario": scenario,
                "booked": booked,
                "booking_started": length >= 4,
                "appointment_id": appointment_id,
                "cohort_date": (at + timedelta(minutes=1)).date().isoformat(),
            }
        )
        if booked:
            names = {name for name, _, _ in visible}
            mature = as_of >= scheduled + timedelta(hours=24)
            appointments.append(
                {
                    "appointment_id": appointment_id,
                    "booking_journey_id": journey_id,
                    "customer_id": customer.customer_id,
                    "scheduled_start_time": stamp(scheduled),
                    "observation_deadline": stamp(scheduled + timedelta(hours=24)),
                    "mature": mature,
                    "started": "video_visit_started" in names,
                    "completed": "visit_completed" in names,
                    "status": (
                        "observing"
                        if not mature
                        else "completed"
                        if "visit_completed" in names
                        else "incomplete"
                    ),
                }
            )
        for j, (name, time, ses) in enumerate(visible):
            has_appointment = name == "appointment_booked" or ses == visit_session
            events.append(
                {
                    "event_id": identifier("evt", seed, i, j),
                    "event_name": name,
                    "event_time": stamp(time),
                    "received_at": stamp(time + timedelta(seconds=2)),
                    "anonymous_id": anonymous,
                    "customer_id": customer.customer_id
                    if name not in BOOKING[:3]
                    else None,
                    "session_id": ses,
                    "booking_journey_id": journey_id if j else None,
                    "appointment_id": appointment_id if has_appointment else None,
                    "scheduled_start_time": stamp(scheduled)
                    if has_appointment
                    else None,
                    "provider_id": provider if j else None,
                    "device_type": "desktop" if ses == visit_session else device,
                    "platform": "web" if ses == visit_session else platform,
                    "specialty": specialty,
                    "schema_version": 1,
                }
            )
    events.sort(key=lambda e: (e["received_at"], e["event_id"]))
    mature = [a for a in appointments if a["mature"]]
    manifest = {
        "seed": seed,
        "start_at": stamp(start_at),
        "as_of": stamp(as_of),
        "scenario_policy": "cycle_seven_acceptance_scenarios",
        "expected_event_counts": dict(sorted(expected_counts.items())),
        "expected_unique_events": sum(expected_counts.values()),
        "booking_funnel": {
            "provider_view": count,
            "booking_started": sum(j["booking_started"] for j in journeys),
            "appointment_booked": len(appointments),
        },
        "mature_appointments": len(mature),
        "mature_completed_appointments": sum(a["completed"] for a in mature),
        "observing_appointments": len(appointments) - len(mature),
        "journeys": journeys,
        "appointments": appointments,
    }
    return events, manifest
