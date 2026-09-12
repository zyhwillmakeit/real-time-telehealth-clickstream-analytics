"""Deterministic delivery faults and Confluent Avro framing."""

import random
import struct
from copy import deepcopy
from datetime import timedelta
from io import BytesIO

from fastavro import schemaless_writer

from generator.customer_data import format_utc_timestamp, parse_utc_timestamp
from scripts.validate_contracts import normalize_timestamps, validate_event


def plan_deliveries(
    events,
    schema,
    rules,
    *,
    seed=6,
    duplicate_rate=0,
    delay_rate=0,
    reorder_rate=0,
    corrupt_rate=0,
    missing_rate=0,
    delay_seconds=2100,
    customer_ids=(),
):
    rates = (duplicate_rate, delay_rate, reorder_rate, corrupt_rate, missing_rate)
    if any(not 0 <= rate <= 1 for rate in rates) or delay_seconds < 0:
        raise ValueError("Rates must be within [0,1]; delay must be nonnegative")
    if not events or len({e["event_id"] for e in events}) != len(events):
        raise ValueError("Input must contain nonempty unique business events")
    for event in events:
        if validate_event(event, schema, rules):
            raise ValueError("Input violates V1 contract")
    rng = random.Random(seed)
    # Map a selected customer consistently across all its authenticated events.
    source_ids = set(customer_ids) | {
        e["customer_id"] for e in events if e["customer_id"]
    }
    missing = {}
    candidate = 99999999
    for customer in sorted({e["customer_id"] for e in events if e["customer_id"]}):
        if rng.random() < missing_rate:
            while f"cus_{candidate:08d}" in source_ids:
                candidate -= 1
            if candidate < 1:
                raise ValueError("No unused customer ID available")
            missing[customer] = f"cus_{candidate:08d}"
            candidate -= 1
    deliveries = []
    for original in events:
        event = deepcopy(original)
        flags = []
        if event["customer_id"] in missing:
            event["customer_id"] = missing[event["customer_id"]]
            flags.append("missing_customer")
        delay = delay_seconds if rng.random() < delay_rate else 0
        if delay:
            flags.append("delayed")
        jitter = rng.uniform(0, 300) if rng.random() < reorder_rate else 0
        if jitter:
            flags.append("reorder_candidate")
        arrival = parse_utc_timestamp(event["received_at"]) + timedelta(
            seconds=delay + jitter
        )
        event["received_at"] = format_utc_timestamp(arrival)
        corrupt = rng.random() < corrupt_rate
        if corrupt:
            flags.append("corrupt")
        record = {
            "event": event,
            "flags": flags,
            "corrupt": corrupt,
            "send_at": format_utc_timestamp(arrival),
            "attempt": 0,
        }
        deliveries.append(record)
        if rng.random() < duplicate_rate:
            retry = deepcopy(record)
            retry["attempt"] = 1
            retry["flags"].append("duplicate")
            retry["send_at"] = format_utc_timestamp(arrival + timedelta(seconds=1))
            deliveries.append(retry)
    deliveries.sort(key=lambda d: (d["send_at"], d["event"]["event_id"], d["attempt"]))
    for i, record in enumerate(deliveries):
        record["delivery_id"] = f"delivery-{i:08d}"
    return deliveries


def encode_delivery(record, schema, rules, schema_id):
    if not 0 < schema_id < 2**32:
        raise ValueError("Schema ID must be a positive uint32")
    if record["corrupt"]:
        return b"\x00" + struct.pack(">I", schema_id)  # Truncated Avro body.
    event, errors = normalize_timestamps(record["event"], rules["timestamp_fields"])
    if errors:
        raise ValueError("Invalid timestamp")
    output = BytesIO()
    output.write(b"\x00" + struct.pack(">I", schema_id))
    schemaless_writer(output, schema, event)
    return output.getvalue()


def partition_key(event):
    return (
        event["booking_journey_id"]
        or event["appointment_id"]
        or event["customer_id"]
        or event["anonymous_id"]
        or event["session_id"]
    )
