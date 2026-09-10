"""Deterministic customer snapshots shared by local tools and future pipelines."""

from __future__ import annotations

import csv
import hashlib
import json
import random
import re
from collections import Counter
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

CUSTOMER_COLUMNS = (
    "customer_id",
    "signup_date",
    "region",
    "membership_plan",
    "acquisition_channel",
    "account_status",
    "created_at",
    "updated_at",
    "is_deleted",
)

REGIONS = ("midwest", "northeast", "south", "west")
MEMBERSHIP_PLANS = ("basic", "premium")
ACQUISITION_CHANNELS = ("employer", "organic", "paid_search", "referral")
ACCOUNT_STATUSES = ("active", "closed", "suspended")

CUSTOMER_ID_PATTERN = re.compile(r"^cus_[0-9]{8}$")
EARLIEST_SIGNUP_DATE = date(2021, 1, 1)
UTC = timezone.utc


@dataclass(frozen=True)
class Customer:
    customer_id: str
    signup_date: date
    region: str
    membership_plan: str
    acquisition_channel: str
    account_status: str
    created_at: datetime
    updated_at: datetime
    is_deleted: bool

    def csv_row(self) -> dict[str, str]:
        return {
            "customer_id": self.customer_id,
            "signup_date": self.signup_date.isoformat(),
            "region": self.region,
            "membership_plan": self.membership_plan,
            "acquisition_channel": self.acquisition_channel,
            "account_status": self.account_status,
            "created_at": format_utc_timestamp(self.created_at),
            "updated_at": format_utc_timestamp(self.updated_at),
            "is_deleted": str(self.is_deleted).lower(),
        }


@dataclass(frozen=True)
class CustomerChange:
    operation: str
    customer_id: str
    field: str
    old_value: str
    new_value: str
    effective_at: datetime

    def csv_row(self) -> dict[str, str]:
        row = asdict(self)
        row["effective_at"] = format_utc_timestamp(self.effective_at)
        return row


def require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("Timestamp must include a UTC offset")
    return value.astimezone(UTC)


def parse_utc_timestamp(value: str) -> datetime:
    return require_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def format_utc_timestamp(value: datetime) -> str:
    return require_utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def weighted_choice(
    rng: random.Random, values: tuple[str, ...], weights: tuple[int, ...]
) -> str:
    if (
        len(values) != len(weights)
        or not values
        or any(weight <= 0 for weight in weights)
    ):
        raise ValueError(
            "Weighted choices require matching values and positive weights"
        )
    target = rng.randrange(sum(weights))
    cumulative = 0
    for value, weight in zip(values, weights, strict=True):
        cumulative += weight
        if target < cumulative:
            return value
    raise AssertionError("Weighted choice did not select a value")


def _created_at(
    rng: random.Random, signup_date: date, snapshot_at: datetime
) -> datetime:
    last_second = 86_399
    if signup_date == snapshot_at.date():
        last_second = (
            snapshot_at.hour * 3_600 + snapshot_at.minute * 60 + snapshot_at.second
        )
    seconds = rng.randint(0, last_second)
    return datetime.combine(signup_date, time.min, tzinfo=UTC) + timedelta(
        seconds=seconds
    )


def generate_customers(
    *, count: int, seed: int, snapshot_at: datetime, start_index: int = 1
) -> list[Customer]:
    """Generate a canonical customer snapshot independent of wall-clock time."""
    if count <= 0:
        raise ValueError("Customer count must be positive")
    if start_index <= 0 or start_index + count - 1 > 99_999_999:
        raise ValueError("Customer ID range must fit cus_########")

    snapshot_at = require_utc(snapshot_at)
    if snapshot_at.date() < EARLIEST_SIGNUP_DATE:
        raise ValueError("Snapshot precedes the earliest supported signup date")

    rng = random.Random(seed)
    signup_window_days = (snapshot_at.date() - EARLIEST_SIGNUP_DATE).days
    customers: list[Customer] = []
    for customer_number in range(start_index, start_index + count):
        signup_date = EARLIEST_SIGNUP_DATE + timedelta(
            days=rng.randint(0, signup_window_days)
        )
        created_at = _created_at(rng, signup_date, snapshot_at)
        customers.append(
            Customer(
                customer_id=f"cus_{customer_number:08d}",
                signup_date=signup_date,
                region=weighted_choice(rng, REGIONS, (23, 18, 37, 22)),
                membership_plan=weighted_choice(rng, MEMBERSHIP_PLANS, (68, 32)),
                acquisition_channel=weighted_choice(
                    rng, ACQUISITION_CHANNELS, (22, 42, 21, 15)
                ),
                account_status=weighted_choice(rng, ACCOUNT_STATUSES, (96, 1, 3)),
                created_at=created_at,
                updated_at=created_at,
                is_deleted=False,
            )
        )
    return customers


def _different_value(rng: random.Random, current: str, allowed: tuple[str, ...]) -> str:
    candidates = tuple(value for value in allowed if value != current)
    return candidates[rng.randrange(len(candidates))]


def apply_deterministic_updates(
    customers: list[Customer],
    *,
    seed: int,
    effective_at: datetime,
    attribute_updates: int,
    soft_deletes: int,
    inserts: int,
) -> tuple[list[Customer], list[CustomerChange]]:
    """Create a new full snapshot plus an auditable deterministic change set."""
    effective_at = require_utc(effective_at)
    if min(attribute_updates, soft_deletes, inserts) < 0:
        raise ValueError("Update counts cannot be negative")

    active = [customer for customer in customers if not customer.is_deleted]
    if attribute_updates + soft_deletes > len(active):
        raise ValueError("Requested changes exceed active customers")
    if any(customer.updated_at > effective_at for customer in customers):
        raise ValueError("Effective time precedes existing customer state")

    rng = random.Random(seed)
    selected = rng.sample(active, attribute_updates + soft_deletes)
    attribute_customers = selected[:attribute_updates]
    deleted_customers = selected[attribute_updates:]
    by_id = {customer.customer_id: customer for customer in customers}
    changes: list[CustomerChange] = []

    attribute_fields: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("region", REGIONS),
        ("membership_plan", MEMBERSHIP_PLANS),
        ("acquisition_channel", ACQUISITION_CHANNELS),
        ("account_status", ACCOUNT_STATUSES),
    )
    for customer in attribute_customers:
        field, allowed = attribute_fields[rng.randrange(len(attribute_fields))]
        old_value = str(getattr(customer, field))
        new_value = _different_value(rng, old_value, allowed)
        by_id[customer.customer_id] = replace(
            customer, **{field: new_value, "updated_at": effective_at}
        )
        changes.append(
            CustomerChange(
                operation="update",
                customer_id=customer.customer_id,
                field=field,
                old_value=old_value,
                new_value=new_value,
                effective_at=effective_at,
            )
        )

    for customer in deleted_customers:
        by_id[customer.customer_id] = replace(
            customer,
            account_status="closed",
            updated_at=effective_at,
            is_deleted=True,
        )
        changes.append(
            CustomerChange(
                operation="soft_delete",
                customer_id=customer.customer_id,
                field="account_status,is_deleted",
                old_value=f"{customer.account_status},false",
                new_value="closed,true",
                effective_at=effective_at,
            )
        )

    max_customer_number = max(
        int(customer.customer_id.removeprefix("cus_")) for customer in customers
    )
    if max_customer_number + inserts > 99_999_999:
        raise ValueError("Inserted customer IDs exceed cus_########")

    insert_rng = random.Random(seed + 1)
    for customer_number in range(
        max_customer_number + 1, max_customer_number + inserts + 1
    ):
        customer = Customer(
            customer_id=f"cus_{customer_number:08d}",
            signup_date=effective_at.date(),
            region=weighted_choice(insert_rng, REGIONS, (23, 18, 37, 22)),
            membership_plan=weighted_choice(insert_rng, MEMBERSHIP_PLANS, (68, 32)),
            acquisition_channel=weighted_choice(
                insert_rng, ACQUISITION_CHANNELS, (22, 42, 21, 15)
            ),
            account_status="active",
            created_at=effective_at,
            updated_at=effective_at,
            is_deleted=False,
        )
        by_id[customer.customer_id] = customer
        changes.append(
            CustomerChange(
                operation="insert",
                customer_id=customer.customer_id,
                field="*",
                old_value="",
                new_value="created",
                effective_at=effective_at,
            )
        )

    return sorted(by_id.values(), key=lambda row: row.customer_id), sorted(
        changes, key=lambda row: (row.customer_id, row.operation)
    )


def validate_customers(
    customers: list[Customer],
    *,
    expected_count: int | None = None,
    snapshot_at: datetime | None = None,
) -> list[str]:
    errors: list[str] = []
    if expected_count is not None and len(customers) != expected_count:
        errors.append(
            f"ROW_COUNT_INVALID:expected={expected_count}:actual={len(customers)}"
        )
    if not customers:
        return sorted({*errors, "SNAPSHOT_EMPTY"})

    customer_ids = [customer.customer_id for customer in customers]
    duplicate_ids = sorted(
        customer_id for customer_id, count in Counter(customer_ids).items() if count > 1
    )
    if duplicate_ids:
        errors.append(f"DUPLICATE_CUSTOMER_ID:{duplicate_ids[0]}")
    if customer_ids != sorted(customer_ids):
        errors.append("CUSTOMER_ORDER_INVALID")

    normalized_snapshot_at = require_utc(snapshot_at) if snapshot_at else None
    domains = {
        "region": REGIONS,
        "membership_plan": MEMBERSHIP_PLANS,
        "acquisition_channel": ACQUISITION_CHANNELS,
        "account_status": ACCOUNT_STATUSES,
    }
    for customer in customers:
        prefix = customer.customer_id
        if not CUSTOMER_ID_PATTERN.fullmatch(customer.customer_id):
            errors.append(f"CUSTOMER_ID_INVALID:{prefix}")
        for field, allowed in domains.items():
            if getattr(customer, field) not in allowed:
                errors.append(f"VALUE_NOT_ALLOWED:{prefix}:{field}")
        try:
            created_at = require_utc(customer.created_at)
            updated_at = require_utc(customer.updated_at)
        except ValueError:
            errors.append(f"TIMESTAMP_NOT_UTC:{prefix}")
            continue
        if customer.signup_date < EARLIEST_SIGNUP_DATE:
            errors.append(f"SIGNUP_DATE_TOO_EARLY:{prefix}")
        if created_at.date() != customer.signup_date:
            errors.append(f"CREATED_AT_SIGNUP_MISMATCH:{prefix}")
        if updated_at < created_at:
            errors.append(f"UPDATE_BEFORE_CREATE:{prefix}")
        if normalized_snapshot_at and updated_at > normalized_snapshot_at:
            errors.append(f"UPDATE_AFTER_SNAPSHOT:{prefix}")
        if customer.is_deleted and customer.account_status != "closed":
            errors.append(f"DELETED_CUSTOMER_NOT_CLOSED:{prefix}")
    return sorted(set(errors))


def write_customer_csv(path: Path, customers: list[Customer]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=CUSTOMER_COLUMNS, lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(customer.csv_row() for customer in customers)
    temporary.replace(path)


def read_customer_csv(path: Path) -> list[Customer]:
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != CUSTOMER_COLUMNS:
            raise ValueError("Customer CSV columns do not match the V1 contract")
        customers: list[Customer] = []
        for line_number, row in enumerate(reader, start=2):
            deleted = row["is_deleted"].lower()
            if deleted not in {"true", "false"}:
                raise ValueError(f"Invalid is_deleted value on line {line_number}")
            try:
                customers.append(
                    Customer(
                        customer_id=row["customer_id"],
                        signup_date=date.fromisoformat(row["signup_date"]),
                        region=row["region"],
                        membership_plan=row["membership_plan"],
                        acquisition_channel=row["acquisition_channel"],
                        account_status=row["account_status"],
                        created_at=parse_utc_timestamp(row["created_at"]),
                        updated_at=parse_utc_timestamp(row["updated_at"]),
                        is_deleted=deleted == "true",
                    )
                )
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid customer row on line {line_number}") from exc
    return customers


def write_change_csv(path: Path, changes: list[CustomerChange]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = tuple(CustomerChange.__dataclass_fields__)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(change.csv_row() for change in changes)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest(
    *,
    customers: list[Customer],
    snapshot_path: Path,
    snapshot_id: str,
    snapshot_at: datetime,
    generator_seed: int,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "contract_version": "1.0.0",
        "snapshot_id": snapshot_id,
        "snapshot_at": format_utc_timestamp(snapshot_at),
        "file": snapshot_path.name,
        "sha256": sha256_file(snapshot_path),
        "generator_seed": generator_seed,
        "row_count": len(customers),
        "unique_customer_count": len({row.customer_id for row in customers}),
        "non_deleted_row_count": sum(not row.is_deleted for row in customers),
        "deleted_row_count": sum(row.is_deleted for row in customers),
        "columns": list(CUSTOMER_COLUMNS),
        "distributions": {
            field: dict(
                sorted(Counter(getattr(row, field) for row in customers).items())
            )
            for field in (
                "region",
                "membership_plan",
                "acquisition_channel",
                "account_status",
            )
        },
    }
    if extra:
        manifest.update(extra)
    return manifest


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"{json.dumps(manifest, indent=2, sort_keys=True)}\n", encoding="utf-8"
    )
