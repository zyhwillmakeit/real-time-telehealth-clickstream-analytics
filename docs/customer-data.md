# Deterministic Customer Data

## Purpose

Day 4 provides the synthetic customer source used by later customer snapshots and event enrichment. The generator contains no names, contact details, addresses, clinical fields, or other protected information. Every output is reproducible from an explicit seed and timestamp.

## Initial snapshot

The reviewed V1 snapshot contains exactly 10,000 rows with unique IDs from `cus_00000001` through `cus_00010000`. It uses:

- generator seed `20260910`;
- snapshot time `2026-09-10T23:59:59Z`;
- the columns and controlled values defined in the [V1 data contract](data-contract.md);
- canonical ascending customer-ID order and UTC timestamps;
- a SHA-256 checksum and value distributions recorded in its manifest.

The extract and manifest are stored in `evidence/day-04`. Regenerate them with:

```bash
.venv/bin/python -m scripts.seed_customers
```

The command validates row count, key uniqueness, ID format, domains, timestamp order, logical deletion state, and snapshot boundaries before writing the result. Re-running it with the same arguments produces the same CSV checksum.

## Deterministic changes

The update command reads a complete snapshot and writes another complete snapshot plus a row-level change log. Its default scenario applies 300 single-attribute changes, 50 logical deletions, and 100 inserts at one fixed effective time:

```bash
.venv/bin/python -m scripts.update_customers
```

Generated update files go to the ignored `data/` directory by default. Existing customers retain `customer_id`, `signup_date`, and `created_at`. Changed rows receive the supplied `effective_at`; logically deleted customers also become `closed`. Inserted IDs continue after the highest source ID. Each existing customer is selected at most once per update batch.

Use explicit arguments to create another reproducible scenario:

```bash
.venv/bin/python -m scripts.update_customers \
  --seed 20260912 \
  --effective-at 2026-09-12T12:00:00Z \
  --attribute-updates 200 \
  --soft-deletes 25 \
  --inserts 50
```

## PostgreSQL loading

Both commands can upsert their completed snapshot into `source.customer_master` using the ignored `.env` file:

```bash
.venv/bin/python -m scripts.seed_customers --load-postgres
.venv/bin/python -m scripts.update_customers --load-postgres
```

The seed command applies the Day 3 DDL before loading. Both loaders use one database transaction and reject stale updates through the `updated_at` comparison. They only report row counts; credentials and connection values are never printed.

The live PostgreSQL load depends on the Day 2 connectivity gate. Local generation, update behavior, validation, and checksum evidence do not require external services.

## Downstream boundary

These scripts model the operational source. Day 11 will extract and publish complete source snapshots into Bronze, reject partial or invalid extracts, and merge only a validated snapshot into `silver.dim_customer_current`.
