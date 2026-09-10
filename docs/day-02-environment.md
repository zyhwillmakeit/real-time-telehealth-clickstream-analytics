# Day 2 — Environment and Connectivity Gate

Day 2 is complete only when all six read-only checks pass against the environment that will be used for implementation. Installing clients or filling configuration templates is not sufficient.

## Tested local toolchain

| Item | Selected version |
|---|---:|
| Python | 3.11.1 |
| confluent-kafka | 2.15.0 |
| fastavro | 1.12.2 |
| psycopg | 3.3.5 |
| databricks-sdk | 0.63.0 |
| databricks-sql-connector | 4.0.5 |
| dbt-core / dbt-databricks | 1.10.19 / 1.10.19 |
| boto3 | 1.43.91 |

The top-level set is in `requirements.txt`; the exact resolved environment is in `requirements.lock`. dbt-databricks is held on the latest 1.10 patch used by this project rather than adopting the 1.12 release during initial setup. Version changes will be deliberate and retested.

## Required services

| Check | Required read-only operation | Pass evidence |
|---|---|---|
| Kafka | Fetch broker and topic metadata | Broker metadata returned; topic may be created on Day 6 |
| Schema Registry | `GET /subjects` | A JSON subject list is returned |
| PostgreSQL | `SELECT 1` | Query returns exactly 1 over the Databricks-reachable endpoint |
| Databricks workspace | Read current user | Workspace API authenticates |
| SQL Warehouse | `SELECT 1` | Warehouse starts if needed and returns exactly 1 |
| Object storage | S3 `HeadBucket` | Configured bucket is reachable by the selected credential chain |

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.lock
cp .env.example .env
```

Place real values only in `.env`, which Git ignores. Then run:

```bash
.venv/bin/python scripts/check_connectivity.py
```

Run a single service during troubleshooting:

```bash
.venv/bin/python scripts/check_connectivity.py --service postgres
```

The script emits only status, check type, missing key names, exception class names, and dependency versions. It does not print endpoints, usernames, tokens, passwords, or full driver errors.

## Configuration notes

- For Confluent Cloud, keep `KAFKA_SECURITY_PROTOCOL=SASL_SSL` and provide both API credentials.
- A local plaintext broker can use `KAFKA_SECURITY_PROTOCOL=PLAINTEXT`; Kafka API credentials are then optional.
- Schema Registry credentials are optional only when the selected registry allows anonymous access.
- `POSTGRES_HOST` must be reachable from Databricks for the later JDBC path. A laptop `localhost` result does not satisfy the cloud gate.
- `DATABRICKS_HTTP_PATH` is the SQL Warehouse connection HTTP path, not its browser URL.
- S3 credentials use the standard boto credential chain. Do not copy permanent credentials into the repository.
- `S3_ENDPOINT_URL` is reserved for an S3-compatible local development service; leave it empty for AWS S3.

## Current gate status

| Check | Status | Current evidence |
|---|---|---|
| Dependency installation | PASS | The exact lock resolved and imported on Python 3.11.1 |
| Local lint/unit checks | PASS | Recorded in `evidence/day-02/README.md` |
| Kafka | BLOCKED | Connection variables are not configured |
| Schema Registry | BLOCKED | Connection variables are not configured |
| PostgreSQL | BLOCKED | Connection variables are not configured |
| Databricks workspace | BLOCKED | Connection variables are not configured |
| SQL Warehouse | BLOCKED | Connection variables are not configured |
| Object storage | BLOCKED | Bucket and region are not configured |

Day 2 remains unchecked in the roadmap until the six service checks pass. The resulting redacted JSON report will be committed as evidence; `.env` remains local.

