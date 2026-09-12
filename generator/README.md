# Generator

Day 4 adds deterministic customer-source generation. `customer_data.py` owns the shared V1 generation, update, CSV, manifest, and validation logic. `customer_postgres.py` owns transactional PostgreSQL upserts using the Day 3 DDL.

```bash
# Reproduce the reviewed 10,000-customer V1 snapshot.
.venv/bin/python -m scripts.seed_customers

# Produce the default deterministic update scenario under ignored data/.
.venv/bin/python -m scripts.update_customers
```

See [`docs/customer-data.md`](../docs/customer-data.md) for guarantees, artifacts, arguments, and optional PostgreSQL loading.

Days 5–6 will add the journey state machine, delivery delay queue, configurable failure injection, and expected-results manifests without duplicating customer generation.

Day 5: run `.venv/bin/python -m scripts.generate_journeys`. See [journey simulation](../docs/day-05-journeys.md).

Day 6: `python -m scripts.publish_events` plans offline; add `--send` for live delivery. See [instructions](../docs/day-06-kafka.md).
