# Evidence

Reviewed reconciliation, recovery, performance, freshness, cost, and final acceptance reports will be indexed here. Large raw logs and generated payloads are excluded from version control.

- [`day-02/`](day-02/) records the local environment preparation and pending cloud-connectivity gate.
- [`day-04/`](day-04/) contains the reviewed deterministic 10,000-customer V1 snapshot and manifest.

- [Day 5 acceptance](day-05/acceptance.json): deterministic journey counts and checksums; full outputs are generated under ignored `data/day-05/`.

- [Day 6 local acceptance](day-06/acceptance.json): offline validation; live Registry and Avro confirmation pending.

- [Day 7 acceptance](day-07/README.md): user-confirmed normal stop/restart passed; 40 reported offsets matched, zero missing or duplicate raw keys, B=30 rows and C=50 rows. Original evidence remains in the Volume; see the index and [machine-readable record](day-07/acceptance.json).

- [Step 8 local acceptance](step-08/acceptance.json): offline checks passed; live Spark/Delta acceptance pending.
