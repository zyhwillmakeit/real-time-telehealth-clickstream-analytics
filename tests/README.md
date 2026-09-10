# Tests

Tests are added with their production contracts and code.

- `test_connectivity.py` checks the redacted Day 2 environment gate behavior.
- `test_contracts.py` checks the Day 3 Avro/rule agreement, all valid and invalid fixtures, retry identity, fail-closed unknown fields, and customer DDL controls.
- `test_customer_data.py` checks deterministic Day 4 generation and updates, CSV round trips, the complete 10,000-row fixture, its manifest checksum, domains, and PostgreSQL configuration safety.

Later days add integration, reconciliation, schema-evolution, failure-recovery, and performance scenarios.

`test_journeys.py` verifies causal ordering, cross-day sessions, contract validity, deterministic IDs, expected outcomes and observation cutoffs. Test discovery is limited to `tests/` so manual connection scripts are not executed.
