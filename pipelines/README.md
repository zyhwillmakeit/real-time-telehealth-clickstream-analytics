# Pipelines

Streaming jobs, customer snapshots, reconciliation, replay, and shared validation logic.

S01 is implemented in `bronze_kafka.py`; `bronze_evidence.py` matches producer acknowledgements to raw Kafka coordinates. See [Day 7](../docs/day-07-bronze.md) and [Notebook](../notebooks/day07_bronze.py).
