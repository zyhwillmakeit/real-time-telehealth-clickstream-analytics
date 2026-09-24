# Pipelines

Streaming jobs, customer snapshots, reconciliation, replay, and shared validation logic.

S01 is implemented in `bronze_kafka.py`; `bronze_evidence.py` matches producer acknowledgements to raw Kafka coordinates. See [Day 7](../docs/07-bronze.md) and [Notebook](../notebooks/07_bronze_layer_validation.py). Normal stop/restart acceptance passed; see the [evidence index](../evidence/day-07/README.md).

Step 8: `event_validation.py` owns decoding and Registry resolution; `silver_validation.py` writes one atomic classified-delivery Delta table with valid/quarantine views. See [guide](../docs/08-validation.md).
