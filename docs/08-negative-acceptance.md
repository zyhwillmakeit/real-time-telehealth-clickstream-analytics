# Step 8 — reproduce the six-case acceptance

## Existing successful run

The owner already confirmed this run passed. Do not resend its messages. To review
or resume it, use the existing producer files, table names, checkpoints and app ID.
The final notebook is a cleaned runbook; its stronger assertions have only been
syntax-checked locally, not rerun in the cloud by this repository update.

Open [08_nagative_acceptance.ipynb](../notebooks/08_nagative_acceptance.ipynb) for
GitHub rendering or import [08_nagative_acceptance.py](../notebooks/08_nagative_acceptance.py)
as a Databricks source notebook. These are matching exports, not separate jobs.
Fill `repo_root` with your Workspace repository path and `registry_url` with your
existing HTTPS Registry endpoint. The notebook reads credentials from the
`telehealth` secret scope and does not embed them or print them.

Execute the cells in order: configuration → Spark health check → producer report
validation → schemas/checkpoint bindings → Bronze → classification → exact offset
and case assertions → restart/recheck → uniquely named evidence report.

## Reproduce from scratch

Create a **new empty** Kafka topic and use new schemas/tables, checkpoints, app ID
and evidence directory. The following commands describe the original naming;
choose a fresh suffix for every resource if that run already exists.

From the local repository root:

```bash
source .venv/bin/activate
# Offline only: no credentials loaded, no network access, no files created.
python -m scripts.step08_acceptance_fixture
# Only for a fresh run after creating the Topic and configuring .env:
python -m scripts.step08_acceptance_fixture --send \
  --topic telehealth-step08-negative-01 \
  --output data/step08-negative-01
```

The script uses the existing repository encoder/producer, explicitly registers
the topic's Avro schema, and sends six fixed deliveries. It intentionally bypasses
semantic input validation for the multi-error fixture, while validating expected
classification offline before sending. A directory that already exists stops a
repeat send. If a send partially fails, keep its report and investigate rather
than deleting the directory and retrying blindly.

Upload `expected.json` and `producer-report.json` from the output directory to
the run's evidence Volume folder. Require PASS and six acknowledged offsets.
Then run the notebook with matching widgets. Install `fastavro==1.12.2` and
`requests==2.34.2` if needed, and restart Python before setting notebook variables.

## Expected results and limits

- Six reported offsets exactly match Bronze and classified output.
- Corrupt Avro: `AVRO_DECODE_FAILED`.
- Multi-error record: `FIELD_REQUIRED:appointment_id` and `DEVICE_PLATFORM_MISMATCH`.
- A 35-minute late event, synthetic customer ID, and duplicate business event
  remain valid: four valid deliveries total.
- Views partition the six physical rows correctly.
- Same-checkpoint restart preserves six rows, with stable query ID and new run ID.
- Evidence is saved without replacing the original `acceptance.json`.

Fixed historical timestamps test rules, not end-to-end latency. Customer snapshot
absence is not asserted. Live tombstones, unknown schema IDs and new input after
S02 restart remain additional pending checks; abrupt termination is Step 23.

If `SELECT 1` hangs, cancel pending cells and test a fresh notebook on the same
compute before continuing. Do not resend the producer batch to fix a compute issue.
