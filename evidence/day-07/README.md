# Day 7 — normal stop/restart acceptance passed

User-confirmed results recorded on 2026-09-23. No new cloud execution or download
of the Volume evidence was performed for this documentation update.

| Check | Accepted result |
|---|---|
| Successful producer reports | Two batches, 40 acknowledged Kafka offsets total |
| Exact-offset reconciliation | All 40 present in Bronze |
| Missing / duplicate raw keys | 0 / 0 |
| A baseline and B restart without new input | 30 rows; unchanged after restart |
| C restart after second batch | 50 rows; increase of 20 |
| Additional baseline rows | 10 from earlier partially successful sends |
| Checkpoint continuity | Same checkpoint, unchanged query ID, updated run ID |

## Original evidence to retain

Acceptance files (directory confirmed by the user; full path from the checked-in
validation notebook configuration):

```text
/Volumes/workspace/ops/validation_evidence/day07-check-03/A.json
/Volumes/workspace/ops/validation_evidence/day07-check-03/B.json
/Volumes/workspace/ops/validation_evidence/day07-check-03/C.json
```

Also retain both successful producer delivery reports. Their configured paths in
the checked-in notebook are:

```text
/Volumes/workspace/ops/validation_evidence/delivery-report.json
/Volumes/workspace/ops/validation_evidence/report-b.json
```

These paths are references, not downloaded local copies. Actual query/run ID values,
Delta versions and runtime details remain in the original evidence; they have not
been invented or transcribed here. Do not replace the successful reports with the
earlier partial-send reports, or overwrite A/B/C during another run.

The extra 10 rows are accounted for in the original baseline. Therefore 50 table
rows versus 40 report offsets is expected and is not a reconciliation failure.

This acceptance proves recovery after normal completion/stop. It does not prove
recovery from process termination, dependency outages or partial multi-table
writes. Those failure-recovery scenarios remain in Day 23.

The cleaned [validation notebook](../../notebooks/07_bronze_layer_validation_README.md)
defaults to REVIEW for inspecting preserved evidence without starting ingestion.
See [runbook](../../docs/07-bronze.md) and [structured record](acceptance.json).
