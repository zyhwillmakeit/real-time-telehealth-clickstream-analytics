# Step 8 evidence index

The original Step 8 milestone is complete based on the project owner's reported
cloud results. The complete expanded live-test matrix is not yet finished.
See [acceptance.json](acceptance.json) for machine-readable status.

| Check | Result | Evidence basis |
|---|---|---|
| Normal coverage | Bronze/classified 50/50; missing, unexpected, duplicate keys and invalid routes all 0 | User-reported output |
| Normal unchanged-input restart | 50 → 50 | User confirmation |
| Six-case routing | 6 deliveries: 4 valid, 2 quarantined; expected error codes | User confirmation; Volume files below |
| Six-case restart | 6 → 6; same query ID, new run ID | User confirmation; Volume files below |
| Offline project suite | 43 passed | Local test execution |
| Tombstone / unknown schema live tests | Pending | Offline tests exist; no live evidence claimed |
| New input after S02 restart | Pending | Step 7 new-input recovery does not establish S02 behavior |

Original cloud evidence is retained at:

```text
/Volumes/workspace/ops/validation_evidence/step08-negative-01/
  expected.json
  producer-report.json
  acceptance.json
```

These raw files were not copied into Git or independently read during this update.
The [final notebook](../../notebooks/08_nagative_acceptance.ipynb) saves subsequent
reports under unique `acceptance-<UTC timestamp>-<suffix>.json` names and never
replaces the original report. Its additional payload/view assertions are a
runbook improvement, not claimed historical execution evidence.

The customer-ID case establishes preservation without a lookup, not independently
verified absence from a particular snapshot. Abnormal termination recovery remains
Step 23. Existing Step 7 Volume evidence is unchanged. Step 9 may proceed.
