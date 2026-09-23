# Bronze Layer Validation

Use `07_bronze_layer_validation.ipynb` for GitHub notebook rendering and Databricks
import. The matching `.py` file is the Databricks source export of the same cells.
Import only one format into the same workspace folder to avoid name conflicts.

This version is based on the user's executed `Bronze Layer Notebook.py`.
It retains the actual acceptance table, topic, checkpoint and evidence directory
as widget defaults. The broker address and secret scope must be supplied at runtime.
No credentials or notebook execution outputs are included.

The default stage is REVIEW: it reads existing A/B/C evidence without connecting
to Kafka or starting ingestion. Legacy evidence from the original notebook is
supported. To perform a new acceptance run, configure a fresh evidence directory
and select A, then B, then C following the embedded instructions. Never overwrite
completed evidence or change checkpoint/target between stages.

Run the parameter cell first and fill widgets, then run the remaining cells for
the selected stage. This works after a new Python session: imports, functions and
baselines are restored on every execution. Keep the repository's `pipelines/`
package available; configure `repo_root` if automatic discovery fails.

Local validation: all code cells compile, source formatting passes, and 32
existing project tests pass. The cleaned notebook has not itself been executed
against Databricks. The earlier successful cloud run was user-reported; its
results remain in the Volume and are not fabricated as notebook outputs.
