# Jobs

Versioned development and demo job configuration will live here. Runtime, connector, scheduling, retry, timeout, and checkpoint settings will be recorded after Day 2 environment validation.


`day07_bronze.template.json` is a manual AvailableNow job template for an existing tested classic cluster. Replace all placeholders and catalog/storage parameters before creating the job. It has no schedule and limits concurrent runs to one. The referenced notebook and sibling pipelines must be present in the workspace Git folder.
