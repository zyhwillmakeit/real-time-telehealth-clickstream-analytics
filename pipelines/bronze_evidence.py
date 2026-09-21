"""Compare exact broker acknowledgements with Bronze delivery keys."""

from collections import Counter


def expected_keys(reports):
    keys = []
    for report in reports:
        deliveries = report.get("deliveries", [])
        if (
            report.get("status") != "PASS"
            or not deliveries
            or report.get("planned") != len(deliveries)
            or report.get("acknowledged") != len(deliveries)
        ):
            raise ValueError("Use complete successful producer delivery reports")
        for row in deliveries:
            if row.get("status") != "ACKNOWLEDGED":
                raise ValueError("Unacknowledged delivery in report")
            if (
                not isinstance(row.get("topic"), str)
                or type(row.get("partition")) is not int
                or row["partition"] < 0
                or type(row.get("offset")) is not int
                or row["offset"] < 0
            ):
                raise ValueError("Invalid broker coordinates")
            keys.append(f"{row['topic']}:{row['partition']}:{row['offset']}")
    if len(keys) != len(set(keys)):
        raise ValueError("Reports overlap in Kafka offsets")
    if not keys:
        raise ValueError("At least one report is required")
    return keys


def compare_keys(expected, actual):
    counts = Counter(actual)
    missing = sorted(set(expected) - counts.keys())
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    return {
        "status": "PASS" if not missing and not duplicates else "FAILED",
        "expected_deliveries": len(expected),
        "matched_deliveries": sum(key in counts for key in expected),
        "duplicate_raw_keys": duplicates,
        "missing_raw_keys": missing,
    }


def verify_bronze(spark, table, reports):
    from pyspark.sql import functions as F

    keys = expected_keys(reports)
    wanted = spark.createDataFrame([(key,) for key in keys], "raw_record_id STRING")
    raw = spark.table(table)
    # Only collect coordinates for this bounded test, never raw message payloads.
    actual = [
        r.raw_record_id
        for r in raw.join(wanted, "raw_record_id").select("raw_record_id").collect()
    ]
    result = compare_keys(keys, actual)
    result["table_rows"] = raw.count()
    result["global_duplicate_groups"] = (
        raw.groupBy("raw_record_id").count().filter(F.col("count") > 1).count()
    )
    if result["global_duplicate_groups"]:
        result["status"] = "FAILED"
    return result
