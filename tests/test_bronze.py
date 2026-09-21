import pytest

from pipelines.bronze_evidence import compare_keys, expected_keys
from pipelines.bronze_kafka import BronzeConfig, jaas_config, reader_options


def config(**changes):
    args = {
        "bootstrap_servers": "broker:9092",
        "topic": "events",
        "table": "dev.bronze.raw",
        "checkpoint": "/Volumes/dev/ops/checkpoints/s01/v1",
    }
    args.update(changes)
    return BronzeConfig(**args)


@pytest.mark.parametrize(
    "changes",
    [
        {"checkpoint": "/tmp/checkpoint"},
        {"checkpoint": "/Volumes/a/../tmp"},
        {"table": "dev.raw; DROP TABLE x"},
        {"topic": "one,two"},
        {"mode": "unknown"},
        {"max_offsets": 0},
    ],
)
def test_reject_unsafe_or_ambiguous_config(changes):
    with pytest.raises(ValueError):
        config(**changes)


def test_reader_preserves_headers_and_fails_on_lost_offsets():
    options = reader_options(config(), 'key"', "secret\\")
    assert options["includeHeaders"] == "true"
    assert options["failOnDataLoss"] == "true"
    assert options["startingOffsets"] == "earliest"
    assert 'username="key\\""' in options["kafka.sasl.jaas.config"]
    assert 'password="secret\\\\"' in options["kafka.sasl.jaas.config"]
    with pytest.raises(ValueError):
        jaas_config("key", "bad\nsecret")


def report(offsets):
    return {
        "status": "PASS",
        "planned": len(offsets),
        "acknowledged": len(offsets),
        "deliveries": [
            {
                "status": "ACKNOWLEDGED",
                "topic": "events",
                "partition": 0,
                "offset": offset,
            }
            for offset in offsets
        ],
    }


def test_restart_expected_coordinates_not_contiguous_ranges():
    keys = expected_keys([report([2, 5]), report([9])])
    assert (
        compare_keys(keys, ["events:0:2", "events:0:5", "events:0:9"])["status"]
        == "PASS"
    )
    assert compare_keys(keys, ["events:0:2", "events:0:9"])["missing_raw_keys"] == [
        "events:0:5"
    ]
    assert compare_keys(keys, keys + [keys[0]])["status"] == "FAILED"


def test_incomplete_or_overlapping_reports_fail():
    for reports in (
        [],
        [report([])],
        [report([1]), report([1])],
        [{**report([1]), "status": "FAILED"}],
    ):
        with pytest.raises(ValueError):
            expected_keys(reports)
