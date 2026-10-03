"""Schema and config validity tests."""
from __future__ import annotations

import json
from datetime import datetime

import pytest
from jsonschema import Draft202012Validator

import traffic_generator as tg

SCHEMA = json.loads(tg.SCHEMA_PATH.read_text(encoding="utf-8"))
VALIDATOR = Draft202012Validator(SCHEMA)


@pytest.fixture(scope="module")
def jsonl_rows(tmp_path_factory):
    out = tmp_path_factory.mktemp("jsonl")
    cfg = tg.GeneratorConfig(days=1, interval_seconds=600, segments_per_area=2, seed=5, output_format="jsonl",
                             output_dir=out, missing_pct=5, invalid_pct=5)
    tg.generate_dataset(cfg)
    return [json.loads(line) for line in (out / "traffic_events.jsonl").read_text(encoding="utf-8").splitlines()]


def test_schema_is_valid_json_schema():
    Draft202012Validator.check_schema(SCHEMA)


def test_schema_matches_generator_columns():
    assert list(SCHEMA["properties"]) == tg.EVENT_COLUMNS
    assert SCHEMA["required"] == tg.EVENT_COLUMNS
    assert SCHEMA["x-partitioning"]["parquet"] == tg.PARTITION_COLUMNS
    for field in tg.OPTIONAL_FIELDS:
        assert "null" in SCHEMA["properties"][field]["type"], field


def test_schema_enums_match_generator():
    props = SCHEMA["properties"]
    assert props["weather"]["enum"][:-1] == list(tg.WEATHER_LABELS)
    assert props["incident_type"]["enum"] == list(tg.INCIDENT_LABELS)
    assert props["congestion_level"]["enum"] == list(tg.CONGESTION_LABELS)
    assert props["day_of_week"]["enum"] == tg.DAY_NAMES


def test_clean_and_missing_field_records_validate(jsonl_rows):
    checked = 0
    for row in jsonl_rows:
        if row["anomaly_type"] in ("None", "Missing_Field"):
            errors = list(VALIDATOR.iter_errors(row))
            assert not errors, (row, errors[0].message)
            checked += 1
    assert checked > 2000


def test_timestamps_are_iso8601_ist(jsonl_rows):
    for row in jsonl_rows[:500]:
        for key in ("event_timestamp", "generated_at"):
            assert row[key].endswith("+05:30")
            datetime.fromisoformat(row[key])
        assert row["generated_at"] > row["event_timestamp"]


def test_most_invalid_value_records_violate_schema(jsonl_rows):
    invalid = [r for r in jsonl_rows if r["anomaly_type"] == "Invalid_Value"]
    assert invalid
    # speed-over-limit anomalies are schema-valid (limit is per segment); the other 3 kinds are not
    failing = sum(1 for r in invalid if not VALIDATOR.is_valid(r))
    assert failing >= len(invalid) * 0.5


def test_road_segment_config():
    raw = json.loads((tg.DEFAULT_CONFIG_DIR / "road_segments.json").read_text(encoding="utf-8"))["road_segments"]
    areas = json.loads((tg.DEFAULT_CONFIG_DIR / "areas.json").read_text(encoding="utf-8"))["areas"]
    assert len(areas) == 10
    assert set(SCHEMA["properties"]["area"]["enum"]) == {a["area_name"] for a in areas}
    ids = [s["road_segment_id"] for s in raw]
    assert len(ids) == len(set(ids))
    per_area = {a["area_id"]: sum(s["area_id"] == a["area_id"] for s in raw) for a in areas}
    assert all(10 <= n <= 20 for n in per_area.values())
    for s in raw:
        assert s["road_type"] in ("Highway", "Arterial", "Collector", "Local")
        assert 12.7 <= s["latitude"] <= 13.2 and 77.4 <= s["longitude"] <= 77.9
        assert s["lane_count"] >= 2 and 20 <= s["speed_limit_kmph"] <= 100
        assert s["capacity_vehicles_per_minute"] > 0 and 0 < s["free_flow_ratio"] <= 1
