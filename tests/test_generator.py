"""Tests for traffic_generator: counts, intervals, constraints, relationships, reproducibility, output."""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pytest

import traffic_generator as tg

SMALL = dict(days=1, interval_seconds=300, segments_per_area=2, seed=7)


def frame(cfg: tg.GeneratorConfig) -> pd.DataFrame:
    return pd.concat(tg.iter_event_batches(cfg), ignore_index=True)


@pytest.fixture(scope="module")
def two_days() -> tuple[pd.DataFrame, pd.DataFrame]:
    """2 weekdays (Wed-Thu), 10-min interval, 3 segments per area."""
    cfg = tg.GeneratorConfig(days=2, interval_seconds=600, segments_per_area=3, seed=11)
    segs = tg.load_city_config(cfg.config_dir, 3).segments
    return frame(cfg), segs


# ---- counts & intervals ------------------------------------------------------------------
def test_record_count_matches_expected_and_steps():
    cfg = tg.GeneratorConfig(**SMALL)
    df = frame(cfg)
    assert len(df) == tg.expected_record_count(cfg, 20) == 288 * 20
    assert (df.groupby("road_segment_id").size() == 288).all()


def test_time_interval_is_constant_per_segment():
    df = frame(tg.GeneratorConfig(**SMALL))
    ts = pd.to_datetime(df["event_timestamp"])
    diffs = ts.groupby(df["road_segment_id"]).diff().dropna()
    assert (diffs == pd.Timedelta(seconds=300)).all()
    assert df["event_timestamp"].iloc[0] == "2026-07-01T00:00:00+05:30"


def test_records_limit_and_partial_start():
    cfg = tg.GeneratorConfig(**{**SMALL, "records_limit": 1234, "start_time": "2026-07-01T23:30:00"})
    df = frame(cfg)
    assert len(df) == 1234
    assert df["event_timestamp"].iloc[0] == "2026-07-01T23:30:00+05:30"
    assert set(df["day_of_week"]) == {"Wednesday", "Thursday"}  # crosses midnight cleanly


def test_unique_event_ids(two_days):
    df, _ = two_days
    assert df["event_id"].is_unique
    assert df["event_id"].str.match(r"^[A-Z]{3}-\d{3}_\d{8}T\d{6}$").all()


# ---- constraints ---------------------------------------------------------------------------
def test_speed_never_exceeds_limit(two_days):
    df, segs = two_days
    limit = df["road_segment_id"].map(segs.set_index("road_segment_id")["speed_limit_kmph"])
    assert (df["avg_speed_kmph"] <= limit).all()
    assert (df["avg_speed_kmph"] >= 0).all()
    zero = df["avg_speed_kmph"] == 0
    assert (df.loc[zero, "incident_type"] == "Road_Closure").all()


def test_values_in_valid_ranges(two_days):
    df, _ = two_days
    assert (df["vehicle_count"] >= 0).all()
    assert df["occupancy_pct"].between(0, 100).all()
    assert (df["data_source"] == "synthetic_simulator").all()
    assert (df["anomaly_type"] == "None").all()
    none = df["incident_type"] == "None"
    assert (df.loc[none, "incident_duration_minutes"] == 0).all()
    assert (df.loc[~none, "incident_duration_minutes"] > 0).all()
    assert (df["is_weekend"] == df["day_of_week"].isin(["Saturday", "Sunday"])).all()
    assert (pd.to_datetime(df["event_timestamp"]).dt.hour == df["hour_of_day"]).all()


# ---- congestion ----------------------------------------------------------------------------
@pytest.mark.parametrize("score,level", [(0, "Low"), (24.9, "Low"), (25, "Moderate"), (44.9, "Moderate"),
                                         (45, "High"), (64.9, "High"), (65, "Severe"), (100, "Severe")])
def test_classify_congestion_thresholds(score, level):
    assert tg.classify_congestion(np.array([score]))[0] == level


def test_congestion_score_formula():
    assert tg.congestion_score(np.array([50.0]), np.array([50.0]), np.array([0.0]))[0] == 0
    assert tg.congestion_score(np.array([0.0]), np.array([50.0]), np.array([100.0]))[0] == 100
    assert tg.congestion_score(np.array([25.0]), np.array([50.0]), np.array([40.0]))[0] == pytest.approx(45)


def test_dataset_congestion_consistent_with_score(two_days):
    df, segs = two_days
    limit = df["road_segment_id"].map(segs.set_index("road_segment_id")["speed_limit_kmph"])
    open_ = df["incident_type"] != "Road_Closure"
    score = tg.congestion_score(df["avg_speed_kmph"], limit, df["occupancy_pct"])
    assert (tg.classify_congestion(score[open_]) == df.loc[open_, "congestion_level"]).all()
    assert (df.loc[~open_, "congestion_level"] == "Severe").all()


# ---- traffic relationships -----------------------------------------------------------------
def metrics(demand=0.5, weather=0, inc_cap=1.0, closed=False, n=4000, seed=0):
    shape = (n, 1)
    return tg.compute_traffic_metrics(
        np.full(shape, demand), np.full(shape, weather), np.full(shape, inc_cap), np.full(shape, closed),
        np.array([60.0]), np.array([4.0]), np.array([50.0]), np.array([0.9]), 60, np.random.default_rng(seed))


def test_more_demand_means_lower_speed_higher_occupancy():
    runs = [metrics(d) for d in (0.2, 0.6, 1.0, 1.5)]
    speeds = [m["avg_speed_kmph"].mean() for m in runs]
    occ = [m["occupancy_pct"].mean() for m in runs]
    assert speeds == sorted(speeds, reverse=True)
    assert occ == sorted(occ)


def test_vehicle_count_hard_capped_by_effective_capacity():
    assert metrics(demand=3.0)["vehicle_count"].max() <= 60            # nominal 60 veh/min
    assert metrics(demand=3.0, weather=3, inc_cap=0.45)["vehicle_count"].max() <= np.ceil(60 * 0.70 * 0.45)
    near_cap = metrics(demand=0.98)["vehicle_count"]                    # Poisson tail would exceed 60
    assert near_cap.max() == 60 and near_cap.mean() > 50


def test_dataset_vehicle_count_within_capacity(two_days):
    df, segs = two_days
    cap = df["road_segment_id"].map(segs.set_index("road_segment_id")["capacity_vehicles_per_minute"])
    assert (df["vehicle_count"] <= np.ceil(cap * 600 / 60)).all()


def test_rain_and_incidents_reduce_speed():
    clear, rain, heavy = (metrics(0.8, w)["avg_speed_kmph"].mean() for w in (0, 2, 3))
    assert clear > rain > heavy
    assert metrics(0.8, inc_cap=0.45)["avg_speed_kmph"].mean() < clear


def test_road_closure_blocks_traffic():
    m = metrics(0.8, closed=True, inc_cap=0.0)
    assert (m["vehicle_count"] == 0).all() and (m["avg_speed_kmph"] == 0).all()
    assert (m["congestion_level"] == "Severe").all()


def test_peak_hours_busier_than_night(two_days):
    df, _ = two_days
    by_hour = df.groupby("hour_of_day").agg(cnt=("vehicle_count", "mean"), spd=("avg_speed_kmph", "mean"))
    night = by_hour.loc[[2, 3, 4]]
    for peak in ([9, 10], [18, 19]):
        assert by_hour.loc[peak, "cnt"].mean() > 3 * night["cnt"].mean()
        assert by_hour.loc[peak, "spd"].mean() < night["spd"].mean()
    assert df["occupancy_pct"].corr(df["avg_speed_kmph"]) < 0


def test_junction_areas_more_congested_than_commercial(two_days):
    df, _ = two_days
    peak = df[df["hour_of_day"].isin([9, 18, 19])]
    severe_high = peak["congestion_level"].isin(["High", "Severe"]).groupby(peak["area"]).mean()
    assert severe_high["Silk Board"] > severe_high["MG Road"]


def test_weekend_profile_differs_from_weekday():
    base = dict(days=1, interval_seconds=900, segments_per_area=2, seed=3)
    weekday = frame(tg.GeneratorConfig(**base, start_time="2026-07-01T00:00:00"))   # Wednesday
    weekend = frame(tg.GeneratorConfig(**base, start_time="2026-07-05T00:00:00"))   # Sunday
    assert weekend["is_weekend"].all() and not weekday["is_weekend"].any()
    tech = ["Whitefield", "Electronic City", "Outer Ring Road"]
    am = lambda d: d[d["area"].isin(tech) & (d["hour_of_day"] == 9)]["vehicle_count"].mean()
    assert am(weekday) > 1.4 * am(weekend)


def test_repeated_days_are_not_identical(two_days):
    df, _ = two_days
    day = df["event_timestamp"].str[:10]
    a = df[day == "2026-07-01"]["vehicle_count"].to_numpy()
    b = df[day == "2026-07-02"]["vehicle_count"].to_numpy()
    assert len(a) == len(b) and not np.array_equal(a, b)


# ---- reproducibility -----------------------------------------------------------------------
def test_same_seed_reproduces_exactly():
    cfg = tg.GeneratorConfig(**{**SMALL, "duplicate_pct": 1, "missing_pct": 1})
    pd.testing.assert_frame_equal(frame(cfg), frame(cfg))


def test_different_seed_differs():
    a = frame(tg.GeneratorConfig(**SMALL))
    b = frame(tg.GeneratorConfig(**{**SMALL, "seed": 8}))
    assert not a["vehicle_count"].equals(b["vehicle_count"])


# ---- anomalies -----------------------------------------------------------------------------
def test_anomaly_injection_is_labelled():
    cfg = tg.GeneratorConfig(**{**SMALL, "duplicate_pct": 2, "delayed_pct": 3, "missing_pct": 4, "invalid_pct": 5})
    df = frame(cfg)
    n = 288 * 20
    counts = df["anomaly_type"].value_counts()
    assert counts["Duplicate"] == round(n * 0.02)
    assert counts["Delayed"] == round(n * 0.03)
    assert counts["Missing_Field"] == round(n * 0.04)
    assert counts["Invalid_Value"] == round(n * 0.05)
    assert len(df) == n + counts["Duplicate"]

    dups = df[df["anomaly_type"] == "Duplicate"]
    assert dups["event_id"].isin(df.loc[df["anomaly_type"] != "Duplicate", "event_id"]).all()
    delayed = df[df["anomaly_type"] == "Delayed"]
    lag = pd.to_datetime(delayed["generated_at"]) - pd.to_datetime(delayed["event_timestamp"])
    assert (lag >= pd.Timedelta(minutes=5)).all()
    missing = df[df["anomaly_type"] == "Missing_Field"]
    assert missing[tg.OPTIONAL_FIELDS].isna().any(axis=1).all()
    clean = df[df["anomaly_type"] == "None"]
    assert not clean[tg.OPTIONAL_FIELDS].isna().any().any()
    inv = df[df["anomaly_type"] == "Invalid_Value"]
    bad = (inv["vehicle_count"] < 0) | (inv["avg_speed_kmph"] > 100) | (inv["occupancy_pct"] > 100) | (inv["latitude"] == 0)
    assert bad.all()
    assert pd.to_datetime(df["generated_at"]).is_monotonic_increasing  # arrival order


def test_duplicates_are_exact_copies_of_clean_events():
    df = frame(tg.GeneratorConfig(**{**SMALL, "duplicate_pct": 10, "delayed_pct": 10,
                                     "missing_pct": 10, "invalid_pct": 10}))
    dups = df[df["anomaly_type"] == "Duplicate"].set_index("event_id")
    originals = df[df["anomaly_type"] != "Duplicate"].set_index("event_id").loc[dups.index]
    assert (originals["anomaly_type"] == "None").all()
    same = [c for c in tg.EVENT_COLUMNS if c not in ("event_id", "generated_at", "anomaly_type")]
    pd.testing.assert_frame_equal(dups[same], originals[same])
    assert (pd.to_datetime(dups["generated_at"]) > pd.to_datetime(originals["generated_at"])).all()


def test_arrival_order_is_global_across_batches():
    # ~12 steps per batch, so 5-120 min delays always cross many batch boundaries
    cfg = tg.GeneratorConfig(**{**SMALL, "batch_size": 240, "delayed_pct": 5, "duplicate_pct": 2})
    batches = list(tg.iter_event_batches(cfg))
    df = pd.concat(batches, ignore_index=True)
    assert len(batches) > 20
    assert pd.to_datetime(df["generated_at"]).is_monotonic_increasing   # across all batch boundaries
    n_dup = df["anomaly_type"].eq("Duplicate").sum()
    assert n_dup > 0 and len(df) == 5760 + n_dup                       # nothing lost in the buffer
    assert set(df["event_id"]) == set(frame(tg.GeneratorConfig(**SMALL))["event_id"])
    # delays exceed the 60-min batch span, so late rows were carried into later batches
    delayed = df[df["anomaly_type"] == "Delayed"]
    lag = pd.to_datetime(delayed["generated_at"]) - pd.to_datetime(delayed["event_timestamp"])
    assert (lag > pd.Timedelta(minutes=65)).any()


def test_arrival_order_respects_records_limit():
    cfg = tg.GeneratorConfig(**{**SMALL, "batch_size": 240, "delayed_pct": 5, "records_limit": 3000})
    df = frame(cfg)
    assert len(df) == 3000 and pd.to_datetime(df["generated_at"]).is_monotonic_increasing


def test_anomalies_disabled_by_default():
    assert not tg.GeneratorConfig().anomalies_enabled


# ---- output formats & partitioning ----------------------------------------------------------
@pytest.mark.parametrize("fmt", ["csv", "jsonl"])
def test_text_outputs(tmp_path, fmt):
    cfg = tg.GeneratorConfig(**SMALL, output_format=fmt, output_dir=tmp_path)
    summary = tg.generate_dataset(cfg)
    path = tmp_path / f"traffic_events.{fmt}"
    assert summary.files == [path] and summary.rows_written == 5760
    for name in ("areas.csv", "road_segments.csv", "traffic_event_schema.json"):
        assert (tmp_path / name).exists()
    if fmt == "csv":
        df = pd.read_csv(path, keep_default_na=False, na_values=[""])
        assert list(df.columns) == tg.EVENT_COLUMNS
    else:
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        assert len(rows) == 5760 and list(rows[0]) == tg.EVENT_COLUMNS
        assert isinstance(rows[0]["is_weekend"], bool) and isinstance(rows[0]["vehicle_count"], int)
    segs = pd.read_csv(tmp_path / "road_segments.csv")
    assert len(segs) == 20 and {"road_type", "lane_count", "capacity_vehicles_per_minute"} <= set(segs.columns)


def test_csv_rolls_files_at_max_rows(tmp_path):
    cfg = tg.GeneratorConfig(**SMALL, output_dir=tmp_path, max_rows_per_file=2000)
    summary = tg.generate_dataset(cfg)
    assert [p.name for p in summary.files] == [f"traffic_events_part-{i:05d}.csv" for i in range(3)]
    total = sum(len(pd.read_csv(p)) for p in summary.files)
    assert total == 5760


def test_parquet_partitioning(tmp_path):
    cfg = tg.GeneratorConfig(days=2, interval_seconds=600, segments_per_area=2, seed=1,
                             output_format="parquet", output_dir=tmp_path)
    summary = tg.generate_dataset(cfg)
    root = tmp_path / "traffic_events"
    # one file per year/month/day/area partition: 2 days x 10 areas
    assert len(summary.files) == 20
    rel = summary.files[0].relative_to(root).parts
    assert rel[0] == "year=2026" and rel[1] == "month=07" and rel[2].startswith("day=") and rel[3].startswith("area=")
    table = ds.dataset(root, format="parquet", partitioning="hive").to_table()
    assert table.num_rows == 2 * 144 * 20
    assert set(tg.EVENT_COLUMNS) <= set(table.column_names)
    df = table.to_pandas()
    assert df["event_id"].is_unique and set(df["area"]) == set(tg.load_city_config(cfg.config_dir, 2).areas["area_name"])


def test_existing_output_requires_overwrite(tmp_path):
    cfg = tg.GeneratorConfig(**SMALL, output_dir=tmp_path)
    tg.generate_dataset(cfg)
    with pytest.raises(tg.ConfigError):
        tg.generate_dataset(cfg)
    tg.generate_dataset(replace(cfg, overwrite=True))


@pytest.mark.parametrize("bad", [{"days": 0}, {"interval_seconds": 0}, {"segments_per_area": 21},
                                 {"duplicate_pct": 101}, {"start_time": "yesterday"}, {"seed": -1}])
def test_invalid_config_rejected(tmp_path, bad):
    with pytest.raises(tg.ConfigError):
        tg.generate_dataset(tg.GeneratorConfig(**{**SMALL, **bad, "output_dir": tmp_path}))


def test_cli_exit_codes(tmp_path):
    ok = tg.main(["--days", "0.25", "--interval-seconds", "900", "--segments-per-area", "1",
                  "--output-dir", str(tmp_path), "--output-format", "jsonl"])
    assert ok == 0 and (tmp_path / "traffic_events.jsonl").exists()
    assert tg.main(["--days", "-1", "--output-dir", str(tmp_path)]) == 2


def test_start_time_parsing():
    assert tg.parse_start_time("2026-07-01T00:00:00").utcoffset().total_seconds() == 19800
    utc = tg.parse_start_time("2026-06-30T18:30:00+00:00")
    assert utc.replace(tzinfo=None) == datetime(2026, 7, 1)
