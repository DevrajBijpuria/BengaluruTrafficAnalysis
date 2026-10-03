#!/usr/bin/env python3
"""Synthetic Bengaluru traffic event generator.

Produces SYNTHETIC road-segment traffic observations for a demo real-time traffic
intelligence pipeline (Kafka -> Spark -> S3 -> Snowflake -> dbt -> Power BI).
Nothing produced here is measured traffic data: every record carries
``data_source = "synthetic_simulator"``.

Model summary (details in README.md):

* demand_ratio = hourly area-type profile x day-of-week x area peak ratio x road-type
  factor x segment factor x daily random factor x (1 + AR(1) minute noise)
* effective capacity = capacity x weather factor x incident factor
* speed follows a BPR-style volume-delay curve, capped at the speed limit
* vehicle_count ~ Poisson(throughput), throughput capped by capacity (with capacity drop)
* occupancy is derived from flow, speed and lane count (density x vehicle length)
* congestion_level is derived from a documented congestion score

Usage: ``python traffic_generator.py --help``
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, NamedTuple

import numpy as np
import pandas as pd

log = logging.getLogger("traffic_generator")

# Asia/Kolkata has had a fixed +05:30 offset with no DST since 1945, so a fixed-offset
# tz avoids a tzdata dependency on Windows.
IST = timezone(timedelta(hours=5, minutes=30), "IST")
ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_DIR = ROOT / "config"
SCHEMA_PATH = ROOT / "schemas" / "traffic_event_schema.json"
DEFAULT_START_TIME = "2026-07-01T00:00:00+05:30"

EVENT_COLUMNS = [
    "event_id", "event_timestamp", "area", "road_segment_id", "road_name", "latitude", "longitude",
    "vehicle_count", "avg_speed_kmph", "occupancy_pct", "congestion_level", "weather", "incident_type",
    "incident_duration_minutes", "is_weekend", "hour_of_day", "day_of_week", "data_source",
    "generated_at", "anomaly_type",
]
PARTITION_COLUMNS = ["year", "month", "day", "area"]
SEGMENT_COLUMNS = [
    "road_segment_id", "area_id", "area", "road_name", "latitude", "longitude", "road_type",
    "lane_count", "speed_limit_kmph", "capacity_vehicles_per_minute", "free_flow_ratio", "base_demand_factor",
]
OPTIONAL_FIELDS = ["road_name", "latitude", "longitude", "occupancy_pct", "weather", "incident_duration_minutes"]
DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

WEATHER_LABELS = np.array(["Clear", "Cloudy", "Rain", "Heavy_Rain"], dtype=object)
INCIDENT_LABELS = np.array(["None", "Accident", "Roadwork", "Vehicle_Breakdown", "Road_Closure"], dtype=object)
CONGESTION_LABELS = np.array(["Low", "Moderate", "High", "Severe"], dtype=object)
ROAD_CLOSURE = 4

# ---- Traffic model parameters (documented in README "Traffic model") --------------------
CONGESTION_THRESHOLDS = (25.0, 45.0, 65.0)   # score cut-offs: Low | Moderate | High | Severe
BPR_ALPHA, BPR_BETA = 1.0, 2.5               # speed = free_flow / (1 + a * (demand/capacity)^b)
CAPACITY_DROP = 0.15                         # throughput loss (max) once demand exceeds capacity
EFFECTIVE_VEHICLE_LENGTH_M = 6.5             # vehicle + detector zone, for occupancy
MIN_MOVING_SPEED = 3.0                       # km/h, crawl speed floor for open roads
MAX_OCCUPANCY = 98.0
EMIT_LATENCY_MS = (200, 5000)                # sensor -> producer latency after interval end
SPEED_NOISE_SD = 0.05                        # multiplicative speed noise
AR_PHI_PER_MINUTE, AR_SD = 0.97, 0.07        # minute-level demand noise (AR(1))
DAILY_SEGMENT_SD, DAILY_AREA_SD = 0.07, 0.05  # day-to-day lognormal demand variation
WEATHER_CAPACITY = np.array([1.00, 0.97, 0.85, 0.70])   # Clear, Cloudy, Rain, Heavy_Rain
WEATHER_SPEED = np.array([1.00, 0.98, 0.88, 0.72])
ROAD_TYPE_DEMAND = {"Highway": 1.00, "Arterial": 1.00, "Collector": 0.80, "Local": 0.55}
ROAD_TYPE_HAZARD = {"Highway": 1.5, "Arterial": 1.2, "Collector": 0.8, "Local": 0.5}
# Hour-of-day weights for the start of rain spells (afternoon/evening convective rain).
RAIN_START_HOUR_WEIGHTS = np.array([2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 6, 8, 9, 9, 8, 6, 5, 4, 3, 2], float)


class IncidentSpec(NamedTuple):
    rate_per_segment_day: float
    median_minutes: float
    sigma: float
    min_minutes: int
    max_minutes: int
    capacity_factor: float     # capacity multiplier while active
    recovery_fraction: float   # recovery period = fraction x duration, clipped to 10-60 min
    timing: str                # "demand" | "night" | "day"
    rain_multiplier: tuple     # hazard multiplier per weather code
    road_type_sensitive: bool


INCIDENT_SPECS = {
    1: IncidentSpec(0.040, 40, 0.5, 15, 180, 0.45, 0.50, "demand", (1.0, 1.0, 2.0, 3.0), True),   # Accident
    2: IncidentSpec(0.015, 180, 0.4, 60, 480, 0.60, 0.25, "night", (1.0, 1.0, 0.5, 0.2), False),  # Roadwork
    3: IncidentSpec(0.120, 25, 0.5, 10, 90, 0.70, 0.30, "demand", (1.0, 1.0, 1.5, 2.5), True),    # Vehicle_Breakdown
    4: IncidentSpec(0.004, 60, 0.6, 20, 300, 0.00, 0.50, "day", (1.0, 1.0, 1.0, 1.5), False),     # Road_Closure
}


class ConfigError(ValueError):
    """Invalid generator configuration or config files."""


# ---- Configuration -----------------------------------------------------------------------
@dataclass
class GeneratorConfig:
    """All generator settings. Defaults match the project defaults (10 areas x 10 segments, 30 days, 60 s)."""

    days: float = 30.0
    interval_seconds: int = 60
    segments_per_area: int = 10
    output_format: str = "csv"
    output_dir: Path = Path("output")
    seed: int = 42
    start_time: str = DEFAULT_START_TIME
    records_limit: int | None = None
    batch_size: int = 500_000
    max_rows_per_file: int = 1_000_000
    duplicate_pct: float = 0.0
    delayed_pct: float = 0.0
    missing_pct: float = 0.0
    invalid_pct: float = 0.0
    overwrite: bool = False
    config_dir: Path = DEFAULT_CONFIG_DIR

    def validate(self) -> None:
        """Raise ConfigError on any out-of-range setting."""
        if not (self.days > 0):
            raise ConfigError("--days must be > 0")
        if not (1 <= self.interval_seconds <= 86_400):
            raise ConfigError("--interval-seconds must be between 1 and 86400")
        if self.segments_per_area < 1:
            raise ConfigError("--segments-per-area must be >= 1")
        if self.output_format not in ("csv", "jsonl", "parquet"):
            raise ConfigError("--output-format must be csv, jsonl or parquet")
        if self.seed < 0:
            raise ConfigError("--seed must be a non-negative integer")
        if self.records_limit is not None and self.records_limit < 1:
            raise ConfigError("--records-limit must be >= 1")
        if self.batch_size < 1 or self.max_rows_per_file < 1:
            raise ConfigError("--batch-size and --max-rows-per-file must be >= 1")
        pcts = [self.duplicate_pct, self.delayed_pct, self.missing_pct, self.invalid_pct]
        if any(p < 0 or p > 100 for p in pcts) or sum(pcts) > 100:
            raise ConfigError("anomaly percentages must be 0-100 and sum to <= 100")
        if self.total_steps < 1:
            raise ConfigError("--days is shorter than one --interval-seconds")
        parse_start_time(self.start_time)

    @property
    def total_steps(self) -> int:
        return int(self.days * 86_400 // self.interval_seconds)

    @property
    def anomalies_enabled(self) -> bool:
        return any([self.duplicate_pct, self.delayed_pct, self.missing_pct, self.invalid_pct])


def parse_start_time(value: str) -> datetime:
    """Parse an ISO-8601 start time. Naive values are treated as Asia/Kolkata local time."""
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ConfigError(f"--start-time is not ISO-8601: {value!r}") from exc
    dt = dt.replace(tzinfo=IST) if dt.tzinfo is None else dt.astimezone(IST)
    return dt.replace(microsecond=0)


@dataclass
class CityConfig:
    """Loaded area/segment configuration, restricted to the selected segments."""

    areas: pd.DataFrame
    segments: pd.DataFrame
    profiles: dict[str, dict[str, list[float]]]
    dow_multiplier: dict[str, float]
    monthly_rain_spells: dict[str, float]


def load_city_config(config_dir: Path, segments_per_area: int) -> CityConfig:
    """Load config/areas.json and config/road_segments.json and pick N segments per area."""
    try:
        areas_cfg = json.loads((config_dir / "areas.json").read_text(encoding="utf-8"))
        segs_cfg = json.loads((config_dir / "road_segments.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"cannot read config files in {config_dir}: {exc}") from exc

    areas = pd.DataFrame(areas_cfg["areas"])
    segments = pd.DataFrame(segs_cfg["road_segments"])
    profiles = areas_cfg["area_type_hourly_profiles"]
    for atype in areas["area_type"].unique():
        prof = profiles.get(atype)
        if not prof or any(len(prof.get(k, [])) != 24 for k in ("weekday", "weekend")):
            raise ConfigError(f"area type {atype!r} needs 24-value weekday and weekend profiles")
    missing = set(SEGMENT_COLUMNS) - set(segments.columns)
    if missing:
        raise ConfigError(f"road_segments.json missing fields: {sorted(missing)}")
    if segments["road_segment_id"].duplicated().any():
        raise ConfigError("duplicate road_segment_id in road_segments.json")
    if set(segments["road_type"]) - set(ROAD_TYPE_DEMAND):
        raise ConfigError(f"road_type must be one of {sorted(ROAD_TYPE_DEMAND)}")

    available = segments.groupby("area_id").size()
    if segments_per_area > available.min():
        raise ConfigError(f"--segments-per-area {segments_per_area} exceeds configured segments "
                          f"(max {available.min()} per area)")
    order = {a: i for i, a in enumerate(areas["area_id"])}
    segments = (segments.assign(_o=segments["area_id"].map(order))
                .sort_values(["_o", "road_segment_id"]).groupby("area_id", sort=False)
                .head(segments_per_area).drop(columns="_o").reset_index(drop=True))
    areas = areas.assign(segment_count=areas["area_id"].map(segments.groupby("area_id").size()))
    return CityConfig(areas, segments[SEGMENT_COLUMNS], profiles,
                      areas_cfg["day_of_week_multiplier"], areas_cfg["monthly_rain_spells_per_day"])


# ---- Pure traffic relationships (unit-tested directly) -----------------------------------
def congestion_score(speed_kmph: np.ndarray, speed_limit_kmph: np.ndarray, occupancy_pct: np.ndarray) -> np.ndarray:
    """0-100 score: half from speed reduction vs the limit, half from occupancy."""
    speed_ratio = np.clip(np.asarray(speed_kmph, float) / np.asarray(speed_limit_kmph, float), 0, 1)
    return 0.5 * (1 - speed_ratio) * 100 + 0.5 * np.asarray(occupancy_pct, float)


def classify_congestion(score: np.ndarray) -> np.ndarray:
    """Map congestion scores to Low/Moderate/High/Severe using CONGESTION_THRESHOLDS."""
    return CONGESTION_LABELS[np.digitize(score, CONGESTION_THRESHOLDS)]


def compute_traffic_metrics(demand_ratio: np.ndarray, weather_code: np.ndarray, incident_capacity: np.ndarray,
                            closed: np.ndarray, capacity_per_minute: np.ndarray, lane_count: np.ndarray,
                            speed_limit: np.ndarray, free_flow_ratio: np.ndarray, interval_seconds: int,
                            rng: np.random.Generator) -> dict[str, np.ndarray]:
    """Derive vehicle_count, avg_speed_kmph, occupancy_pct and congestion from demand and conditions.

    All array arguments broadcast to (n_steps, n_segments). ``demand_ratio`` is demand as a
    fraction of the nominal capacity; weather and incidents lower the effective capacity.
    """
    cap_interval = capacity_per_minute * interval_seconds / 60.0
    eff_cap = cap_interval * WEATHER_CAPACITY[weather_code] * incident_capacity
    demand = demand_ratio * cap_interval
    x = demand / np.maximum(eff_cap, 1e-9)
    throughput = np.minimum(demand, eff_cap * (1 - CAPACITY_DROP * np.clip(x - 1, 0, 1)))
    # Poisson variation around the capacity-limited mean, hard-capped at the effective capacity
    # (rounded up to a whole vehicle so short intervals on small roads can still pass one).
    counts = np.minimum(rng.poisson(np.maximum(throughput, 0)), np.ceil(eff_cap)).astype(np.int64)

    free_flow = speed_limit * free_flow_ratio * WEATHER_SPEED[weather_code]
    speed = free_flow / (1 + BPR_ALPHA * x ** BPR_BETA) * rng.normal(1.0, SPEED_NOISE_SD, x.shape)
    speed = np.round(np.clip(speed, MIN_MOVING_SPEED, speed_limit), 1)

    flow_per_lane_hour = counts * (3600.0 / interval_seconds) / lane_count
    density = flow_per_lane_hour / speed                     # vehicles per km per lane
    occupancy = np.round(np.clip(density * EFFECTIVE_VEHICLE_LENGTH_M / 10.0, 0, MAX_OCCUPANCY), 1)

    counts = np.where(closed, 0, counts)
    speed = np.where(closed, 0.0, speed)
    occupancy = np.where(closed, 0.0, occupancy)
    score = congestion_score(speed, np.broadcast_to(speed_limit, speed.shape), occupancy)
    level = np.where(closed, "Severe", classify_congestion(score))
    return {"vehicle_count": counts, "avg_speed_kmph": speed, "occupancy_pct": occupancy,
            "congestion_score": score, "congestion_level": level}


# ---- Simulator -----------------------------------------------------------------------------
class Incident(NamedTuple):
    segment: int
    start: float          # seconds from run start
    end: float
    recovery_end: float
    code: int
    duration_min: int
    capacity_factor: float


@dataclass
class TrafficSimulator:
    """Stateful simulator: plans weather and incidents per day and emits time-major batches."""

    cfg: GeneratorConfig
    city: CityConfig
    run_start: datetime = field(init=False)
    _incidents: list[Incident] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        seg, areas = self.city.segments, self.city.areas
        self.run_start = parse_start_time(self.cfg.start_time)
        area_idx = {a: i for i, a in enumerate(areas["area_id"])}
        self.area_types = sorted(areas["area_type"].unique())
        type_idx = {t: i for i, t in enumerate(self.area_types)}
        self.n_areas, self.n_seg = len(areas), len(seg)
        self.seg_area = seg["area_id"].map(area_idx).to_numpy()
        self.seg_type = areas["area_type"].map(type_idx).to_numpy()[self.seg_area]
        self.lanes = seg["lane_count"].to_numpy(float)
        self.limit = seg["speed_limit_kmph"].to_numpy(float)
        self.capacity = seg["capacity_vehicles_per_minute"].to_numpy(float)
        self.ff_ratio = seg["free_flow_ratio"].to_numpy(float)
        self.static_demand = (areas["peak_demand_ratio"].to_numpy()[self.seg_area]
                              * seg["road_type"].map(ROAD_TYPE_DEMAND).to_numpy()
                              * seg["base_demand_factor"].to_numpy())
        self.hazard = seg["road_type"].map(ROAD_TYPE_HAZARD).to_numpy()
        # Profiles sampled at half-hours, wrapped so interpolation is continuous over midnight.
        self.profile_x = np.arange(-0.5, 25.0, 1.0)
        self.profile_y = {day_kind: np.array([[p[day_kind][-1], *p[day_kind], p[day_kind][0]]
                                              for p in (self.city.profiles[t] for t in self.area_types)])
                          for day_kind in ("weekday", "weekend")}
        init_rng = np.random.default_rng([self.cfg.seed, 999_999])
        self.ar_state = init_rng.normal(0, AR_SD, self.n_seg)
        self.busy_until = np.full(self.n_seg, -np.inf)
        self.weather_carry = np.zeros((self.n_areas, 1440), np.int8)
        self.seg_ids = seg["road_segment_id"].to_numpy(str)

    # -- per-day planning --
    def _profile(self, minutes: np.ndarray, day_kind: str) -> np.ndarray:
        """Hourly profile interpolated at minute-of-day values -> (len(minutes), n_area_types)."""
        return np.stack([np.interp(minutes / 60.0, self.profile_x, y) for y in self.profile_y[day_kind]], axis=1)

    def _plan_weather(self, rng: np.random.Generator, month: int) -> np.ndarray:
        """Paint rain spells and cloudy periods on a 2-day minute grid; returns today's (n_areas, 1440) codes."""
        grid = np.zeros((self.n_areas, 2880), np.int8)
        grid[:, :1440] = self.weather_carry
        lam = float(self.city.monthly_rain_spells.get(str(month), 0.3))
        start_p = RAIN_START_HOUR_WEIGHTS / RAIN_START_HOUR_WEIGHTS.sum()

        def paint(a: int, start: float, dur: float, heavy: bool) -> None:
            s, e = int(start), int(start + dur)
            c0, c1 = max(0, s - int(rng.uniform(30, 120))), min(2880, e + int(rng.uniform(15, 60)))
            grid[a, c0:c1] = np.maximum(grid[a, c0:c1], 1)
            grid[a, s:min(e, 2880)] = np.maximum(grid[a, s:min(e, 2880)], 2)
            if heavy:
                hs, he = int(s + 0.25 * dur), min(int(s + 0.75 * dur), 2880)
                grid[a, hs:he] = 3

        def spell() -> tuple[float, float, bool]:
            start = rng.choice(24, p=start_p) * 60 + rng.uniform(0, 60)
            return start, float(np.clip(rng.lognormal(math.log(70), 0.6), 10, 300)), rng.random() < 0.3

        wetness = rng.gamma(2.0, 0.5)  # citywide wet/dry day, correlates areas
        for _ in range(rng.poisson(lam * wetness)):
            start, dur, heavy = spell()
            for a in range(self.n_areas):
                if rng.random() < 0.75:
                    paint(a, max(0.0, start + rng.normal(0, 25)), dur * rng.uniform(0.6, 1.4), heavy)
        for a in range(self.n_areas):
            for _ in range(rng.poisson(0.3 * lam * wetness)):
                paint(a, *spell())
            for _ in range(rng.poisson(0.4 + 0.8 * lam)):
                s = rng.uniform(0, 1440)
                e = int(min(2880, s + rng.uniform(60, 360)))
                grid[a, int(s):e] = np.maximum(grid[a, int(s):e], 1)
        self.weather_carry = grid[:, 1440:].copy()
        return grid[:, :1440]

    def _plan_incidents(self, rng: np.random.Generator, weather: np.ndarray, day_kind: str,
                        day_offset: float, run_seconds: float) -> None:
        """Sample incident starts/durations for the day; non-overlapping per segment."""
        minutes = np.arange(1440, dtype=float)
        demand_w = self._profile(minutes, day_kind).T                       # (n_types, 1440)
        night = np.where((minutes < 360) | (minutes >= 1320), 1.0, 0.1)
        candidates = []
        for code, spec in INCIDENT_SPECS.items():
            base = {"demand": demand_w, "night": np.broadcast_to(night, demand_w.shape),
                    "day": 0.2 + demand_w}[spec.timing][self.seg_type]      # (n_seg, 1440)
            w = base * np.asarray(spec.rain_multiplier)[weather][self.seg_area]
            lam = spec.rate_per_segment_day * (w.mean(1) / base.mean(1))
            if spec.road_type_sensitive:
                lam = lam * self.hazard
            for s in np.flatnonzero(counts := rng.poisson(lam)):
                cdf = np.cumsum(w[s])
                for _ in range(counts[s]):
                    minute = min(int(np.searchsorted(cdf, rng.random() * cdf[-1], side="right")), 1439)
                    dur = int(np.clip(rng.lognormal(math.log(spec.median_minutes), spec.sigma),
                                      spec.min_minutes, spec.max_minutes))
                    start = day_offset + minute * 60 + rng.uniform(0, 60)
                    rec = float(np.clip(spec.recovery_fraction * dur, 10, 60))
                    candidates.append(Incident(int(s), start, start + dur * 60, start + (dur + rec) * 60,
                                               code, dur, spec.capacity_factor))
        for inc in sorted(candidates, key=lambda i: i.start):
            if 0 <= inc.start < run_seconds and inc.start >= self.busy_until[inc.segment]:
                self._incidents.append(inc)
                self.busy_until[inc.segment] = inc.recovery_end
        self._incidents = [i for i in self._incidents if i.recovery_end > day_offset]

    # -- batch emission --
    def iter_batches(self) -> Iterator[pd.DataFrame]:
        """Yield time-major DataFrames of clean events (chunks never cross local midnight)."""
        cfg, interval, total = self.cfg, self.cfg.interval_seconds, self.cfg.total_steps
        chunk_steps = max(1, cfg.batch_size // self.n_seg)
        run_seconds = total * interval
        t0_local = np.datetime64(self.run_start.replace(tzinfo=None), "s")
        step, day_idx = 0, 0
        while step < total:
            day_start = self.run_start + timedelta(seconds=step * interval)
            midnight = datetime(day_start.year, day_start.month, day_start.day, tzinfo=IST)
            day_offset = (midnight - self.run_start).total_seconds()
            day_end = min(total, math.ceil((day_offset + 86_400) / interval))
            day_name = DAY_NAMES[midnight.weekday()]
            day_kind = "weekend" if midnight.weekday() >= 5 else "weekday"

            rng = np.random.default_rng([cfg.seed, day_idx, 0])
            daily = (np.exp(rng.normal(0, DAILY_SEGMENT_SD, self.n_seg))
                     * np.exp(rng.normal(0, DAILY_AREA_SD, self.n_areas))[self.seg_area])
            weather = self._plan_weather(rng, midnight.month)
            self._plan_incidents(rng, weather, day_kind, day_offset, run_seconds)
            ctx = {"day_offset": day_offset, "day_kind": day_kind, "day_name": day_name, "daily": daily,
                   "weather": weather, "dow": float(self.city.dow_multiplier.get(day_name, 1.0)),
                   "t0_local": t0_local, "date": midnight}
            for chunk_idx, c0 in enumerate(range(step, day_end, chunk_steps)):
                chunk_rng = np.random.default_rng([cfg.seed, day_idx, chunk_idx + 1])
                yield self._simulate_chunk(c0, min(c0 + chunk_steps, day_end), ctx, chunk_rng)
            step, day_idx = day_end, day_idx + 1

    def _simulate_chunk(self, c0: int, c1: int, ctx: dict, rng: np.random.Generator) -> pd.DataFrame:
        interval, n_t, n_s = self.cfg.interval_seconds, c1 - c0, self.n_seg
        steps = np.arange(c0, c1)
        secs = steps * float(interval)
        minute_of_day = (secs - ctx["day_offset"]) / 60.0

        # Demand with AR(1) noise carried across chunks/days.
        phi = AR_PHI_PER_MINUTE ** (interval / 60.0)
        shocks = rng.normal(0, AR_SD * math.sqrt(1 - phi ** 2), (n_t, n_s))
        ar = np.empty((n_t, n_s))
        state = self.ar_state
        for i in range(n_t):
            state = phi * state + shocks[i]
            ar[i] = state
        self.ar_state = state
        profile = self._profile(minute_of_day, ctx["day_kind"])[:, self.seg_type]
        demand = np.clip(profile * ctx["dow"] * self.static_demand * ctx["daily"] * (1 + ar), 0, None)

        minute_idx = np.clip(minute_of_day.astype(int), 0, 1439)
        weather = ctx["weather"][:, minute_idx].T[:, self.seg_area]          # (n_t, n_seg)

        inc_cap = np.ones((n_t, n_s))
        inc_code = np.zeros((n_t, n_s), np.int8)
        inc_dur = np.zeros((n_t, n_s), np.int32)
        lo, hi = secs[0], secs[-1] + interval
        for inc in self._incidents:
            if inc.start >= hi or inc.recovery_end <= lo:
                continue
            active = (secs >= inc.start) & (secs < inc.end)
            inc_cap[active, inc.segment] = inc.capacity_factor
            inc_code[active, inc.segment] = inc.code
            inc_dur[active, inc.segment] = inc.duration_min
            recovering = (secs >= inc.end) & (secs < inc.recovery_end)
            floor = max(inc.capacity_factor, 0.3)
            ramp = floor + (1 - floor) * (secs[recovering] - inc.end) / (inc.recovery_end - inc.end)
            inc_cap[recovering, inc.segment] = np.minimum(inc_cap[recovering, inc.segment], ramp)

        m = compute_traffic_metrics(demand, weather, inc_cap, inc_code == ROAD_CLOSURE, self.capacity,
                                    self.lanes, self.limit, self.ff_ratio, interval, rng)

        ts = ctx["t0_local"] + (steps * interval).astype("timedelta64[s]")
        iso = np.datetime_as_string(ts, unit="s")
        compact = np.char.replace(np.char.replace(iso, "-", ""), ":", "")
        latency_ms = rng.integers(*EMIT_LATENCY_MS, n_t * n_s)
        generated = (np.repeat(ts.astype("datetime64[ms]"), n_s)
                     + np.timedelta64(interval * 1000, "ms") + latency_ms.astype("timedelta64[ms]"))
        seg = self.city.segments
        date = ctx["date"]
        return pd.DataFrame({
            "event_id": np.char.add(np.tile(np.char.add(self.seg_ids, "_"), n_t), np.repeat(compact, n_s)),
            "event_timestamp": np.repeat(np.char.add(iso, "+05:30"), n_s),
            "area": np.tile(seg["area"].to_numpy(), n_t),
            "road_segment_id": np.tile(self.seg_ids, n_t),
            "road_name": np.tile(seg["road_name"].to_numpy(), n_t),
            "latitude": np.tile(seg["latitude"].to_numpy(), n_t),
            "longitude": np.tile(seg["longitude"].to_numpy(), n_t),
            "vehicle_count": m["vehicle_count"].ravel(),
            "avg_speed_kmph": m["avg_speed_kmph"].ravel(),
            "occupancy_pct": m["occupancy_pct"].ravel(),
            "congestion_level": m["congestion_level"].ravel().astype(object),
            "weather": WEATHER_LABELS[weather.ravel()],
            "incident_type": INCIDENT_LABELS[inc_code.ravel()],
            "incident_duration_minutes": pd.array(inc_dur.ravel(), dtype="Int32"),
            "is_weekend": ctx["day_kind"] == "weekend",
            "hour_of_day": np.repeat((minute_of_day // 60).astype(np.int32), n_s),
            "day_of_week": ctx["day_name"],
            "data_source": "synthetic_simulator",
            "generated_at": generated,
            "anomaly_type": "None",
            "year": f"{date.year:04d}", "month": f"{date.month:02d}", "day": f"{date.day:02d}",
        })


# ---- Anomaly injection ---------------------------------------------------------------------
def inject_anomalies(df: pd.DataFrame, cfg: GeneratorConfig, rng: np.random.Generator) -> pd.DataFrame:
    """Inject labelled data-quality anomalies into disjoint row sets of a batch.

    Duplicate: exact copy of a *clean* event (same event_id, all fields equal) re-sent 1-30 s
    later; only generated_at and anomaly_type differ. Delayed: generated_at pushed 5-120 min
    late. Missing_Field: one optional field nulled. Invalid_Value: one impossible value.
    Row order is left alone; arrival ordering happens in iter_event_batches.
    """
    if not cfg.anomalies_enabled or df.empty:
        return df
    n = len(df)
    counts = [round(n * p / 100) for p in (cfg.delayed_pct, cfg.missing_pct, cfg.invalid_pct, cfg.duplicate_pct)]
    perm = rng.permutation(n)
    delayed, missing, invalid, dup = np.split(perm[:sum(counts)], np.cumsum(counts)[:-1])
    original, df = df, df.copy()
    col = df.columns.get_loc

    if len(delayed):
        gen = df["generated_at"].to_numpy("datetime64[ms]").copy()
        gen[delayed] += (rng.uniform(5, 120, len(delayed)) * 60_000).astype("timedelta64[ms]")
        df["generated_at"] = gen
        df.iloc[delayed, col("anomaly_type")] = "Delayed"
    if len(missing):
        fields = rng.choice(OPTIONAL_FIELDS, len(missing))
        for f in OPTIONAL_FIELDS:
            rows = missing[fields == f]
            df.iloc[rows, col(f)] = None if df[f].dtype == object else np.nan
        df.iloc[missing, col("anomaly_type")] = "Missing_Field"
    if len(invalid):
        kinds = rng.integers(0, 4, len(invalid))
        df.iloc[invalid[kinds == 0], col("vehicle_count")] = -rng.integers(1, 50, (kinds == 0).sum())
        df.iloc[invalid[kinds == 1], col("avg_speed_kmph")] = np.round(rng.uniform(150, 300, (kinds == 1).sum()), 1)
        df.iloc[invalid[kinds == 2], col("occupancy_pct")] = np.round(rng.uniform(100.5, 150, (kinds == 2).sum()), 1)
        df.iloc[invalid[kinds == 3], col("latitude")] = 0.0
        df.iloc[invalid, col("anomaly_type")] = "Invalid_Value"
    if len(dup):
        copies = original.iloc[dup].copy()   # sourced from the unmodified batch: always clean events
        copies["generated_at"] = (copies["generated_at"].to_numpy("datetime64[ms]")
                                  + (rng.integers(1, 30, len(dup)) * 1000).astype("timedelta64[ms]"))
        copies["anomaly_type"] = "Duplicate"
        df = pd.concat([df, copies], ignore_index=True)
    return df


def finalize_batch(df: pd.DataFrame) -> pd.DataFrame:
    """Format generated_at as ISO-8601 +05:30 (ms precision)."""
    df["generated_at"] = np.char.add(np.datetime_as_string(df["generated_at"].to_numpy("datetime64[ms]"),
                                                           unit="ms"), "+05:30")
    return df


def _arrival_ordered(batches: Iterator[pd.DataFrame], interval_seconds: int) -> Iterator[pd.DataFrame]:
    """Re-emit batches in global generated_at (arrival) order, across batch boundaries.

    Every future clean event is emitted no earlier than
    ``next event_timestamp + interval + minimum latency``, and anomalies only move generated_at later,
    so rows below that frontier are final. Everything else (mostly Delayed / Duplicate rows)
    waits in a buffer for a later batch. Memory: one batch plus the pending late rows.
    """
    step = np.timedelta64(interval_seconds * 1000, "ms")
    pending = None
    for batch in batches:
        last_event = np.datetime64(batch["event_timestamp"].max()[:19], "ms")
        frontier = last_event + 2 * step + np.timedelta64(EMIT_LATENCY_MS[0], "ms")
        pending = batch if pending is None else pd.concat([pending, batch], ignore_index=True)
        ready = pending["generated_at"].to_numpy("datetime64[ms]") < frontier
        yield pending[ready].sort_values("generated_at", kind="stable", ignore_index=True)
        pending = pending[~ready]
    if pending is not None and len(pending):
        yield pending.sort_values("generated_at", kind="stable", ignore_index=True)


def iter_event_batches(cfg: GeneratorConfig, city: CityConfig | None = None,
                       arrival_order: bool = True) -> Iterator[pd.DataFrame]:
    """Yield final event batches (EVENT_COLUMNS + partition columns), honouring records_limit.

    With duplicate/delayed anomalies enabled and ``arrival_order`` set, the concatenated output is
    globally sorted by generated_at, like a replayable stream. Otherwise rows are in event-time order.
    """
    city = city or load_city_config(cfg.config_dir, cfg.segments_per_area)
    batches = (inject_anomalies(b, cfg, np.random.default_rng([cfg.seed, i, 7_777]))
               for i, b in enumerate(TrafficSimulator(cfg, city).iter_batches()))
    if arrival_order and (cfg.duplicate_pct or cfg.delayed_pct):
        batches = _arrival_ordered(batches, cfg.interval_seconds)
    remaining = cfg.records_limit if cfg.records_limit is not None else math.inf
    for batch in batches:
        if batch.empty:
            continue
        if len(batch) > remaining:
            batch = batch.iloc[: int(remaining)]
        remaining -= len(batch)
        yield finalize_batch(batch)
        if remaining <= 0:
            return


# ---- Output --------------------------------------------------------------------------------
def _arrow_schema():
    import pyarrow as pa
    return pa.schema([
        ("event_id", pa.string()), ("event_timestamp", pa.string()), ("area", pa.string()),
        ("road_segment_id", pa.string()), ("road_name", pa.string()), ("latitude", pa.float64()),
        ("longitude", pa.float64()), ("vehicle_count", pa.int32()), ("avg_speed_kmph", pa.float64()),
        ("occupancy_pct", pa.float64()), ("congestion_level", pa.string()), ("weather", pa.string()),
        ("incident_type", pa.string()), ("incident_duration_minutes", pa.int32()), ("is_weekend", pa.bool_()),
        ("hour_of_day", pa.int32()), ("day_of_week", pa.string()), ("data_source", pa.string()),
        ("generated_at", pa.string()), ("anomaly_type", pa.string()),
        ("year", pa.string()), ("month", pa.string()), ("day", pa.string()),
    ])


class EventWriter:
    """Streams batches to CSV/JSONL (rolled at max_rows_per_file) or Hive-partitioned Parquet."""

    def __init__(self, cfg: GeneratorConfig, expected_rows: int) -> None:
        self.cfg, self.out = cfg, Path(cfg.output_dir)
        self.single_file = expected_rows <= cfg.max_rows_per_file
        self.part, self.rows_in_part, self.batch_no = 0, 0, 0
        self.files: list[Path] = []

    def _text_path(self) -> Path:
        ext = self.cfg.output_format
        name = f"traffic_events.{ext}" if self.single_file else f"traffic_events_part-{self.part:05d}.{ext}"
        return self.out / name

    def write(self, df: pd.DataFrame) -> None:
        if self.cfg.output_format == "parquet":
            self._write_parquet(df)
            return
        df = df[EVENT_COLUMNS]
        while len(df):
            room = len(df) if self.single_file else self.cfg.max_rows_per_file - self.rows_in_part
            chunk, df = df.iloc[:room], df.iloc[room:]
            path = self._text_path()
            first = self.rows_in_part == 0
            if first:
                self.files.append(path)
            if self.cfg.output_format == "csv":
                chunk.to_csv(path, mode="w" if first else "a", header=first, index=False, lineterminator="\n")
            else:
                text = chunk.to_json(orient="records", lines=True, force_ascii=False)
                with open(path, "w" if first else "a", encoding="utf-8", newline="\n") as fh:
                    fh.write(text if text.endswith("\n") else text + "\n")
            self.rows_in_part += len(chunk)
            if not self.single_file and self.rows_in_part >= self.cfg.max_rows_per_file:
                self.part, self.rows_in_part = self.part + 1, 0

    def _write_parquet(self, df: pd.DataFrame) -> None:
        import pyarrow as pa
        import pyarrow.dataset as ds
        schema = _arrow_schema()
        table = pa.Table.from_pandas(df[schema.names], schema=schema, preserve_index=False)
        part_schema = pa.schema([schema.field(c) for c in PARTITION_COLUMNS])
        max_rows = self.cfg.max_rows_per_file
        ds.write_dataset(table, self.out / "traffic_events", format="parquet",
                         partitioning=ds.partitioning(part_schema, flavor="hive"),
                         basename_template=f"part-{self.batch_no:05d}-{{i}}.parquet",
                         existing_data_behavior="overwrite_or_ignore",
                         max_rows_per_file=max_rows, max_rows_per_group=min(max_rows, 256_000))
        self.batch_no += 1

    def close(self) -> list[Path]:
        if self.cfg.output_format == "parquet":
            self.files = sorted((self.out / "traffic_events").rglob("*.parquet"))
        return self.files


def event_output_paths(out: Path) -> list[Path]:
    """Existing event outputs this tool owns (checked before writing)."""
    return sorted(out.glob("traffic_events*"))


def write_metadata(cfg: GeneratorConfig, city: CityConfig) -> None:
    """Write areas.csv, road_segments.csv and a copy of the event schema."""
    out = Path(cfg.output_dir)
    city.areas.to_csv(out / "areas.csv", index=False, lineterminator="\n")
    city.segments.to_csv(out / "road_segments.csv", index=False, lineterminator="\n")
    shutil.copyfile(SCHEMA_PATH, out / "traffic_event_schema.json")


@dataclass
class GenerationSummary:
    expected_rows: int
    rows_written: int
    files: list[Path]
    seconds: float


def expected_record_count(cfg: GeneratorConfig, n_segments: int) -> int:
    """Rows the run will produce: steps x segments (+ duplicates), capped by records_limit."""
    base = cfg.total_steps * n_segments
    rows = base + round(base * cfg.duplicate_pct / 100)
    return min(rows, cfg.records_limit) if cfg.records_limit else rows


def generate_dataset(cfg: GeneratorConfig) -> GenerationSummary:
    """Validate config, write metadata, then stream all event batches to disk."""
    cfg.validate()
    city = load_city_config(cfg.config_dir, cfg.segments_per_area)
    out = Path(cfg.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    existing = event_output_paths(out)
    if existing:
        if not cfg.overwrite:
            raise ConfigError(f"{out} already contains event output ({existing[0].name}); use --overwrite")
        for p in existing:
            shutil.rmtree(p) if p.is_dir() else p.unlink()

    n_seg = len(city.segments)
    expected = expected_record_count(cfg, n_seg)
    steps_per_day = 86_400 / cfg.interval_seconds
    log.info("SYNTHETIC data only - not real Bengaluru traffic measurements.")
    log.info("Plan: %s days x %.0f steps/day x %d segments (%d areas) = %s base records; expected output %s records%s",
             cfg.days, steps_per_day, n_seg, len(city.areas), f"{cfg.total_steps * n_seg:,}", f"{expected:,}",
             f" (limit {cfg.records_limit:,})" if cfg.records_limit else "")
    write_metadata(cfg, city)

    writer = EventWriter(cfg, expected)
    started, written, last_log = time.monotonic(), 0, 0.0
    # Parquet row order carries no meaning and late rows would create extra tiny files, so only
    # the streamable text formats are re-ordered into arrival order.
    for batch in iter_event_batches(cfg, city, arrival_order=cfg.output_format != "parquet"):
        writer.write(batch)
        written += len(batch)
        elapsed = time.monotonic() - started
        if elapsed - last_log >= 2 or written >= expected:
            rate = written / max(elapsed, 1e-9)
            eta = max(expected - written, 0) / max(rate, 1e-9)
            log.info("%s / %s rows (%.1f%%) | %s rows/s | ETA %.0fs | through %s", f"{written:,}", f"{expected:,}",
                     100 * written / max(expected, 1), f"{rate:,.0f}", eta, batch["event_timestamp"].iloc[-1])
            last_log = elapsed
    files = writer.close()
    summary = GenerationSummary(expected, written, files, time.monotonic() - started)
    log.info("Done: %s rows in %d file(s) under %s in %.1fs", f"{written:,}", len(files), out.resolve(), summary.seconds)
    return summary


# ---- CLI -----------------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate a SYNTHETIC Bengaluru traffic event dataset.",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    d = GeneratorConfig()
    p.add_argument("--days", type=float, default=d.days, help="days of history to simulate")
    p.add_argument("--interval-seconds", type=int, default=d.interval_seconds, help="observation interval")
    p.add_argument("--segments-per-area", type=int, default=d.segments_per_area, help="road segments per area (max 20)")
    p.add_argument("--output-format", choices=["csv", "jsonl", "parquet"], default=d.output_format)
    p.add_argument("--output-dir", type=Path, default=d.output_dir)
    p.add_argument("--seed", type=int, default=d.seed, help="random seed (reproducible output)")
    p.add_argument("--start-time", default=d.start_time, help="ISO-8601 start; naive = Asia/Kolkata")
    p.add_argument("--records-limit", type=int, default=None, help="stop after this many records")
    p.add_argument("--batch-size", type=int, default=d.batch_size, help="max rows simulated per batch")
    p.add_argument("--max-rows-per-file", type=int, default=d.max_rows_per_file,
                   help="roll CSV/JSONL files and cap Parquet files at this many rows")
    p.add_argument("--duplicate-pct", type=float, default=0.0, help="%% of records re-sent as duplicates")
    p.add_argument("--delayed-pct", type=float, default=0.0, help="%% of records emitted late")
    p.add_argument("--missing-pct", type=float, default=0.0, help="%% of records with a nulled optional field")
    p.add_argument("--invalid-pct", type=float, default=0.0, help="%% of records with an invalid value")
    p.add_argument("--overwrite", action="store_true", help="replace existing event output in --output-dir")
    p.add_argument("--config-dir", type=Path, default=DEFAULT_CONFIG_DIR)
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    cfg = GeneratorConfig(**{k: v for k, v in vars(args).items()})
    try:
        generate_dataset(cfg)
    except ConfigError as exc:
        log.error("%s", exc)
        return 2
    except OSError as exc:
        log.error("I/O error: %s", exc)
        return 1
    except KeyboardInterrupt:
        log.error("Interrupted - output in %s is incomplete", cfg.output_dir)
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
