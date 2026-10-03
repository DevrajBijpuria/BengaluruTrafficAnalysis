# Bengaluru Synthetic Traffic Dataset Generator

Generates a large, realistic, **fully synthetic** road-traffic dataset for 10 Bengaluru areas. It is the
data source for a Real-Time Traffic Intelligence Platform demo (Kafka → PySpark Structured Streaming →
Amazon S3 → Snowflake → dbt → Power BI). This module only generates the data; the pipeline code lives in the rest of the repository.

> **This is simulated data. It is not Bengaluru traffic data.** Road names refer to real Bengaluru roads
> so the data looks familiar. The coordinates are approximate, and the lane counts, speed limits,
> capacities, traffic volumes, speeds, weather and incidents are all produced by a statistical model.
> Every record has `data_source = "synthetic_simulator"`. Do not use this data for real traffic
> decisions, or present it as measured data.

## Project layout

```
traffic_generator.py              # generator + CLI (single module)
config/areas.json                 # areas, area-type hourly demand profiles, weekday & monthly-rain factors
config/road_segments.json         # 20 synthetic road segments per area (200 total)
schemas/traffic_event_schema.json # JSON Schema (draft 2020-12) for one event, with SQL type hints
requirements.txt
pytest.ini
tests/test_generator.py           # counts, intervals, constraints, relationships, reproducibility, outputs
tests/test_schema.py              # schema validity, generated records vs schema, config sanity
```

## Quick start

```bash
pip install -r requirements.txt
```

Small development dataset (1 day, 5-minute interval, 2 segments per area, 5,760 rows):

```bash
python traffic_generator.py --days 1 --interval-seconds 300 --segments-per-area 2 --output-dir output/dev
```

Default 30-day history (10 areas × 10 segments × 60 s, 4,320,000 rows, about 10 s as Parquet or about 35 s as CSV):

```bash
python traffic_generator.py --output-format parquet --output-dir output/30d
```

Large dataset (90 days × 20 segments per area = 25,920,000 rows):

```bash
python traffic_generator.py --days 90 --segments-per-area 20 --output-format parquet --output-dir output/90d
```

Streaming-style JSONL with data-quality anomalies for testing Kafka and Spark:

```bash
python traffic_generator.py --days 7 --output-format jsonl --duplicate-pct 1 --delayed-pct 2 --missing-pct 1 --invalid-pct 0.5 --output-dir output/dq
```

Fixed number of records from a custom start time:

```bash
python traffic_generator.py --start-time 2026-09-01T06:00:00 --records-limit 250000 --seed 7 --output-dir output/sample
```

Run the tests:

```bash
python -m pytest
```

## CLI options

| Option | Default | Meaning |
|---|---|---|
| `--days` | `30` | Days to simulate (fractions allowed, e.g. `0.5`) |
| `--interval-seconds` | `60` | One observation per segment per interval (1–86400) |
| `--segments-per-area` | `10` | Segments per area, from 1 to 20 (the first N from `road_segments.json`) |
| `--output-format` | `csv` | `csv`, `jsonl` or `parquet` |
| `--output-dir` | `output` | Destination directory |
| `--seed` | `42` | Random seed. The same seed and settings give byte-identical output |
| `--start-time` | `2026-07-01T00:00:00+05:30` | ISO-8601 start time. A time without an offset is read as Asia/Kolkata |
| `--records-limit` | none | Stop after this many output records |
| `--batch-size` | `500000` | Maximum rows simulated per in-memory batch |
| `--max-rows-per-file` | `1000000` | Roll CSV/JSONL files at this size, and cap Parquet files at it |
| `--duplicate-pct`, `--delayed-pct`, `--missing-pct`, `--invalid-pct` | `0` | Anomaly injection. Percentage of records affected (see below) |
| `--overwrite` | off | Replace existing `traffic_events*` output in `--output-dir` |
| `--config-dir` | `config/` | Use a different areas and segments configuration |

The generator prints the planned and expected record count before it starts. It logs progress (rows, %,
rows/s, ETA) at least every 2 s. It exits with code `2` on a configuration error and `1` on an I/O error.
Memory use is limited by `--batch-size`. Batches never cross local midnight.

## Output files

| File | Contents |
|---|---|
| `traffic_events.csv` / `traffic_events.jsonl` | Events. If the expected row count is larger than `--max-rows-per-file`, the output is split into `traffic_events_part-00000.csv`, `…-00001.csv`, … |
| `traffic_events/year=YYYY/month=MM/day=DD/area=<name>/part-*.parquet` | Parquet events, Hive-partitioned. Snappy compression, row groups of up to 256k rows |
| `road_segments.csv` | Metadata for the selected segments (see below) |
| `areas.csv` | Area metadata: id, name, type, centre coordinates, peak demand ratio, description, segment count |
| `traffic_event_schema.json` | Copy of the event schema |

Parquet notes:
* Partition columns (`year`, `month`, `day`, `area`) are stored in the directory path, not in the files.
  Readers such as Spark, `pyarrow.dataset` and Snowflake external tables (via `METADATA$FILENAME`) recover them.
* Area names are URI-encoded in paths, for example `area=Electronic%20City`. Spark and pyarrow decode them automatically.
* With the defaults, each day × area partition is one file of 14,400 rows (about 330 KB). That is the
  finest grain the requested partition scheme allows. For bigger files, raise `--segments-per-area`, or
  lower the interval (at 10 s a partition holds 86,400 rows per 10 segments). Compaction downstream is another option.

### `road_segments.csv`

`road_segment_id`, `area_id`, `area`, `road_name`, `latitude`, `longitude`, `road_type`
(Highway/Arterial/Collector/Local), `lane_count`, `speed_limit_kmph`, `capacity_vehicles_per_minute`,
plus two model parameters: `free_flow_ratio` (typical uncongested speed ÷ limit) and
`base_demand_factor` (the segment's relative demand).

| Road type | Lanes | Speed limit (km/h) | Capacity per lane (veh/min) |
|---|---|---|---|
| Highway (flyovers, elevated, NH, ORR main carriageway) | 6–8 | 60 (80 on elevated/Hebbal flyover) | 30 |
| Arterial | 4–6 | 50 | 15 |
| Collector | 2–4 | 40 | 12 |
| Local | 2 | 30 | 8 |

## Event schema

Full definition: [`schemas/traffic_event_schema.json`](schemas/traffic_event_schema.json). All formats use the same field names, order and representation.

| Field | Type | Notes |
|---|---|---|
| `event_id` | string | `<road_segment_id>_<YYYYMMDDTHHMMSS>`. Unique per segment and interval, and stable across reruns (idempotent MERGE key) |
| `event_timestamp` | string (ISO-8601) | Start of the interval, `+05:30`, e.g. `2026-07-01T08:15:00+05:30` |
| `area` | string | One of the 10 areas |
| `road_segment_id` | string | `WFD-001` … joins to `road_segments.csv` |
| `road_name` | string, nullable* | |
| `latitude`, `longitude` | float, nullable* | Synthetic, 6 dp |
| `vehicle_count` | int | Vehicles in the interval (all lanes) |
| `avg_speed_kmph` | float (1 dp) | Never above `speed_limit_kmph`. `0` only during `Road_Closure` |
| `occupancy_pct` | float (1 dp), nullable* | 0–98 |
| `congestion_level` | string | `Low`, `Moderate`, `High`, `Severe` |
| `weather` | string, nullable* | `Clear`, `Cloudy`, `Rain`, `Heavy_Rain` |
| `incident_type` | string | `None`, `Accident`, `Roadwork`, `Vehicle_Breakdown`, `Road_Closure` |
| `incident_duration_minutes` | int, nullable* | Total duration of the active incident. `0` when `None` |
| `is_weekend` | bool | Saturday or Sunday |
| `hour_of_day` | int | 0–23, IST |
| `day_of_week` | string | `Monday` … `Sunday` |
| `data_source` | string | Always `synthetic_simulator` |
| `generated_at` | string (ISO-8601, ms) | Simulated sensor emission time: interval end + 0.2–5 s latency |
| `anomaly_type` | string | `None` for clean rows. Otherwise `Duplicate`, `Delayed`, `Missing_Field`, `Invalid_Value` |

\* These fields are null only in records with `anomaly_type = Missing_Field`. Clean records never contain nulls.

Timestamps are strings in every format (CSV, JSONL and Parquet), so one contract serves all three.
Cast them in Spark with `to_timestamp`, or in Snowflake with `TIMESTAMP_TZ`. Asia/Kolkata is written as a
fixed `+05:30`, which is correct because IST has no daylight saving time.

> **Gotcha:** the category value is the literal string `"None"`. pandas `read_csv` turns it into NaN by
> default, so use `keep_default_na=False, na_values=[""]`. Spark and Snowflake `COPY INTO` keep it as
> a string. In Snowflake, set `NULL_IF=('')` and `EMPTY_FIELD_AS_NULL=TRUE` for CSV.

## Traffic model

Records are produced from a set of linked rules, not independent random values. For each segment `s` at time `t`:

**1. Demand** (as a fraction of nominal capacity)

```
demand_ratio = profile[area_type, weekday|weekend](t)     # hourly curve, interpolated per minute
             × day_of_week_multiplier                      # Fri 1.05, Sun 0.90, ...
             × area.peak_demand_ratio                      # Silk Board 1.45 ... Yeshwanthpur 1.00
             × road_type_factor                            # Highway/Arterial 1.0, Collector 0.8, Local 0.55
             × segment.base_demand_factor                  # 0.85–1.15
             × daily_factor                                # lognormal: segment sd 0.07 × area sd 0.05, redrawn daily
             × (1 + AR(1) noise)                           # minute-level, phi = 0.97/min, sd 0.07
```

Area types and their hourly profiles are in `config/areas.json`:

| Area type | Areas | Pattern |
|---|---|---|
| `tech_corridor` | Whitefield, Electronic City, Outer Ring Road | Sharp weekday peaks from 8–11 AM and 5–9 PM. Weekend traffic is about half |
| `major_junction` | Silk Board, Marathahalli, Hebbal | High all day with peaks. Weekends stay busy |
| `commercial` | MG Road, Indiranagar | Heavier in the evening and late at night. Weekend evenings are as busy as weekdays |
| `residential` | Koramangala, Yeshwanthpur | The morning outbound peak is strongest. Moderate weekends |

**2. Effective capacity**: `capacity × interval/60 × weather_capacity × incident_capacity`

| Weather | Capacity | Free-flow speed |
|---|---|---|
| Clear | 1.00 | 1.00 |
| Cloudy | 0.97 | 0.98 |
| Rain | 0.85 | 0.88 |
| Heavy_Rain | 0.70 | 0.72 |

**3. Volume, speed and occupancy**

```
x              = demand / effective_capacity
vehicle_count  = min( Poisson( min(demand, eff_capacity × (1 − 0.15 × clip(x − 1, 0, 1))) ),   # capacity drop when oversaturated
                      ceil(eff_capacity) )                                                     # hard physical cap
avg_speed      = clip( free_flow / (1 + 1.0 × x^2.5) × N(1, 0.05),  3,  speed_limit )     # BPR-style volume-delay curve
free_flow      = speed_limit × free_flow_ratio × weather_speed
occupancy_pct  = clip( (vehicle_count/lane/hour ÷ avg_speed) × 6.5 m ÷ 10,  0, 98 )       # density × effective vehicle length
```

So higher demand gives lower speed and higher occupancy. Two limits always hold:
- `vehicle_count ≤ ceil(capacity_vehicles_per_minute × interval_seconds / 60)`. The count varies
  like a Poisson draw around a mean that respects capacity, and the draw is cut off at the effective
  capacity for that interval, which is lower during rain and incidents. It is rounded up to a whole
  vehicle so that very short intervals on small roads can still let one vehicle through. Near
  saturation, some counts pile up exactly at the cap.
- `avg_speed_kmph ≤ speed_limit_kmph`.

Both limits are enforced by tests and make good dbt tests too.

**4. Congestion level**

```
congestion_score = 0.5 × (1 − avg_speed / speed_limit) × 100  +  0.5 × occupancy_pct      # 0–100
Low < 25 ≤ Moderate < 45 ≤ High < 65 ≤ Severe                    (Road_Closure is always Severe)
```

The score is calculated from the rounded published values. Anyone can recompute `congestion_level`
from a record plus `road_segments.csv`, which makes it a good dbt test.

**5. Weather** (area level, planned per day)
- Rain spells per day come from `monthly_rain_spells_per_day`, which follows Bengaluru's
  seasons (dry Dec–Mar, pre-monsoon April–May, monsoon June–October). A citywide "wet day" factor
  (Gamma(2, 0.5)) is also drawn, so on a rainy day most areas get rain.
- Citywide spells reach each area with 75% probability, shifted by ±25 min. There are also some local spells.
- Spells usually start in the afternoon or evening. Durations are lognormal with a median of 70 min.
  30% of spells have a heavy-rain core. Each spell has cloudy periods before and after it, and there are extra random cloudy periods.

**6. Incidents** (per segment, sampled per day, never overlapping on one segment)

| Type | Rate / segment / day | Median duration (min, range) | Capacity while active | Timing | Rain effect |
|---|---|---|---|---|---|
| Accident | 0.040 × road hazard | 40 (15–180) | 45% | Follows demand | ×2 rain, ×3 heavy |
| Vehicle_Breakdown | 0.120 × road hazard | 25 (10–90) | 70% | Follows demand | ×1.5 / ×2.5 |
| Roadwork | 0.015 | 180 (60–480) | 60% | Mostly 22:00–06:00 | Less likely in rain |
| Road_Closure | 0.004 | 60 (20–300) | 0% (count = speed = occupancy = 0) | Daytime-weighted | ×1.5 heavy |

The road hazard factor is Highway 1.5, Arterial 1.2, Collector 0.8 and Local 0.5. After an incident
clears, a **recovery period** follows, lasting 25–50% of the incident's duration (10–60 min).
During recovery, capacity climbs linearly back to 100%. The rows show `incident_type = None`, but some
congestion remains.

## Data-quality anomaly injection (off by default)

Each percentage applies to the clean rows of each batch. Anomaly sets never overlap, and each affected row is labelled in `anomaly_type`.

| `anomaly_type` | Flag | What is injected |
|---|---|---|
| `Duplicate` | `--duplicate-pct` | A copy of a **clean** event (`anomaly_type = None`), taken from the unmodified batch: same `event_id` and identical values, re-sent 1–30 s later. Only `generated_at` and `anomaly_type` differ. Adds rows. Malformed events are never duplicated, so dedup and validation can be tested separately |
| `Delayed` | `--delayed-pct` | `generated_at` is 5–120 min after the event (for testing watermarks and late data) |
| `Missing_Field` | `--missing-pct` | One optional field set to null: `road_name`, `latitude`, `longitude`, `occupancy_pct`, `weather` or `incident_duration_minutes` |
| `Invalid_Value` | `--invalid-pct` | One of: negative `vehicle_count`, `avg_speed_kmph` of 150–300, `occupancy_pct` above 100, or `latitude = 0` |

**Row order.** With anomalies off, output is in event-time order: every segment for time *t*, then every
segment for *t + interval*. When duplicates or delays are enabled, CSV and JSONL output is in **global
arrival order**: the whole file set is sorted by `generated_at`, including across batch and file
boundaries. A late record appears after the later events that were emitted before it, so replaying
the file line by line behaves like a real stream. This works through a small buffer. No future clean
event can be emitted before `next event_timestamp + interval + 200 ms`. Rows earlier than that point
are written. Later rows, mostly Delayed and Duplicate ones, are held until a later batch. Memory stays at
one batch plus the pending late rows. Parquet output is left in event-time order within each batch,
because Parquet row order has no meaning and carrying rows forward would create extra tiny files in old partitions.

## Reproducibility

The same `--seed` with the same settings produces identical files. Each day, batch and anomaly pass uses
its own seeded random stream (`numpy.random.default_rng([seed, …])`). Turning anomaly injection on does
not change the underlying traffic values. `--records-limit` only truncates the output. `generated_at` is
simulated, not the wall-clock time, so it is reproducible too.

## Ingestion hints

- **Kafka**: replay CSV or JSONL line by line, keyed by `road_segment_id`. Because the file is in global
  arrival order, each partition gets its segments' records in `generated_at` order, which is the only
  ordering Kafka guarantees. Use `event_timestamp` as the event time and `generated_at` as the producer
  or record timestamp. Delayed records then arrive out of event-time order, as intended.
- **Spark**: build the `StructType` from the schema's `properties`. Set the watermark on
  `to_timestamp(event_timestamp)`. `Delayed` records test late-data handling.
- **Snowflake**: the schema's `x-sql-type` hints map to table DDL. `event_id` is the MERGE key,
  and deduplicating on it removes the `Duplicate` rows.

## Assumptions and limitations

- **Synthetic only.** No real sensor, GPS, probe or government data was used. Profiles, capacities,
  rates and effects are informed guesses about urban Indian traffic, not calibrated measurements.
- Coordinates are points spread within about 2 km of approximate area centres. They are not snapped to
  road geometry, and segments have no length or direction.
- Segments are independent: there is **no network model**, so queues do not spill back into neighbouring
  segments, and an incident does not divert traffic elsewhere. Only weather is shared, and only across an area.
- Vehicle mix (two-wheelers, autos, buses) is not modelled. `vehicle_count` is a generic count.
- Weather follows simple seasonal rules. It is not based on IMD data, and it has no effect on demand, only on capacity and speed.
- Public holidays, festivals, cricket matches, metro works and other special events are not modelled
  (apart from random `Road_Closure` events).
- Occupancy comes from a simple density × vehicle-length relationship. Real loop detectors behave differently.
- Parquet timestamps are stored as ISO strings for a single cross-format contract, not as native timestamp types.
- The weekly and daily variation is realistic enough for analytics demos such as peak detection, area
  comparison, weather impact, incident impact and data-quality handling. It is not suitable for traffic
  engineering, forecasting benchmarks or policy work.
