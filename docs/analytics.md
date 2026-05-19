# Analytics

`src/analytics.py` reads the curated Parquet zone and writes one CSV folder per query under `outputs/analytics/`.

Run:

```bash
spark-submit src/analytics.py
```

## Query 1: Top 5 Anomaly Hours

Purpose: identify the time windows where operational incidents are concentrated.

Output folder:

```text
outputs/analytics/top_anomaly_hours
```

Columns:

- `hour_utc`
- `anomaly_count`
- `observation_count`

Interpretation: a high anomaly count with a high observation count may indicate a real site condition; a high anomaly count with a low observation count may indicate a broken sensor.

## Query 2: Sensor Statistics

Purpose: compare each sensor type across the whole curated lake.

Output folder:

```text
outputs/analytics/sensor_statistics
```

Columns:

- `sensor_type`
- `global_mean`
- `min_value`
- `max_value`
- `stddev_value`
- `anomaly_rate_percent`
- `observation_count`

Interpretation: anomaly rate is the most useful cross-sensor metric because the value scales are different.

## Query 3: Daily Temperature Evolution

Purpose: detect day-level temperature drifts and daily anomaly clusters.

Output folder:

```text
outputs/analytics/temperature_daily_evolution
```

Columns:

- `day_utc`
- `mean_temperature`
- `anomaly_count`
- `observation_count`

Interpretation: a rising daily mean combined with a rising anomaly count can indicate overheating in monitored equipment.

## Query 4: Partition Pruning

Purpose: show that Hive-style partitions reduce the amount of data Spark scans.

Output folder:

```text
outputs/analytics/partition_pruning_demo
```

The script executes:

1. an unfiltered count over the curated zone;
2. a filtered count on partition columns `sensor_type`, `year`, and `month`;
3. a speedup calculation.

Columns:

- `filter_description`
- `unfiltered_seconds`
- `filtered_seconds`
- `speedup_factor`
- `unfiltered_count`
- `filtered_count`

Expected result: on a small local dataset the speedup may be modest because startup overhead dominates. On a larger lake, partition pruning avoids scanning unrelated sensor types and months, so the filtered query becomes significantly cheaper.

## Notes

The analytics job uses the curated zone, not raw data, because curated records have already been schema-validated, filtered for physical plausibility, and enriched with the independently computed `is_anomaly` field.

