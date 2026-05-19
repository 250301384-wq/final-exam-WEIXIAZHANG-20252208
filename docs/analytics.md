# Analytics

Run analytics only after the Spark pipeline has written curated Parquet data:

```bash
spark-submit src/analytics.py
```

The script prints results to stdout and writes real outputs under `outputs/analytics/`.

## Required Queries

1. Top 5 hours with the highest number of anomalies.
2. Per sensor type: global mean, min, max, standard deviation, and anomaly rate percentage.
3. Daily mean and anomaly count for the `temperature` sensor.
4. Partition pruning timing comparison.

## Output Files

- `outputs/analytics/top_anomaly_hours/`
- `outputs/analytics/sensor_statistics/`
- `outputs/analytics/temperature_daily_evolution/`
- `outputs/analytics/partition_pruning_demo/`
- `outputs/analytics/summary.md`
- `outputs/analytics/partition_pruning_explain.txt`

`summary.md` is generated from the actual Spark result rows. It is the file to use for numeric excerpts in the final submission.

## Partition Pruning

The pruning demo intentionally does not cache the curated dataframe before timing. It runs:

1. an unfiltered count over the curated zone;
2. a filtered count using partition columns `sensor_type`, `year`, and `month`;
3. a speedup calculation;
4. `explain(mode="extended")` for the filtered dataframe.

On a small local dataset, the speedup can be modest because Spark startup and metadata overhead dominate. The important evidence is that `partition_pruning_explain.txt` shows partition filters for `sensor_type`, `year`, and `month`.

## Interpretation

Use the top anomaly hours to identify incident periods. Use per-sensor statistics to compare anomaly rates across different scales. Use daily temperature evolution to detect drift. Use the pruning plan to justify the curated-zone partition strategy.

