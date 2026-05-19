# Reflection Questions

## 1. Crash after raw write but before curated write

If the pipeline writes a micro-batch to the raw zone and crashes before the curated sink commits, the lake temporarily contains the Kafka payload but not the cleaned analytical record. This is acceptable for recovery because the Kafka source offset for the curated query is stored in its own checkpoint directory. On restart, the curated query resumes from its last committed offset and processes the missing batch. The important design point is to use a separate checkpoint per sink; sharing one checkpoint across raw, curated, and consumption would mix progress metadata and could make recovery inconsistent.

## 2. Scaling the producer to 50,000 messages per second

The first bottlenecks would probably be producer batching, broker disk throughput, partition count, and Spark micro-batch processing time. Three partitions are enough for the exam, but not necessarily for 50,000 messages per second. I would increase partitions, tune producer compression and batch size, verify broker disk and network metrics, and scale Spark executors. I would also monitor consumer lag to know whether Kafka ingestion or Spark processing is the limiting stage.

## 3. Kafka as source of truth vs Parquet data lake

Kafka is strong as a short- to medium-term source of truth for replayable event streams, especially when consumers need ordering, low latency, and independent offsets. It is weaker for long historical retention because storage is expensive and analytical scans are awkward. A Parquet data lake is better for long-term history, partition pruning, batch analytics, and cheap columnar storage. I would use Kafka as the operational event log and Parquet as the historical analytical source.

## 4. Broken sensor emitting aberrant values for two hours

The pipeline detects bad values in two ways. Physically impossible values are filtered out during validation, while plausible but abnormal values are kept and marked with `is_anomaly`. If a sensor emits abnormal values for two hours, analytics will show a sustained anomaly rate spike by sensor type and time. To isolate without deleting, I would write suspicious records to a quarantine zone or add a `quality_status` column such as `valid`, `anomalous`, or `quarantined`.

## 5. Adding a new `co2` sensor type

The required changes are precise. In `src/producer.py`, add `co2` to `SENSOR_CONFIG` with unit and ranges. In `src/spark_pipeline.py`, extend the validation predicate and anomaly rule. In `api/app.py`, add `co2` to `SENSORS` and define semantic validation. In documentation, update the schema description, API examples, and technical notes. No Kafka topic change is required because the key-based partitioning already works for new sensor values, although a higher event volume may justify more partitions.

