# Reflection Questions

## 1. Crash after raw write but before curated write

The raw zone may contain a Kafka payload that is not yet present in curated data. Separate checkpoints prevent this from becoming permanent: the curated query resumes from its own last committed Kafka offset and reprocesses the missing batch. A single shared checkpoint across multiple sinks would be unsafe because it would mix progress from sinks that commit at different moments.

## 2. Producer scaled to 50,000 messages per second

The first bottlenecks would likely be partition count, broker disk throughput, network bandwidth, and Spark micro-batch latency. I would increase Kafka partitions, tune producer compression and batching, monitor broker I/O, and scale Spark executors. Consumer lag is the main signal for deciding whether Kafka ingestion or Spark processing is behind.

## 3. Kafka as source of truth vs Parquet data lake

Kafka is a strong operational source of truth for recent ordered events, replay, and independent consumer offsets. It is expensive and inconvenient for long analytical history. A Parquet data lake is better for historical retention, column pruning, partition pruning, and batch SQL. In this design Kafka is the operational log, while Parquet is the analytical source.

## 4. Broken sensor emitting aberrant values for two hours

Physically impossible values are filtered during validation. Plausible but abnormal values are preserved with `is_anomaly=true`, so analytics can reveal a sustained anomaly spike by time and sensor type. In production I would add a quarantine or `quality_status` column to isolate suspicious data without deleting it.

## 5. Adding a new `co2` sensor type

Update `src/producer.py` with unit and normal/anomalous ranges, update `src/spark_pipeline.py` validation and anomaly rules, and update `api/app.py` sensor validation. Documentation and curl examples should also mention `co2`. The Kafka topic can remain the same because key-based partitioning already works for new sensor values.

