# Architecture

## Goal

The platform implements the complete data lifecycle for AeroSense IoT readings:

```text
Generation -> Kafka ingestion -> Spark processing -> Data lake -> API -> Analytics
```

The design favours a minimal but production-aware architecture: reliable Kafka writes, deterministic partitioning, explicit streaming schemas, checkpointed sinks, and reproducible local execution.

## Components

### Kafka

The Kafka layer runs as three Confluent Kafka 7.5 brokers in KRaft mode. The `sensor-events` topic has three partitions and replication factor three. Messages are keyed by `sensor`, so all readings for a given sensor type are consistently routed to the same partition and preserve order within that key.

### Producer

`src/producer.py` emits realistic readings for `temperature`, `humidity`, and `pressure`. It uses:

- `acks="all"`
- `retries=5`
- `max_in_flight_requests_per_connection=1`
- batching through `linger_ms=20` and `batch_size=32768`

At least one event out of ten is forced to be anomalous, which guarantees enough test data for the Spark anomaly detection chain.

### Spark streaming pipeline

`src/spark_pipeline.py` reads Kafka in streaming mode, stores raw JSON, parses payloads with an explicit schema, validates physically plausible values, computes an independent `is_anomaly` flag, applies a 2-minute watermark, and computes 5-minute sensor averages.

It writes three independent sinks:

- Raw zone: immutable Kafka payloads partitioned by ingestion time.
- Curated zone: cleaned event records partitioned by event time and sensor type.
- Consumption zone: 5-minute aggregate windows partitioned by use case and sensor type.

Each sink has its own checkpoint directory under `/tmp/datalake/_checkpoints`.

### Data lake

The data lake follows the requested three-zone layout:

```text
/tmp/datalake/
|-- raw/
|   `-- source=kafka/topic=sensor-events/year=YYYY/month=MM/day=DD/hour=HH/
|-- curated/
|   `-- domain=iot/sensor_type=.../year=YYYY/month=MM/day=DD/
`-- consumption/
    `-- use_case=sensor_averages/sensor_type=.../year=YYYY/month=MM/
```

Raw partitions are based on ingestion time, while curated and consumption partitions are based on event time. This makes replay/debugging and business analysis separate concerns.

### Analytics

`src/analytics.py` reads the curated zone and runs the four required analyses:

1. Top 5 anomaly hours.
2. Statistics per sensor type.
3. Daily temperature evolution.
4. Partition pruning timing comparison.

Results are printed and written as CSV folders under `outputs/analytics/`.

### REST API

`api/app.py` exposes health, sensor list, latest Kafka reading, daily stats, recent anomalies, and Kafka publishing endpoints. It validates query parameters and request bodies strictly and returns consistent JSON for success and errors.

## Data Contract

Kafka messages follow this JSON structure:

```json
{
  "sensor": "temperature",
  "value": 27.45,
  "unit": "C",
  "timestamp": 1737543600000,
  "source": "site-A-rack-12",
  "anomaly": false
}
```

The Spark job intentionally recomputes anomaly detection in `is_anomaly` rather than trusting the producer field. This protects the processing layer from bad or malicious producers.

