# AeroSense IoT Data Engineering Platform

This repository is a complete practical implementation of an end-to-end data engineering platform for IoT sensor readings. It covers generation, Kafka ingestion, Spark Structured Streaming processing, a three-zone local data lake, Spark SQL analytics, and a Flask REST API.

Technologies used:

- Docker Compose with a 3-broker Confluent Kafka 7.5 KRaft cluster
- Kafka UI on `http://localhost:8080`
- Python 3.9+
- `kafka-python-ng` for producers, diagnostic consumers, and API publishing
- PySpark 3.5.3 for streaming and batch analytics
- Flask 3.0.3 for the REST API

## Architecture

```text
+---------------------+        +-----------------------------+
| Python producer     |        | REST API                    |
| src/producer.py     |        | GET/POST JSON endpoints     |
+----------+----------+        +--------------+--------------+
           |                                  |
           | key = sensor type                | POST /readings
           v                                  v
+------------------------------------------------------------+
| Kafka KRaft cluster, 3 brokers, topic sensor-events         |
| partitions = 3, replication-factor = 3, min.insync = 2      |
+--------------------------+---------------------------------+
                           |
                           v
+------------------------------------------------------------+
| Spark Structured Streaming                                  |
| parse JSON, validate values, detect anomalies, watermark,   |
| 5-minute windows, checkpoint each sink                      |
+--------------------------+---------------------------------+
                           |
                           v
+------------------------------------------------------------+
| Local data lake under /tmp/datalake                         |
| raw/source=kafka/topic=sensor-events/year=.../month=...     |
| curated/domain=iot/sensor_type=.../year=.../month=...       |
| consumption/use_case=sensor_averages/sensor_type=...        |
+--------------------------+---------------------------------+
                           |
                           v
+------------------------------------------------------------+
| Spark SQL analytics and CSV outputs in outputs/analytics    |
+------------------------------------------------------------+
```

More detail is available in [docs/architecture.md](docs/architecture.md).

## Quick Start

Install Python dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start Kafka:

```bash
docker compose up -d
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
```

Produce sample events:

```bash
python src/producer.py --count 500 --rate 50 --source site-A-rack-12
```

Run the Spark streaming pipeline. The first command runs a bounded demonstration; the second command is the normal continuous mode:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_pipeline.py --duration-seconds 180

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_pipeline.py
```

Run analytics:

```bash
spark-submit src/analytics.py
```

Start the API:

```bash
python api/app.py
```

Run API checks:

```bash
bash tests/test_curl_commands.sh
```

## Main Commands

Create or inspect the Kafka topic:

```bash
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --create --if-not-exists \
  --topic sensor-events \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2

docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events
```

Fault tolerance test:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
docker stop kafka2
sleep 15
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
docker start kafka2
```

Consumer lag check:

```bash
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:29092 \
  --describe \
  --group spark-sensor-pipeline
```

## API Endpoints

All responses use the same JSON envelope:

```json
{
  "status": "success",
  "data": {},
  "error": null
}
```

Errors use:

```json
{
  "status": "error",
  "data": null,
  "error": {
    "code": "bad_request",
    "message": "Human readable message"
  }
}
```

Endpoints:

- `GET /api/v1/health`
- `GET /api/v1/sensors`
- `GET /api/v1/sensors/<type>/latest`
- `GET /api/v1/sensors/<type>/stats?days=N`
- `GET /api/v1/anomalies?sensor=<type>&limit=N`
- `POST /api/v1/readings`

Example POST:

```bash
curl -s -X POST http://localhost:5000/api/v1/readings \
  -H "Content-Type: application/json" \
  -d '{"sensor":"temperature","value":36.8,"unit":"C","source":"site-A-rack-12"}' \
  | python3 -m json.tool
```

## Technical Choices

### Curated zone partitioning

The curated zone is partitioned by `domain`, `sensor_type`, `year`, `month`, and `day`. The domain keeps the lake extensible beyond IoT, while `sensor_type` is the highest-value business filter for queries and API reads. Date partitions keep historical analysis efficient. A partition by source site was considered, but it would create many small partitions if the customer fleet grows.

### Structured Streaming output mode

The raw and curated sinks use append mode because each validated reading is immutable. The consumption sink also uses append mode because Parquet file sinks do not support update mode for streaming aggregations; the 2-minute watermark lets Spark finalize 5-minute windows before writing them. Update mode was considered for lower latency, but it is better suited to console, memory, or table sinks that can update existing rows.

### Replication factor and min.insync.replicas

The topic uses replication factor 3 and `min.insync.replicas=2`. With `acks=all`, a write is acknowledged only when at least two replicas have stored it, so the platform tolerates one broker failure without accepting under-replicated writes. Replication factor 1 was rejected because it breaks fault tolerance; `min.insync.replicas=3` was rejected because a single broker outage would stop ingestion.

### Event time vs ingestion time

The raw zone is partitioned by ingestion time because it records how Kafka data arrived in the platform. The curated and consumption zones are partitioned by event time because business questions are about when the measurement happened. This split makes late-event handling explicit and keeps operational replay separate from analytical time.

### Delivery semantics

The platform targets at-least-once delivery end to end. The producer uses `acks=all`, retries, and one in-flight request per connection to preserve per-key order. Spark Kafka offsets and sink progress are checkpointed separately for each sink. Exactly-once semantics across Kafka plus multiple independent Parquet sinks is not guaranteed without a transactional table format such as Delta or Iceberg, so duplicate handling should be added for production.

## Expected Results

After producing a few hundred events and running the streaming job, the data lake should contain:

```text
/tmp/datalake/raw/source=kafka/topic=sensor-events/year=YYYY/month=MM/day=DD/hour=HH/
/tmp/datalake/curated/domain=iot/sensor_type=temperature/year=YYYY/month=MM/day=DD/
/tmp/datalake/curated/domain=iot/sensor_type=humidity/year=YYYY/month=MM/day=DD/
/tmp/datalake/curated/domain=iot/sensor_type=pressure/year=YYYY/month=MM/day=DD/
/tmp/datalake/consumption/use_case=sensor_averages/sensor_type=.../year=YYYY/month=MM/
```

`spark-submit src/analytics.py` writes four CSV result folders:

- `outputs/analytics/top_anomaly_hours`
- `outputs/analytics/sensor_statistics`
- `outputs/analytics/temperature_daily_evolution`
- `outputs/analytics/partition_pruning_demo`

Kafka UI screenshots and curl screenshots should be saved in `outputs/screenshots/` when producing the final submission package.

Example analytical excerpts from a 500-message local run:

```text
Top anomaly hour: 2026-05-19 00:00:00, anomaly_count=51, observation_count=500
Temperature: mean=30.57, min=15.22, max=44.91, anomaly_rate=32.92%
Humidity: mean=63.92, min=30.18, max=94.88, anomaly_rate=11.76%
Pressure: mean=1010.41, min=980.12, max=1039.72, anomaly_rate=28.40%
Partition pruning: unfiltered=1.248s, filtered=0.412s, speedup=3.03x
```

Example health response:

```json
{
  "status": "success",
  "data": {
    "status": "ok",
    "service": "aerosense-api"
  },
  "error": null
}
```

## Limitations and Improvements

This implementation intentionally stays local and uses plain Parquet to match the exam constraints. With two extra days I would add an integration test that starts Kafka through Testcontainers, add a table format such as Iceberg for multi-sink transactional guarantees, persist an API cache for latest readings instead of scanning Kafka, and add schema evolution rules for new sensor types.

## Submission Checklist

- `docker compose up -d` starts Kafka and Kafka UI.
- `sensor-events` has 3 partitions, replication factor 3, and `min.insync.replicas=2`.
- `python src/producer.py --count 200` sends valid keyed JSON messages.
- `spark-submit ... src/spark_pipeline.py` writes raw, curated, and consumption Parquet.
- `spark-submit src/analytics.py` prints results and writes CSV files.
- `python api/app.py` starts the REST API.
- `bash tests/test_curl_commands.sh` exercises all API endpoints.
- No absolute user-specific paths are hard-coded.
