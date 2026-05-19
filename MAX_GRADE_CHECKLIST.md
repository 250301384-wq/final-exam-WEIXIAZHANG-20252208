# Maximum Grade Checklist

Use this checklist after running `bash scripts/run_full_demo.sh`.

## Part 1: Kafka Infrastructure and Fault Tolerance

- [ ] `docker compose up -d` starts `kafka1`, `kafka2`, `kafka3`, and `kafka-ui`.
- [ ] `sensor-events` exists with 3 partitions.
- [ ] `sensor-events` has replication factor 3.
- [ ] Topic config includes `min.insync.replicas=2`.
- [ ] `docs/fault_tolerance.md` and `outputs/evidence/` contain before/after broker failure output.

## Part 2: Producer

- [ ] `src/producer.py` supports `--count`, `--rate`, `--source`, `--seed`, `--demo-time-span-minutes`, and `--dry-run`.
- [ ] Producer uses `acks="all"`, `retries=5`, `max_in_flight_requests_per_connection=1`, `linger_ms`, and `batch_size`.
- [ ] Kafka key is exactly the sensor type.
- [ ] Normal generated values are non-anomalous.
- [ ] At least 10% of records are deliberately anomalous when `count >= 10`.
- [ ] Producer summary shows sent, delivered, failed, anomaly count, per-sensor counts, and delivery sample.

## Part 3: Spark Structured Streaming

- [ ] `src/spark_pipeline.py` reads Kafka in streaming mode without a fixed `kafka.group.id`.
- [ ] JSON parsing uses an explicit schema and rejects invalid records.
- [ ] The pipeline filters physically impossible values.
- [ ] `is_anomaly` is computed independently from the producer flag.
- [ ] Raw, curated, and consumption zones are populated in `/tmp/datalake`.
- [ ] Consumption zone contains 5-minute aggregates with a 2-minute watermark.
- [ ] Each sink has a separate checkpoint directory.

## Part 4: Analytics

- [ ] `src/analytics.py` runs the four required queries.
- [ ] CSV outputs are generated under `outputs/analytics/`.
- [ ] `outputs/analytics/summary.md` contains real numeric excerpts.
- [ ] Partition pruning timing does not use a cached dataframe.
- [ ] `outputs/analytics/partition_pruning_explain.txt` shows partition filters.

## Part 5: REST API

- [ ] All six endpoints respond.
- [ ] Responses use a consistent JSON envelope.
- [ ] GET path sensor errors use 404 where specified.
- [ ] Query parameter errors use 400.
- [ ] POST semantic validation errors use 422.
- [ ] Global 404, 405, and 500 handlers return JSON.

## Part 6: Documentation

- [ ] README has fresh-run commands, cleanup, architecture, technical choices, and evidence instructions.
- [ ] `docs/architecture.md` explains the data flow.
- [ ] `docs/fault_tolerance.md` points to real command outputs.
- [ ] `docs/analytics.md` explains the generated analytics results.
- [ ] `docs/reflection.md` answers all bonus questions concisely.
- [ ] `docs/api_examples.md` provides curl tests and expected status codes.

