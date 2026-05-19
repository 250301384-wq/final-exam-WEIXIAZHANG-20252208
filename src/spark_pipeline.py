import argparse
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    expr,
    from_unixtime,
    from_json,
    lit,
    max as spark_max,
    mean,
    min as spark_min,
    sum as spark_sum,
    when,
    window,
)
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"
DEFAULT_TOPIC = "sensor-events"
DEFAULT_DATALAKE_ROOT = "/tmp/datalake"
DEFAULT_CHECKPOINT_ROOT = "/tmp/datalake/_checkpoints"


EVENT_SCHEMA = StructType(
    [
        StructField("sensor", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("unit", StringType(), False),
        StructField("timestamp", LongType(), False),
        StructField("source", StringType(), False),
        StructField("anomaly", BooleanType(), False),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming pipeline for IoT readings.")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--datalake-root", default=DEFAULT_DATALAKE_ROOT)
    parser.add_argument("--checkpoint-root", default=DEFAULT_CHECKPOINT_ROOT)
    parser.add_argument("--duration-seconds", type=int, default=0, help="0 means run until interrupted.")
    parser.add_argument("--trigger", default="20 seconds", help="Structured Streaming trigger interval.")
    return parser.parse_args()


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("AeroSense-IoT-Streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def add_time_partitions(df: DataFrame, timestamp_column: str) -> DataFrame:
    return (
        df.withColumn("year", date_format(col(timestamp_column), "yyyy"))
        .withColumn("month", date_format(col(timestamp_column), "MM"))
        .withColumn("day", date_format(col(timestamp_column), "dd"))
        .withColumn("hour", date_format(col(timestamp_column), "HH"))
    )


def build_streams(spark: SparkSession, args: argparse.Namespace) -> tuple[DataFrame, DataFrame, DataFrame]:
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("kafka.group.id", "spark-sensor-pipeline")
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    raw_base = kafka_df.selectExpr(
        "CAST(key AS STRING) AS message_key",
        "CAST(value AS STRING) AS raw_json",
        "topic",
        "partition",
        "offset",
        "timestamp AS ingestion_time",
    )

    raw_sink = add_time_partitions(raw_base.withColumn("source", lit("kafka")), "ingestion_time")

    parsed = raw_base.select(
        from_json(col("raw_json"), EVENT_SCHEMA).alias("event"),
        col("raw_json"),
        col("message_key"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("ingestion_time"),
    ).where(col("event").isNotNull())

    events = parsed.select(
        col("event.sensor").alias("sensor_type"),
        col("event.value").alias("value"),
        col("event.unit").alias("unit"),
        col("event.timestamp").alias("event_timestamp_ms"),
        col("event.source").alias("sensor_source"),
        col("event.anomaly").alias("producer_anomaly"),
        col("raw_json"),
        col("message_key"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("ingestion_time"),
        from_unixtime((col("event.timestamp") / lit(1000)).cast("long")).cast("timestamp").alias("event_time"),
    )

    valid = events.where(
        (
            (col("sensor_type") == "temperature")
            & (col("unit") == "C")
            & col("value").between(-40.0, 85.0)
        )
        | (
            (col("sensor_type") == "humidity")
            & (col("unit") == "%")
            & col("value").between(0.0, 100.0)
        )
        | (
            (col("sensor_type") == "pressure")
            & (col("unit") == "hPa")
            & col("value").between(870.0, 1100.0)
        )
    ).withColumn(
        "is_anomaly",
        (
            ((col("sensor_type") == "temperature") & (col("value") > 35.0))
            | ((col("sensor_type") == "humidity") & (col("value") > 90.0))
            | ((col("sensor_type") == "pressure") & ((col("value") < 990.0) | (col("value") > 1030.0)))
        ),
    )

    curated_sink = add_time_partitions(valid.withColumn("domain", lit("iot")), "event_time").drop("hour")

    consumption_sink = (
        valid.withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "5 minutes"), col("sensor_type"))
        .agg(
            mean("value").alias("mean_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            expr("count(*)").alias("observation_count"),
            spark_sum(when(col("is_anomaly"), lit(1)).otherwise(lit(0))).alias("anomaly_count"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumn("use_case", lit("sensor_averages"))
    )

    consumption_sink = (
        consumption_sink.withColumn("year", date_format(col("window_start"), "yyyy"))
        .withColumn("month", date_format(col("window_start"), "MM"))
    )

    return raw_sink, curated_sink, consumption_sink


def start_query(df: DataFrame, path: Path, checkpoint: Path, trigger: str, partitions: list[str], mode: str = "append"):
    return (
        df.writeStream.format("parquet")
        .outputMode(mode)
        .option("path", str(path))
        .option("checkpointLocation", str(checkpoint))
        .option("compression", "snappy")
        .partitionBy(*partitions)
        .trigger(processingTime=trigger)
        .start()
    )


def main() -> int:
    args = parse_args()
    root = Path(args.datalake_root)
    checkpoints = Path(args.checkpoint_root)
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_sink, curated_sink, consumption_sink = build_streams(spark, args)

    queries = [
        start_query(
            raw_sink,
            root / "raw",
            checkpoints / "raw",
            args.trigger,
            ["source", "topic", "year", "month", "day", "hour"],
        ),
        start_query(
            curated_sink,
            root / "curated",
            checkpoints / "curated",
            args.trigger,
            ["domain", "sensor_type", "year", "month", "day"],
        ),
        start_query(
            consumption_sink,
            root / "consumption",
            checkpoints / "consumption",
            args.trigger,
            ["use_case", "sensor_type", "year", "month"],
        ),
    ]

    try:
        if args.duration_seconds > 0:
            deadline = time.time() + args.duration_seconds
            while time.time() < deadline and all(query.isActive for query in queries):
                time.sleep(1)
        else:
            spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            query.stop()
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
