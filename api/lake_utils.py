from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, desc, min as spark_min, max as spark_max, sum as spark_sum, to_date
from pyspark.sql.utils import AnalysisException


DATALAKE_ROOT = "/tmp/datalake"
_SPARK: Optional[SparkSession] = None


def spark_session() -> SparkSession:
    global _SPARK
    if _SPARK is None:
        _SPARK = (
            SparkSession.builder.appName("AeroSense-API")
            .master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        _SPARK.sparkContext.setLogLevel("WARN")
    return _SPARK


def curated_df(datalake_root: str = DATALAKE_ROOT) -> Optional[DataFrame]:
    path = str(Path(datalake_root) / "curated")
    try:
        return spark_session().read.parquet(path)
    except AnalysisException:
        return None


def row_to_dict(row: Any) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in row.asDict(recursive=True).items():
        if isinstance(value, datetime):
            result[key] = value.replace(tzinfo=timezone.utc).isoformat()
        elif isinstance(value, date):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


def daily_stats(sensor_type: str, days: int, datalake_root: str = DATALAKE_ROOT) -> Optional[List[Dict[str, Any]]]:
    df = curated_df(datalake_root)
    if df is None:
        return None
    filtered = df.where(
        (col("sensor_type") == sensor_type)
        & (col("event_time") >= (spark_session().sql(f"SELECT current_timestamp() - INTERVAL {days} DAYS").first()[0]))
    )
    stats = (
        filtered.groupBy(to_date(col("event_time")).alias("day"))
        .agg(
            avg("value").alias("mean_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            count("*").alias("observation_count"),
            spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
        )
        .orderBy("day")
    )
    rows = [row_to_dict(row) for row in stats.collect()]
    return rows


def recent_anomalies(sensor_type: Optional[str], limit: int, datalake_root: str = DATALAKE_ROOT) -> Optional[List[Dict[str, Any]]]:
    df = curated_df(datalake_root)
    if df is None:
        return None
    filtered = df.where(col("is_anomaly") == True)
    if sensor_type:
        filtered = filtered.where(col("sensor_type") == sensor_type)
    rows = (
        filtered.select(
            "sensor_type",
            "value",
            "unit",
            "event_time",
            "sensor_source",
            "producer_anomaly",
            "is_anomaly",
            "year",
            "month",
            "day",
        )
        .orderBy(desc("event_time"))
        .limit(limit)
        .collect()
    )
    return [row_to_dict(row) for row in rows]
