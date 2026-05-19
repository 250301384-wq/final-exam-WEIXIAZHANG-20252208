import argparse
import json
import math
import random
import signal
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"
TOPIC = "sensor-events"

SENSOR_CONFIG = {
    "temperature": {
        "unit": "C",
        "normal": (15.0, 35.0),
        "anomaly": ((35.1, 45.0),),
    },
    "humidity": {
        "unit": "%",
        "normal": (30.0, 90.0),
        "anomaly": ((90.1, 95.0),),
    },
    "pressure": {
        "unit": "hPa",
        "normal": (990.0, 1030.0),
        "anomaly": ((980.0, 989.9), (1030.1, 1040.0)),
    },
}


@dataclass
class DeliveryStats:
    sent: int = 0
    delivered: int = 0
    failed: int = 0
    anomaly_count: int = 0
    per_sensor: Counter = field(default_factory=Counter)
    delivery_sample: List[Dict[str, Any]] = field(default_factory=list)


def is_anomaly(sensor: str, value: float) -> bool:
    return (
        (sensor == "temperature" and value > 35.0)
        or (sensor == "humidity" and value > 90.0)
        or (sensor == "pressure" and (value < 990.0 or value > 1030.0))
    )


def random_value(range_pair: tuple[float, float]) -> float:
    low, high = range_pair
    return round(random.uniform(low, high), 2)


def normal_value(sensor: str) -> float:
    return random_value(SENSOR_CONFIG[sensor]["normal"])


def anomaly_value(sensor: str) -> float:
    return random_value(random.choice(SENSOR_CONFIG[sensor]["anomaly"]))


def anomaly_indices(count: int) -> set[int]:
    if count < 10:
        return {0} if count > 0 else set()

    required = math.ceil(count * 0.10)
    selected = set(range(0, count, 10))
    while len(selected) < required:
        selected.add(random.randrange(count))
    return selected


def event_timestamp(index: int, count: int, span_minutes: float, end_ms: int) -> int:
    if span_minutes <= 0 or count <= 1:
        return int(time.time() * 1000)
    span_ms = int(span_minutes * 60 * 1000)
    start_ms = end_ms - span_ms
    step_ms = span_ms / (count - 1)
    return int(start_ms + index * step_ms)


def build_event(sensor: str, source: str, force_anomaly: bool, timestamp_ms: int) -> Dict[str, Any]:
    value = anomaly_value(sensor) if force_anomaly else normal_value(sensor)
    event = {
        "sensor": sensor,
        "value": value,
        "unit": SENSOR_CONFIG[sensor]["unit"],
        "timestamp": timestamp_ms,
        "source": source,
        "anomaly": is_anomaly(sensor, value),
    }
    if force_anomaly and not event["anomaly"]:
        raise RuntimeError(f"Generated forced anomaly is not anomalous: {event}")
    if not force_anomaly and event["anomaly"]:
        raise RuntimeError(f"Generated normal reading is anomalous: {event}")
    return event


def create_producer(bootstrap_servers: str) -> Any:
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=[item.strip() for item in bootstrap_servers.split(",")],
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=20,
        batch_size=32768,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce realistic IoT sensor events to Kafka.")
    parser.add_argument("--count", type=int, default=200, help="Number of events to produce.")
    parser.add_argument("--rate", type=float, default=20.0, help="Events per second.")
    parser.add_argument("--source", default="site-A-rack-12", help="Sensor site identifier.")
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed for reproducible demos.")
    parser.add_argument(
        "--demo-time-span-minutes",
        type=float,
        default=0.0,
        help="Spread event_time values across N minutes. Default 0 keeps timestamps realistic/current.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print events without connecting to Kafka.")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.count <= 0:
        raise ValueError("--count must be strictly positive")
    if args.rate <= 0:
        raise ValueError("--rate must be strictly positive")
    if args.demo_time_span_minutes < 0:
        raise ValueError("--demo-time-span-minutes must be non-negative")


def maybe_add_delivery_sample(stats: DeliveryStats, key: str, partition: Optional[int], offset: Optional[int]) -> None:
    if len(stats.delivery_sample) < 10:
        stats.delivery_sample.append({"key": key, "partition": partition, "offset": offset})


def main() -> int:
    args = parse_args()
    validate_args(args)
    if args.seed is not None:
        random.seed(args.seed)

    producer = None if args.dry_run else create_producer(args.bootstrap_servers)
    stats = DeliveryStats()
    stop_requested = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    def on_error(error: Exception) -> None:
        stats.failed += 1
        print(f"Delivery failed: {error}", file=sys.stderr)

    interval = 1.0 / args.rate
    sensors = tuple(SENSOR_CONFIG.keys())
    forced_anomalies = anomaly_indices(args.count)
    demo_end_ms = int(time.time() * 1000)

    try:
        for index in range(args.count):
            if stop_requested:
                break
            sensor = sensors[index % len(sensors)]
            timestamp_ms = event_timestamp(index, args.count, args.demo_time_span_minutes, demo_end_ms)
            event = build_event(
                sensor=sensor,
                source=args.source,
                force_anomaly=index in forced_anomalies,
                timestamp_ms=timestamp_ms,
            )
            stats.sent += 1
            stats.per_sensor[sensor] += 1
            stats.anomaly_count += int(event["anomaly"])

            if args.dry_run:
                stats.delivered += 1
                maybe_add_delivery_sample(stats, sensor, None, None)
            else:
                assert producer is not None
                future = producer.send(args.topic, key=sensor, value=event)
                metadata = future.get(timeout=30)
                stats.delivered += 1
                maybe_add_delivery_sample(stats, sensor, metadata.partition, metadata.offset)

            print(json.dumps(event, sort_keys=True))
            time.sleep(interval)
    except Exception as exc:
        on_error(exc)
    finally:
        if producer is not None:
            producer.flush(timeout=30)
            producer.close(timeout=10)

    print(
        "Producer summary: "
        f"sent={stats.sent}, delivered={stats.delivered}, failed={stats.failed}, "
        f"anomaly_count={stats.anomaly_count}, per_sensor={dict(stats.per_sensor)}",
        file=sys.stderr,
    )
    print(f"Delivery sample: {json.dumps(stats.delivery_sample, sort_keys=True)}", file=sys.stderr)
    return 0 if stats.failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
