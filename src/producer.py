import argparse
import json
import random
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict

from kafka import KafkaProducer
from kafka.errors import KafkaError


BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"
TOPIC = "sensor-events"

SENSOR_CONFIG = {
    "temperature": {"unit": "C", "normal": (15.0, 45.0), "plausible": (-40.0, 85.0)},
    "humidity": {"unit": "%", "normal": (30.0, 95.0), "plausible": (0.0, 100.0)},
    "pressure": {"unit": "hPa", "normal": (980.0, 1040.0), "plausible": (870.0, 1100.0)},
}


@dataclass
class DeliveryStats:
    sent: int = 0
    delivered: int = 0
    failed: int = 0


def is_anomaly(sensor: str, value: float) -> bool:
    return (
        (sensor == "temperature" and value > 35.0)
        or (sensor == "humidity" and value > 90.0)
        or (sensor == "pressure" and (value < 990.0 or value > 1030.0))
    )


def normal_value(sensor: str) -> float:
    low, high = SENSOR_CONFIG[sensor]["normal"]
    return round(random.uniform(low, high), 2)


def anomaly_value(sensor: str) -> float:
    if sensor == "temperature":
        return round(random.uniform(35.1, 45.0), 2)
    if sensor == "humidity":
        return round(random.uniform(90.1, 95.0), 2)
    if sensor == "pressure":
        return round(random.choice([random.uniform(980.0, 989.9), random.uniform(1030.1, 1040.0)]), 2)
    raise ValueError(f"Unsupported sensor: {sensor}")


def build_event(sensor: str, source: str, force_anomaly: bool) -> Dict[str, Any]:
    value = anomaly_value(sensor) if force_anomaly else normal_value(sensor)
    return {
        "sensor": sensor,
        "value": value,
        "unit": SENSOR_CONFIG[sensor]["unit"],
        "timestamp": int(time.time() * 1000),
        "source": source,
        "anomaly": is_anomaly(sensor, value),
    }


def create_producer(bootstrap_servers: str) -> KafkaProducer:
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.count <= 0:
        raise ValueError("--count must be strictly positive")
    if args.rate <= 0:
        raise ValueError("--rate must be strictly positive")
    if args.seed is not None:
        random.seed(args.seed)

    producer = create_producer(args.bootstrap_servers)
    stats = DeliveryStats()
    stop_requested = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    def on_success(_metadata: object) -> None:
        stats.delivered += 1

    def on_error(error: KafkaError) -> None:
        stats.failed += 1
        print(f"Delivery failed: {error}", file=sys.stderr)

    interval = 1.0 / args.rate
    sensors = tuple(SENSOR_CONFIG.keys())

    try:
        for index in range(args.count):
            if stop_requested:
                break
            sensor = sensors[index % len(sensors)]
            force_anomaly = (index % 10 == 0) or (random.random() < 0.03)
            event = build_event(sensor=sensor, source=args.source, force_anomaly=force_anomaly)
            future = producer.send(args.topic, key=sensor, value=event)
            future.add_callback(on_success)
            future.add_errback(on_error)
            stats.sent += 1
            print(json.dumps(event, sort_keys=True))
            time.sleep(interval)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=10)

    print(
        f"Producer summary: sent={stats.sent}, delivered={stats.delivered}, failed={stats.failed}",
        file=sys.stderr,
    )
    return 0 if stats.failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

