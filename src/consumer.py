import argparse
import json
from typing import Iterable

from kafka import KafkaConsumer


BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"
TOPIC = "sensor-events"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Small diagnostic consumer for sensor-events.")
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--group-id", default="sensor-events-debug")
    parser.add_argument("--max-messages", type=int, default=20)
    parser.add_argument("--from-beginning", action="store_true")
    return parser.parse_args()


def iter_messages(args: argparse.Namespace) -> Iterable[object]:
    reset = "earliest" if args.from_beginning else "latest"
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[item.strip() for item in args.bootstrap_servers.split(",")],
        group_id=args.group_id,
        auto_offset_reset=reset,
        enable_auto_commit=False,
        consumer_timeout_ms=15000,
        key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    try:
        for message in consumer:
            yield message
    finally:
        consumer.close()


def main() -> int:
    args = parse_args()
    for index, message in enumerate(iter_messages(args), start=1):
        print(
            json.dumps(
                {
                    "key": message.key,
                    "partition": message.partition,
                    "offset": message.offset,
                    "value": message.value,
                },
                sort_keys=True,
            )
        )
        if index >= args.max_messages:
            break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

