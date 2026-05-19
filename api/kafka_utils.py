import json
import time
from typing import Any, Dict, Optional

from kafka import KafkaConsumer, KafkaProducer, TopicPartition


BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"
TOPIC = "sensor-events"


def bootstrap_list(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> list[str]:
    return [item.strip() for item in bootstrap_servers.split(",")]


def create_producer(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_list(bootstrap_servers),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=20,
        batch_size=32768,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
    )


def publish_reading(event: Dict[str, Any], bootstrap_servers: str = BOOTSTRAP_SERVERS, topic: str = TOPIC) -> Dict[str, Any]:
    producer = create_producer(bootstrap_servers)
    try:
        metadata = producer.send(topic, key=event["sensor"], value=event).get(timeout=10)
        producer.flush(timeout=10)
        return {
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
            "timestamp": int(time.time() * 1000),
        }
    finally:
        producer.close(timeout=5)


def latest_reading(sensor_type: str, bootstrap_servers: str = BOOTSTRAP_SERVERS, topic: str = TOPIC) -> Optional[Dict[str, Any]]:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_list(bootstrap_servers),
        enable_auto_commit=False,
        consumer_timeout_ms=1500,
        key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return None
        topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
        consumer.assign(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)

        for topic_partition in topic_partitions:
            end_offset = end_offsets.get(topic_partition, 0)
            consumer.seek(topic_partition, max(0, end_offset - 200))

        latest: Optional[Dict[str, Any]] = None
        for message in consumer:
            value = message.value
            if value.get("sensor") != sensor_type:
                continue
            candidate = {
                "event": value,
                "kafka": {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "key": message.key,
                },
            }
            if latest is None or value.get("timestamp", 0) >= latest["event"].get("timestamp", 0):
                latest = candidate
        return latest
    finally:
        consumer.close()

