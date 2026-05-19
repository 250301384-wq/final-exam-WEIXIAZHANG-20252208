# Kafka Fault Tolerance

The following output was generated locally by `scripts/run_full_demo.sh` on 2026-05-19 and is also saved under `outputs/evidence/`.

## Before Broker Failure

Command:

```bash
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events
```

Output:

```text
Topic: sensor-events	TopicId: R9-5BNrLTcOv5REGpwRatg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: sensor-events	Partition: 0	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
	Topic: sensor-events	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
	Topic: sensor-events	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
```

All partitions have three replicas and three in-sync replicas.

## After Stopping `kafka2`

Commands:

```bash
docker stop kafka2
sleep 20
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events
```

Output:

```text
Topic: sensor-events	TopicId: R9-5BNrLTcOv5REGpwRatg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: sensor-events	Partition: 0	Leader: 3	Replicas: 2,3,1	Isr: 3,1
	Topic: sensor-events	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1
	Topic: sensor-events	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,3
```

Partition 0 moved leadership from broker 2 to broker 3. The ISR shrank to two brokers, which still satisfies `min.insync.replicas=2`.

## After Restarting `kafka2`

Commands:

```bash
docker start kafka2
sleep 25
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events
```

Output:

```text
Topic: sensor-events	TopicId: R9-5BNrLTcOv5REGpwRatg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: sensor-events	Partition: 0	Leader: 3	Replicas: 2,3,1	Isr: 3,1,2
	Topic: sensor-events	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
	Topic: sensor-events	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,3,2
```

Broker 2 rejoined the ISR for all partitions. This validates the replication factor 3 and `min.insync.replicas=2` design.

