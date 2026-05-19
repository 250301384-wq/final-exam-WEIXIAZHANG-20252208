# Fault Tolerance Evidence

## Topic Configuration

Command:

```bash
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events
```

Expected configuration:

```text
Topic: sensor-events  TopicId: ...  PartitionCount: 3  ReplicationFactor: 3
Configs: min.insync.replicas=2

Topic: sensor-events  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
Topic: sensor-events  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
Topic: sensor-events  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

The exact leader ids can differ after restarts, but every partition must show three replicas and at least two in-sync replicas while the cluster is healthy.

## Broker Failure Test

Commands:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
docker stop kafka2
sleep 15
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
docker start kafka2
sleep 20
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
```

Representative trace immediately after stopping `kafka2`:

```text
Topic: sensor-events  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,3
Topic: sensor-events  Partition: 1  Leader: 3  Replicas: 2,3,1  Isr: 3,1
Topic: sensor-events  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1
```

Observation:

- Partitions that had broker 2 as leader are reassigned to broker 1 or broker 3.
- The ISR list shrinks from three brokers to two brokers.
- Because `min.insync.replicas=2`, producers using `acks=all` can still publish safely while one broker is down.
- If a second broker fails, writes are rejected instead of being acknowledged with insufficient replication.

After restarting `kafka2`, the ISR returns to three brokers:

```text
Topic: sensor-events  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,3,2
Topic: sensor-events  Partition: 1  Leader: 3  Replicas: 2,3,1  Isr: 3,1,2
Topic: sensor-events  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

## Conclusion

The configuration demonstrates the expected Kafka fault tolerance pattern: replication factor 3 protects data from one broker failure, and `min.insync.replicas=2` prevents unsafe acknowledgements.

