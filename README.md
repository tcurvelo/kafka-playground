# Kafka Playground

Collection of experiments exploring interesting aspects of Kafka and its ecosystem. These are learning projects and proof-of-concepts rather than production-ready code.

## Examples

### [Priority-Based Message Partitioning with Custom Balancer (Go)](bucket-priority-pattern/)

Custom partitioner implementing the [Bucket Priority Pattern](https://www.baeldung.com/ops/kafka-message-prioritization) to route urgent messages to dedicated partitions, in Go.

### [Backward-Compatible Avro Consumer (Python)](multi-version-avro-consumer/)

Single consumer handling multiple Avro schema versions through Schema Registry, in Python.

## Setup

```bash
# Start Kafka with Schema Registry
docker-compose up -d
```
