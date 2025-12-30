# Multi-Version Avro Consumer

Demonstrates how a single Kafka consumer can deserialize messages produced with different Avro schema versions using Schema Registry.

## Usage

```bash
# Register schemas
uv run register-schemas

# Produce messages with mixed schema versions (v1 and v2)
uv run produce

# Consume - automatically handles both versions
uv run consume
```

The consumer deserializes both `Balance` (v1) and `BalanceV2` (v2 with optional `currency` field) using the schema ID embedded in each message.
