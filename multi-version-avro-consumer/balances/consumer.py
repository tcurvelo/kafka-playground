from io import BytesIO
from types import ModuleType

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.serde import SchemaId
from confluent_kafka.serialization import MessageField, SerializationContext

from . import settings


def main(config: ModuleType = settings):
    schema_registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    schema_id_url = f"{config.SCHEMA_REGISTRY_URL}/schemas/ids/{{}}"
    avro_deserializer = AvroDeserializer(schema_registry, schema_str=None)
    consumer = Consumer(config.CONSUMER_CONF)
    consumer.subscribe([config.TOPIC_NAME])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            raw_value = msg.value()
            raw_key = msg.key()

            # Extract schema ID using the SchemaId deserializer
            value_schema_id_obj = SchemaId("AVRO")
            key_schema_id_obj = SchemaId("AVRO")
            value_schema_id_obj.from_bytes(BytesIO(raw_value))
            key_schema_id_obj.from_bytes(BytesIO(raw_key))

            # Deserialize - automatically handles the schema version
            key = avro_deserializer(raw_key, SerializationContext(msg.topic(), MessageField.KEY))
            value = avro_deserializer(raw_value, SerializationContext(msg.topic(), MessageField.VALUE))

            print(f"\n{'=' * 60}")
            print(f"📬 Received message: {key=} | {value=}")
            print(f"📦 Key Schema   (ID={key_schema_id_obj.id}): {schema_id_url.format(key_schema_id_obj.id)}")
            print(f"📦 Value Schema (ID={value_schema_id_obj.id}): {schema_id_url.format(value_schema_id_obj.id)}")
            print(f"{'=' * 60}\n")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
