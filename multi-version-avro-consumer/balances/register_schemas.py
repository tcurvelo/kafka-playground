from types import ModuleType

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient, SchemaRegistryError

from . import settings
from .models import BalanceKey, BalanceV1, BalanceV2

models = [
    (BalanceKey, BalanceV1, settings.TOPIC_NAME),
    (BalanceKey, BalanceV2, settings.TOPIC_NAME),
]


def register_schemas(client: SchemaRegistryClient):
    for key, value, topic in models:
        for subject, cls in [(f"{topic}-key", key), (f"{topic}-value", value)]:
            client.register_schema(subject, Schema(cls.avro_schema(), "AVRO"), True)
            client.set_compatibility(subject, "BACKWARD")


def main(config: ModuleType = settings):
    client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    try:
        register_schemas(client)
        print("Schemas registered successfully.")
    except SchemaRegistryError as e:
        print(f"Failed to register schemas: {e}")


if __name__ == "__main__":
    main()
