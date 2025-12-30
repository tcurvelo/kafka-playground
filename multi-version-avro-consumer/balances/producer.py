from dataclasses import asdict
from types import ModuleType
from typing import Union

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from . import settings
from .models import BalanceV1, BalanceKey, BalanceV2


class BalanceProducer:
    producer: Producer
    schema_registry_client: SchemaRegistryClient
    topic: str

    def __init__(self, config: ModuleType):
        self.schema_registry_client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
        self.producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
        self.topic = config.TOPIC_NAME

    def send_balance(self, balance: Union[BalanceV1, BalanceV2]):
        # Create serializers with the specific schema for this model
        key_serializer = AvroSerializer(self.schema_registry_client, schema_str=BalanceKey.avro_schema())
        value_serializer = AvroSerializer(self.schema_registry_client, schema_str=balance.__class__.avro_schema())

        key = asdict(BalanceKey(account_id=balance.account_id))
        value = asdict(balance)
        self.produce(key, value, key_serializer, value_serializer)

    def produce(self, key: dict, value: dict, key_serializer: AvroSerializer, value_serializer: AvroSerializer):
        self.producer.produce(
            topic=self.topic,
            key=key_serializer(key, SerializationContext(self.topic, MessageField.KEY)),
            value=value_serializer(value, SerializationContext(self.topic, MessageField.VALUE)),
            on_delivery=self.delivery_report,
        )
        self.producer.flush()

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def main(config: ModuleType = settings):
    producer = BalanceProducer(config)

    producer.send_balance(BalanceV1(account_id="ACC001", balance=1000))
    producer.send_balance(BalanceV2(account_id="ACC002", balance=2000, currency="USD"))
    producer.send_balance(BalanceV1(account_id="ACC003", balance=500))
    print("Messages sent successfully")


if __name__ == "__main__":
    main()
