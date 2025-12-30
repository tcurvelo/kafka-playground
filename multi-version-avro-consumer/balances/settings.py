KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_NAME = "balances"
CONSUMER_CONF = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": f"{TOPIC_NAME}-group",
    "auto.offset.reset": "earliest",
}
