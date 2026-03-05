from confluent_kafka import Producer


def build_producer(kafka_brokers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": kafka_brokers,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 10,
            "batch.num.messages": 10000,
        }
    )
