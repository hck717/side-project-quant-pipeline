from confluent_kafka import Producer
import socket

def get_kafka_producer(bootstrap_servers: str) -> Producer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": socket.gethostname(),
        "queue.buffering.max.messages": 100000,
        "batch.num.messages": 1000,
        "linger.ms": 50,
        "compression.type": "snappy",
    }
    return Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
