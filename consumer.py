from confluent_kafka import Consumer
import json

CONSUMER_CONFIG = {
    "bootstrap.servers": '127.0.0.1:9092',
    "auto.offset.reset": "earliest",
    "group.id": 'group-hello',
    "enable.auto.commit": False,
    "session.timeout.ms": 60000,
    "heartbeat.interval.ms": 30000,
}

TOPICS = ["test-topic"]

consumer = Consumer(CONSUMER_CONFIG)

consumer.subscribe(TOPICS)

while True:
    message = consumer.poll(timeout=10.0)
    
    decoded_msg = json.loads(message.value().decode('utf-8'))
    consumer.commit(asynchronous=False)

    print(f"Received message from Producer via broker: {decoded_msg}")