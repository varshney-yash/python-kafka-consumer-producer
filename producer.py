from confluent_kafka import Producer
import json

PRODUCER_CONFIG = {
    "bootstrap.servers": '127.0.0.1:9092',
    "api.version.request": True,
    "acks": "all",
}

data = {"message":"hello again from python"}
key = "p1"

kafka_client = Producer(PRODUCER_CONFIG)

kafka_client.produce(
    'test-topic',
    json.dumps(data).encode("utf-8"),
    key=key.encode("utf-8"),
    callback=lambda err, msg: print(
        f"Push failed for key: {key} error:{err} message:{msg.value()}"
    ) if err else print(
        f"Push succeeded for key: {key} error:{err} message:{msg.value()}"
    ),
)

kafka_client.flush(timeout=3)