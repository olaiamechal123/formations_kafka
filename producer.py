from confluent_kafka import Producer
import uuid
import json
Producer = Producer({'bootstrap.servers': 'localhost:9092'})

order = {
    "order_id" : str(uuid.uuid4()),
    "user": "lara",
    "item" :"frozen yoguet",
    "quantity":10
}

#convert json to kafka data
value = json.dumps(order).encode("utf-8")
def delivery_report(err,msg):
    if err:
        print(f"delivery failed:{err}")
    else:
        print(f"delivery {msg.value().decode("utf-8")}")
        print(f"delivred to {msg.topic()}:partition {msg.partition()}")
Producer.produce(
    topic="orders",
    value=value,
    callback = delivery_report)


Producer.flush()

