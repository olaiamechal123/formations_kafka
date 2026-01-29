from confluent_kafka import Consumer
import json
consumer_config = {
    "bootstrap.servers":"localhost:9092",
    "group.id":"order-tracker",
    "auto.offset.reset":"earliest"

}
consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("Consumer is running and subscribed to orders topic")

#ckeck actively the topics
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error :" ,msg.error())
            continue
        value = msg.value().decode("utf-8")
        order = json.loads(value)
        print(f"received order : {order['quantity']} {order['item']} from {order['user']}")

except KeyboardInterrupt:
    print("\n Closing Consumer")
finally:
    consumer.close()

