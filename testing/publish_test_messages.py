import pulsar
import uuid
import json

pulsar_url = 'pulsar://localhost:6650'
topic_name = 'persistent://public/default/orders'

orders_number = 1000
order_number_prefix = 22300000

client = pulsar.Client(pulsar_url)
producer = client.create_producer(topic_name)

for id in range(0, orders_number):
    order_id = str(uuid.uuid4())
    order_number = str(order_number_prefix + id)
    message = {"order_id": order_id, "order_number": order_number}
    producer.send(json.dumps(message).encode('utf-8'))
    print(f"Message sent: {message}")

producer.close()
client.close()
