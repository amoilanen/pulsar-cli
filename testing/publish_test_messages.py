import pulsar
import uuid
import json
from datetime import datetime, timezone


# For example when using a StreamNative hosted instance
#pulsar_url = "pulsar+ssl://<your-host>.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud:6651"
#issuer_url = "https://auth.streamnative.cloud/"
#private_key_path = "<path-to-json-oauth-credentials>"
#audience = "urn:sn:pulsar:o-<your-organization>:<your-instance>"
#params = {
#    "issuer_url": issuer_url,
#    "private_key": private_key_path,
#    "audience": audience
#}
#authentication = pulsar.AuthenticationOauth2(json.dumps(params, indent=4))

pulsar_url = 'pulsar://localhost:6650'
authentication = None

client = None
if authentication is None:
    client = pulsar.Client(pulsar_url)
else:
    client = pulsar.Client(pulsar_url, authentication=authentication)

topic_name = 'persistent://public/default/orders'
orders_number = 100
order_number_prefix = 223000

producer = client.create_producer(topic_name)

for id in range(0, orders_number):
    order_id = str(uuid.uuid4())
    order_number = str(order_number_prefix + id)
    message = {"order_id": order_id, "order_number": order_number}
    producer.send(json.dumps(message).encode('utf-8'), properties = {
        "published_at": datetime.now(timezone.utc).isoformat()
    })
    print(f"Message sent: {message}")

producer.close()
client.close()
