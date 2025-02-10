import pulsar
import json
import uuid
import time

search_term = "22300020"
topic_name = "orders"
topic_base = f"persistent://public/default/{topic_name}"
max_messages_per_partition = 50
seek_window_minutes = 60

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

partitions = client.get_topic_partitions(topic_name)
num_partitions = len(partitions)

print(f"Topic '{topic_base}' has {num_partitions} partitions.")

found_messages = []

for partition in range(0, num_partitions):
    partition_name = f"{topic_base}-partition-{partition}"
    temp_subscription = f"temporary-pulsar-cli-{uuid.uuid4()}"
    consumer = client.subscribe(partition_name, temp_subscription, consumer_type=pulsar.ConsumerType.Exclusive, initial_position=pulsar.InitialPosition.Earliest)

    current_time_minutes = int(time.time() / 60)
    timestamp = current_time_minutes - seek_window_minutes
    consumer.seek(timestamp * 60 * 1000)

    print(f"Reading from partition {partition}...")
    reading_from_partition = True
    messages_read_count = 0
    while reading_from_partition:
        try:
            msg = consumer.receive(timeout_millis=2000)
            message = msg.data().decode("utf-8")
            if search_term in message:
                parsed_message = json.loads(message)
                found_messages.append(parsed_message)
            consumer.acknowledge(msg)
            messages_read_count = messages_read_count + 1
            #print(f"Read {messages_read_count} from partition {partition_name}")
            #print(message)
            reading_from_partition = messages_read_count < max_messages_per_partition
        except pulsar.Timeout:
            reading_from_partition = False
            print(f"No more messages in partition {partition}, moving to next...")

if len(found_messages) > 0:
    print("Found messages:")
    print(json.dumps(found_messages))
else:
    print("No messages found")


