import pulsar
import json
import uuid

search_term = "22300500"

pulsar_url = "pulsar://localhost:6650"
topic_name = "orders"

topic_base = f"persistent://public/default/{topic_name}"
max_messages_per_partition = 1000

client = pulsar.Client(pulsar_url)
partitions = client.get_topic_partitions(topic_name)
num_partitions = len(partitions)

print(f"Topic '{topic_base}' has {num_partitions} partitions.")

found_messages = []

for partition in range(0, num_partitions):
    partition_name = f"{topic_base}-partition-{partition}"
    temp_subscription = f"temporary-pulsar-cli-{uuid.uuid4()}"
    consumer = client.subscribe(partition_name, temp_subscription, consumer_type=pulsar.ConsumerType.Exclusive, initial_position=pulsar.InitialPosition.Earliest)
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


