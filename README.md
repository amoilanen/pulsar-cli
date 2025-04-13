# pulsar-cli
CLI utilities for Apache Pulsar: publishing messages, searching for messages

### Installation

```bash
cargo build --release
cargo install --path .
```

### Usage

Examples assume you have installed the binary using `cargo install --path .` and it's available in your PATH as `pulsar-cli`.

#### Attach to a topic (create subscription)

Attach to the earliest position:
```bash
pulsar-cli attach --topic=public/default/orders
```

Attach to the latest position:
```bash
pulsar-cli attach --topic=public/default/orders --position=latest
```

#### Detach from a topic (remove subscription)

```bash
pulsar-cli detach --topic=public/default/orders
```

#### Publish messages to a topic

Publish a single message (reads JSON from stdin):
```bash
pulsar-cli publish --topic=public/default/orders
# Paste JSON payload and press Enter
{
    "properties": {"published_at": "2025-03-13T21:56:54.160296+00:00"},
    "data": {
        "order_id": "3d2543b4-2a58-4c80-b18d-ac58f3524087",
        "order_number": "123456"
    }
}
```

Publish multiple messages (reads JSON array from stdin):
```bash
pulsar-cli publish --topic=public/default/orders
# Paste JSON array payload and press Enter
[
    {
        "properties": {"published_at": "2025-03-13T21:56:54.160296+00:00"},
        "data": {
            "order_id": "3d2543b4-2a58-4c80-b18d-ac58f3524087",
            "order_number": "123456"
        }
    },
    {
        "properties": {"published_at": "2025-03-14T10:15:23.123456+00:00"},
        "data": {
            "order_id": "7f1d4a5d-9c2f-4e4b-a1d8-f2b9c4a3f8e2",
            "order_number": "123457"
        }
    }
    # ... more messages
]
```

#### Search for messages in a topic

Search for a term within message data:
```bash
pulsar-cli search --topic=public/default/orders --search-term=123457
```

Search and output only the message data:
```bash
pulsar-cli search --topic=public/default/orders --search-term=225692 --output-only-message-data
```

Search and acknowledge messages after finding them:
```bash
pulsar-cli search --topic=public/default/orders --search-term=225692 --acknowledge-searched
```

#### Watch a topic (will output new messages)

```bash
pulsar-cli watch --topic=public/default/orders
```

#### Using a specific Pulsar environment

If you have configured environments using `pulsarctl`, you can specify which one to use with the `--pulsarctl-env` flag.
For example, if you have configured an environment named `streamnative`:

```bash
pulsar-cli publish --topic=public/default/orders --pulsarctl-env=streamnative
# Paste JSON payload and press Enter
[
    {
        "properties": {"published_at": "2025-03-13T21:56:54.160296+00:00"},
        "data": {
            "order_id": "3d2543b4-2a58-4c80-b18d-ac58f3524087",
            "order_number": "123456"
        }
    }
]

pulsar-cli attach --topic=public/default/orders --pulsarctl-env=streamnative
pulsar-cli search --topic=public/default/orders --search-term=12345 --pulsarctl-env=streamnative
# etc.
```
