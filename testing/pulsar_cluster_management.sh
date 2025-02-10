# Retain messages and auto-expire inactive subscriptions
pulsarctl namespaces set-retention public/default --size 50M --time 120m

# Create a local partitioned topic
pulsarctl topics create persistent://public/default/orders 5