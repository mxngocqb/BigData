from confluent_kafka import Consumer, KafkaError

# Kafka configuration
kafka_bootstrap_servers = 'localhost:29092'
kafka_topic_name = 'weather'

# Create Kafka consumer
consumer_config = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'  # Start reading from the beginning of the topic
}

consumer = Consumer(consumer_config)
consumer.subscribe([kafka_topic_name])

while True:
    msg = consumer.poll(1.0)  # Poll for messages, timeout set to 1 second

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            print(f"Reached end of partition for {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        else:
            print(msg.error())
        continue

    # Process the received message
    print(f"Received message: {msg.value().decode('utf-8')}")

# Close down consumer to commit final offsets.
consumer.close()
