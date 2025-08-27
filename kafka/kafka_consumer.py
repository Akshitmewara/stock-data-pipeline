from kafka import KafkaConsumer
import json

# Kafka consumer setup
consumer = KafkaConsumer(
    'stock_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Start reading at the earliest message in the topic
    enable_auto_commit=True,
    group_id='stock-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Starting Kafka consumer...")

for message in consumer:
    stock_data = message.value
    print("Received message:")
    print(stock_data)
