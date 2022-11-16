from kafka import KafkaConsumer
import json

STOCK_TOPIC = "stock_data"
brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
consumer = KafkaConsumer(STOCK_TOPIC, bootstrap_servers=brokers)

for message in consumer:
    msg = json.loads(message.value.decode())
    to = msg["OPEN"]