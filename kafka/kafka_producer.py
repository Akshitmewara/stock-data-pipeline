import requests
from kafka import KafkaProducer
import json
import time

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL = "https://www.alphavantage.co/query"
params = {
    "function": "TIME_SERIES_INTRADAY",
    "symbol": "IBM",
    "interval": "1min",
    "apikey": "2BKLL3YEEG5LGYT"
}

while True:
    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        stock_data = response.json()
        print("Fetched ", stock_data)

        producer.send('stock_topic', stock_data)

    except Exception as e:
        print("Error fetching or sending ", e)

    time.sleep(60)  # fetch every minute
    