import time
import json
from confluent_kafka import Producer
import requests

# Kafka configuration
kafka_bootstrap_servers = 'localhost:29092'
kafka_topic_name = 'weather'

# Create Kafka producer
producer_config = {
    'bootstrap.servers': kafka_bootstrap_servers,
}

producer = Producer(producer_config)

json_message = None
city_name = None
temperature = None
humidity = None
openweathermap_api_endpoint = None
appid = "543c0c97a6ac8f8280dd779bb71b2033"

def get_weather_detail(openweathermap_api_endpoint):
    api_response = requests.get(openweathermap_api_endpoint)
    json_data = api_response.json()
    city_name = json_data["name"]
    humidity = json_data["main"]["humidity"]
    temperature = json_data["main"]["temp"]
    json_message = {
        "CityName": city_name,
        "Temperature": temperature,
        "Humidity": humidity,
        "CreationTime": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    return json_message

def get_appid(appid):
    # Implement this function to get the appid
    return appid

while True:
    city_name = "Hanoi"
    appid = get_appid(appid)
    openweathermap_api_endpoint = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={appid}"
    json_message = get_weather_detail(openweathermap_api_endpoint)
    
    # Send message to Kafka topic
    producer.produce(kafka_topic_name, value=json.dumps(json_message))
    
    print(f"Published message: {json.dumps(json_message)}")
    print("Waiting for 2 seconds...")
    producer.flush()  # Make sure to flush the messages to Kafka
    time.sleep(2)
