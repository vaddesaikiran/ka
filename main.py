import requests
from quixstreams import Application
import json

# Make a request to the Open-Meteo API
response = requests.get("https://api.open-meteo.com/v1/forecast",
                params={
                        "latitude": 51.5,
                        "longitude": -0.11,
                        "current_weather": "true",  # Corrected API parameter
                    },
        )

# Get the weather data as JSON
weather = response.json()

# Create a QuixStreams Application instance
app = Application(
    broker_address="localhost:9092",
    loglevel="DEBUG",  # Fixed missing comma
)

# Produce the weather data to Kafka
 # This will print the formatted weather data
with app.get_producer() as producer:
    producer.produce(
        topic="weather_data_demo",  # Corrected typo in the topic name
        key="London",
        value=json.dumps(weather),
    )
