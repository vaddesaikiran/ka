import time
import requests
from quixstreams import Application
import json
import logging

# Make a request to the Open-Meteo API
def get_weather():
    response = requests.get("https://api.open-meteo.com/v1/forecast",
        params={
                "latitude": 51.5,
                "longitude": -0.11,
                "current_weather": "true",  # Corrected API parameter
            },
        )

# Get the weather data as JSON
    return response.json()

# Create a QuixStreams Application instance
def main():
        app = Application(
            broker_address="localhost:9092",
            loglevel="DEBUG",  # Fixed missing comma
        )

        # Produce the weather data to Kafka
        with app.get_producer() as producer:
            while True:
                    weather = get_weather()
                    logging.debug("Got Weather: %s", weather)
                    producer.produce(
                        topic="weather_data_demo",  # Corrected typo in the topic name
                        key="London",
                        value=json.dumps(weather),
                    )
                    logging.info("produced...sleeping")
                    time.sleep(10)

if __name__=="__main__":
    logging.basicConfig(level="DEBUG")
    main()
