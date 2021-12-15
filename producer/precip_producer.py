import json
import requests
from time import sleep
from datetime import datetime
from kafka import KafkaProducer

# INCOMPLETE FILE - PENDING THE DECISION TO USE WEATHER DATA IN THE PROJECT

def api_to_kafka(endpoint: str):
    """Executes the API request to Open Weather"""
    response = requests.get(endpoint)
    weather_json = json.dumps(response.json(), indent=3)
    weather_dict = (json.loads(weather_json))
    parse_data(weather_dict)


def parse_data(river_dict: dict):
    """Parses out the necessary information to produce events to Kafka"""
    pass



if __name__ == "__main__":

    gallatin = {'latitude': 45.4973, 'longitude': -111.2707083}
    jefferson = {'latitude': 45.6132833, 'longitude': -112.3293972}
    lowerMadison = {'latitude': 45.49023056, 'longitude': -111.6345056}
    upperMadison = {'latitude': 44.88865556, 'longitude': -111.580886}

    weather_endpoint = "https://api.openweathermap.org/data/2.5/onecall?lat=" + str(gallatin["latitude"]) + "&lon=" + str(gallatin["longitude"]) + "&exclude=daily&appid=8d09ba3f05951c0a3af24e56b5b449d5"
    response = requests.get(weather_endpoint)
    weather_json = json.dumps(response.json(), indent=3)
    print(weather_json)
    weather_dict = (json.loads(weather_json))




