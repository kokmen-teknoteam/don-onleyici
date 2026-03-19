import os
import json

# API Configs
FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"
HISTORICAL_API_URL = "https://archive-api.open-meteo.com/v1/archive"
NASA_POWER_API_URL = "https://power.larc.nasa.gov/api/temporal/hourly/point"

LATITUDE = 38.3552
LONGITUDE = 38.3095

if os.path.exists("target_location.json"):
    try:
        with open("target_location.json", "r") as _f:
            _loc = json.load(_f)
            LATITUDE = _loc.get("lat", LATITUDE)
            LONGITUDE = _loc.get("lon", LONGITUDE)
    except Exception:
        pass

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC_SENSORS = "ktt-teknofest/tarla1/sensors/json"
MQTT_TOPIC_ACTUATORS = "ktt-teknofest/tarla1/actuators/system"

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "wind_speed_10m"
]

HOURLY_FORECAST_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "wind_speed_10m",
    "soil_temperature_0cm",
    "soil_moisture_0_to_1cm"
]

NASA_VARIABLES = [
    "T2M", 
    "RH2M", 
    "T2MDEW", 
    "WS10M"
]
