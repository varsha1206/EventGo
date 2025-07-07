import json, os
from kafka import KafkaConsumer, KafkaProducer
import requests
from datetime import datetime
import ast
 
DATA_DIR = "./current"
BOOTSTRAP_SERVERS = "localhost:9092"
 
# Create data directory if it doesn't exist
os.makedirs(DATA_DIR, exist_ok=True)
 
consumer = KafkaConsumer(
    "city_data",
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    group_id="weather-fetcher"
)
 
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
 
def fetch_weather(lat, lon):
    print(f"[weather_producer] Fetching weather for lat: {lat}, lon: {lon}")
    """Fetch weather data from Open-Meteo API"""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,cloudcover,weathercode,windspeed_10m,precipitation"
    }
    try:
        res = requests.get(url, params=params, timeout=10).json()
        return res.get("current", {})
    except Exception as e:
        print(f"Error fetching weather: {e}")
        return {}
 
def get_weather_category(weathercode):
    """Categorize weather based on weathercode"""
    if weathercode in [0, 1]:
        return "sunny"
    elif weathercode in [2, 3]:
        return "cloudy"
    elif weathercode in [51, 53, 55, 61, 63, 65, 80, 81, 82]:
        return "rainy"
    else:
        return "other"
 
print(f"[weather_producer] Starting weather producer...")
 
for msg in consumer:
    session_file = f"{DATA_DIR}/weather_session.json"
    with open(session_file, "a") as f:
        print("[weather_producer] Waiting for new city data...")
        data = msg.value
        # print(results)
        # for data in results:
        print(data)
        city = data["city"]
        print(f"[weather_producer] Processing city: {city}")
        lat, lon = data["lat"], data["lon"]
        weather = fetch_weather(lat, lon)
        if weather:
            print(f"[weather_producer] Fetched weather for {city}: {weather}")
            weather_category = get_weather_category(weather.get("weathercode", -1))
            weather_data = {
                "city": city,
                "lat": lat,
                "lon": lon,
                "weather": weather,
                "weather_category": weather_category,
                "timestamp": datetime.now().isoformat()
                }
            # Send to Kafka topic for sink connector
            producer.send("weather_data", value=weather_data)
            producer.flush()    
            # Write to session file
            f.write(json.dumps(weather_data) + "\n")
            print(f"[weather_producer] {city}: {weather.get('temperature_2m')}°C, {weather_category}")
