# fetch_cities.py
import json
import pandas as pd
import uuid
from kafka import KafkaConsumer, KafkaProducer
from geopy.distance import geodesic
 
GERMAN_CITIES_CSV = "german_cities_extended.csv"
 
consumer = KafkaConsumer(
    "city_requests",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",  # Changed to latest
    group_id="city-fetcher"
)
 
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
 
# Load cities from file
try:
    df = pd.read_csv(GERMAN_CITIES_CSV)
    print(f"[fetch_cities] Loaded {len(df)} German cities")
except FileNotFoundError:
    print(f"[fetch_cities] Warning: {GERMAN_CITIES_CSV} not found. Using default cities.")
    # Fallback to original cities
    df = pd.read_csv("german_cities.csv")
 
print("[fetch_cities] Listening for requests...")
 
for msg in consumer:
    request = msg.value
    city = request["city"]
    range_km = request["range_km"]
    # Generate session ID for this request
    session_id = str(uuid.uuid4())[:8]
    print(f"[fetch_cities] New request - Session: {session_id}, City: {city}, Range: {range_km}km")
 
    # Get user city coordinates
    user_row = df[df["city"].str.lower() == city.lower()]
    if user_row.empty:
        print(f"[fetch_cities] City '{city}' not found in dataset.")
        continue
    user_coords = (user_row.iloc[0]["lat"], user_row.iloc[0]["lon"])
 
    # Always include the user's city
    results = [{
        "city": user_row.iloc[0]["city"],
        "lat": user_row.iloc[0]["lat"],
        "lon": user_row.iloc[0]["lon"],
        "session_id": session_id
    }]
 
    producer.send("city_data",{
        "city": user_row.iloc[0]["city"],
        "lat": user_row.iloc[0]["lat"],
        "lon": user_row.iloc[0]["lon"],
        "session_id": session_id
    })
    print("Sent user data")
    # Filter cities in range
    for _, row in df.iterrows():
        if row["city"].lower() == city.lower():
            continue  # Skip user's city (already added)
        target_coords = (row["lat"], row["lon"])
        distance = geodesic(user_coords, target_coords).km
        if distance <= range_km:
            results.append({
                "city": row["city"],
                "lat": row["lat"],
                "lon": row["lon"],
                "session_id": session_id
            })
            data = {
                "city": row["city"],
                "lat": row["lat"],
                "lon": row["lon"],
                "session_id": session_id
            }
            print(f"[fetch_cities] Sent: {data}")
            producer.send("city_data",value=data)

 
    print(f"[fetch_cities] Found {len(results)} cities near {city} within {range_km} km")
    producer.flush()
    # # Send each city to the city_data topic
    # producer.send("city_data", value=results)
    # print(f"[fetch_cities] Sent: {results}")
 
    # producer.flush()
    # Send session info to a control topic
    session_info = {
        "session_id": session_id,
        "base_city": city,
        "range_km": range_km,
        "city_count": len(results)
    }
    producer.send("session_control", value=session_info)
