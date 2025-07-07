from pymongo import MongoClient
import pprint

MONGO_URI = "mongodb://localhost:27017"
client = MongoClient(MONGO_URI)

# List all databases
print("📚 Databases:")
for db_name in client.list_database_names():
    print(" -", db_name)

# Connect to your database
db_name = "eventcast"
db = client[db_name]

# List collections
print(f"\n📁 Collections in '{db_name}':")
for col in db.list_collection_names():
    print(" -", col)

# Print Weather Data
print("\n🌤️ Sample Weather Data:")
weather_col = db["weather"]
for doc in weather_col.find({}, {"_id": 0, "city": 1, "weather.temperature_2m": 1, "weather.cloudcover": 1}).limit(5):
    pprint.pprint(doc)

# Print Event Data
print("\n🎪 Sample Event Data:")
events_col = db["event"]
for doc in events_col.find({}, {"_id": 0, "venue": 1, "date": 1, "name": 1}).limit(5):
    pprint.pprint(doc)

print("\n📍 Most Searched City:")
cities_col = db["cities"]
result = list(cities_col.aggregate([
    {"$group": {"_id": "$city", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
]))
if result:
    pprint.pprint(result[0])
else:
    print("No city search data available.")

# Most preferred event genre
print("\n🎧 Most Preferred Event Genre:")
result = list(events_col.aggregate([
    {"$match": {"genre": {"$exists": True, "$ne": None}}},
    {"$group": {"_id": "$genre", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
]))
if result:
    pprint.pprint(result[0])
else:
    print("No genre data available.")