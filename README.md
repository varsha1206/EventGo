# 🌤️ EventGo – Weather & Event Finder

EventGo is your go-to app for finding *sunny vibes and good times* in Germany. It fetches real-time weather data and live events, then helps you locate where the skies are clear *and* the fun is happening – all within a radius you choose.

---

## 🚀 What It Does

- ⛅ **Live Weather Fetching** from [Open-Meteo](https://open-meteo.com/)
- 🎫 **Event Discovery**
- 📍 **Radius-based Search** – just set how far you're willing to travel for fun and sun
- 🔄 **Kafka-Powered Sync** – real-time pipeline using Kafka Confluent
- 🗃️ **MongoDB** for weather and event storage
- 🐳 **Containerized with Docker** – easy to run anywhere

---

## 🧠 Why I Built It

Because sometimes all you need is:
1. Sunshine ☀️  
2. Live music 🎶  
3. And a way to find both at once.

---

## 🛠️ Tech Stack

- Python (Streamlit, requests, etc.)
- Apache Kafka (Confluent Cloud)
- MongoDB
- Docker

---

## 📦 Run Locally

```bash
# Clone the repo then
cd eventgo

# Start it up
docker-compose up --build
