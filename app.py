# streamlit_app.py

import streamlit as st
import pandas as pd
import folium
import json
import time
import os
import uuid
from streamlit_folium import st_folium
from kafka import KafkaProducer
import requests
from datetime import datetime, timedelta
from folium import plugins

# ------------- SETTINGS -------------
KAFKA_TOPIC = "city_requests"
DATA_DIR = "./current"
BOOTSTRAP_SERVERS = "localhost:9092"
WAIT_SECONDS = 30
TICKETMASTER_API_KEY = "bN9mOHps6ZdE1nIioUAogmd4GnmtxrFs"
# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
# -------------- STYLING --------------
st.set_page_config(
    page_title="🌤️EventCast🌤️",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)
st.session_state['get_weather'] = False
# Custom CSS
st.markdown("""
<style>
    .main {
        padding: 0rem 1rem;
    }
    .stButton>button {
        background-color: #0066cc;
        color: white;
        border-radius: 5px;
        border: none;
        padding: 0.5rem 1rem;
        font-weight: bold;
        transition: all 0.3s;
    }
    .stButton>button:hover {
        background-color: #0052a3;
        transform: translateY(-2px);
    }
    .weather-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .event-card {
        background-color: #e8f4f8;
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        border-left: 4px solid #0066cc;
    }
</style>
""", unsafe_allow_html=True)

# -------------- FUNCTIONS --------------
def fetch_events(city_name, event_type=None, weather_category=None, date_range=None):
    """Fetch events from Ticketmaster API with filtering"""
    url = "https://app.ticketmaster.com/discovery/v2/events.json"
    
    params = {
        "apikey": TICKETMASTER_API_KEY,
        "city": city_name,
        "size": 10,
        "sort": "date,asc",
        "countryCode": "DE"
    }
    
    # Add event type filter
    if event_type and event_type != "All":
        if event_type == "Music":
            params["classificationName"] = "Music"
        elif event_type == "Sports":
            params["classificationName"] = "Sports"
        elif event_type == "Arts & Theatre":
            params["classificationName"] = "Arts & Theatre"
        elif event_type == "Family":
            params["classificationName"] = "Family"
    
    # Add date range filter
    if date_range:
        start_date = datetime.now().strftime("%Y-%m-%dT00:00:00Z")
        end_date = (datetime.now() + timedelta(days=date_range)).strftime("%Y-%m-%dT23:59:59Z")
        params["startDateTime"] = start_date
        params["endDateTime"] = end_date
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        events = []
        for evt in data.get("_embedded", {}).get("events", []):
            # Extract event details
            event_info = {
                "name": evt.get("name", "Unknown Event"),
                "url": evt.get("url", "#"),
                "date": evt.get("dates", {}).get("start", {}).get("localDate", "TBA"),
                "time": evt.get("dates", {}).get("start", {}).get("localTime", "TBA"),
                "venue": evt.get("_embedded", {}).get("venues", [{}])[0].get("name", "Unknown Venue"),
                "type": evt.get("classifications", [{}])[0].get("segment", {}).get("name", "Other"),
                "genre": evt.get("classifications", [{}])[0].get("genre", {}).get("name", ""),
                "min_price": None,
                "max_price": None
            }
            
            # Get price range if available
            if "priceRanges" in evt and evt["priceRanges"]:
                event_info["min_price"] = evt["priceRanges"][0].get("min")
                event_info["max_price"] = evt["priceRanges"][0].get("max")
            
            # Filter by weather suitability
            if weather_category:
                is_indoor = any(keyword in event_info["venue"].lower() 
                              for keyword in ["hall", "theater", "theatre", "center", "centre", "arena"])
                
                if weather_category == "rainy" and not is_indoor:
                    continue  # Skip outdoor events on rainy days
                elif weather_category == "sunny" and event_info["type"] == "Sports":
                    # Prioritize outdoor sports on sunny days
                    events.insert(0, event_info)
                    continue
            
            events.append(event_info)
        
        return events[:5]  
        
    except Exception as e:
        print(f"Error fetching events for {city_name}: {str(e)}")
        return []

def get_weather_icon(weathercode):
    """Return emoji icon based on weather code"""
    if weathercode in [0, 1]:
        return "☀️"
    elif weathercode in [2, 3]:
        return "☁️"
    elif weathercode in [51, 53, 55, 61, 63, 65, 80, 81, 82]:
        return "🌧️"
    else:
        return "🌫️"

def get_weather_color(weather_category):
    """Return color for weather marker"""
    colors = {
        "sunny": "#FFD700",
        "cloudy": "#87CEEB",
        "rainy": "#4682B4",
        "other": "#808080"
    }
    return colors.get(weather_category, "#808080")

# ------------- STREAMLIT UI -------------
# Header
col1, col2 = st.columns([3, 1])
with col1:
    st.title("🌤️ EventCast")
    st.markdown("*Discover weather conditions and events across German cities*")

# Sidebar
with st.sidebar:
    st.header("🔍 Search Parameters")
    
    # City and range inputs
    city = st.text_input("📍 Base City", value="Heidelberg", placeholder="Enter a German city...")
    radius = st.slider("📏 Search Radius (km)", 10, 500, 100, step=10)
    
    st.divider()
    
    # Weather preferences
    st.subheader("🌡️ Weather Preferences")
    weather_pref = st.radio(
        "Preferred Weather",
        ["Any", "Sunny ☀️", "Cloudy ☁️", "Rainy 🌧️"],
        horizontal=False
    )
    
    st.divider()
    
    # Event preferences
    st.subheader("🎭 Event Preferences")
    include_events = st.checkbox("🎪 Include Events", value=True)
    
    if include_events:
        event_type = st.selectbox(
            "Event Category",
            ["All", "Music", "Sports", "Arts & Theatre", "Family"]
        )
        
        date_range = st.slider(
            "Date Range (days from today)",
            1, 90, 30
        )
        
        weather_based_events = st.checkbox(
            "🌦️ Weather-based recommendations",
            value=True,
            help="Recommend indoor events for rainy weather, outdoor for sunny"
        )

# Main content area
if "session_id" not in st.session_state:
    st.session_state["session_id"] = None

col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    search_button = st.button("🔍 Search", type="primary", use_container_width=True)

# Search action
if search_button:
    if not city:
        st.error("❌ Please enter a valid city name.")
    else:
        # Generate new session ID
        session_id = str(uuid.uuid4())[:8]
        st.session_state["session_id"] = session_id
        # Clear weather_session.json
        with open(f"{DATA_DIR}/weather_session.json", "w") as f:
            pass
        
        with st.spinner(f"🔄 Searching for cities within {radius}km of {city}..."):
            # Send request to Kafka
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            
            payload = {"city": city, "range_km": radius}
            producer.send(KAFKA_TOPIC, value=payload)
            producer.flush()
            producer.close()
            
            st.success(f"✅ Request sent - Session ID: {session_id}")
            
            # Wait for data
            session_file = f"{DATA_DIR}/weather_session.json"
            start_time = time.time()
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            while not os.path.exists(session_file) or os.path.getsize(session_file) == 0:
                elapsed = time.time() - start_time
                progress = min(elapsed / WAIT_SECONDS, 0.95)
                progress_bar.progress(progress)
                status_text.text(f"⏳ Fetching weather data... {int(elapsed)}s")
                time.sleep(10)
            
            progress_bar.progress(1.0)
            status_text.text("✅ Data received!")
            time.sleep(0.5)
            progress_bar.empty()
            status_text.empty()

# Display results
if st.session_state["session_id"]:
    session_file = f"{DATA_DIR}/weather_session.json"
    
    if os.path.exists(session_file):
        # Load data
        with open(session_file) as f:
            records = [json.loads(line) for line in f.readlines()]
        
        df = pd.DataFrame(records)
        
        # Apply weather filter
        if weather_pref != "Any":
            weather_map = {
                "Sunny ☀️": "sunny",
                "Cloudy ☁️": "cloudy",
                "Rainy 🌧️": "rainy"
            }
            filter_category = weather_map.get(weather_pref)
            df_filtered = df[df["weather_category"] == filter_category]
            
            if df_filtered.empty:
                st.warning(f"⚠️ No cities found with {weather_pref} weather. Showing all cities.")
                df_filtered = df
        else:
            df_filtered = df
        
        # Display summary
        col1, col2, col3, col4,col5 = st.columns(5)
        with col1:
            st.metric("Cities Found", len(df_filtered))
        with col2:
            avg_temp = df_filtered["weather"].apply(lambda x: x.get("temperature_2m", 0)).mean()
            st.metric("Avg Temperature", f"{avg_temp:.1f}°C")
        with col3:
            sunny_count = len(df_filtered[df_filtered["weather_category"] == "sunny"])
            st.metric("Sunny Cities", sunny_count)
        with col4:
            cloudy_count = len(df_filtered[df_filtered["weather_category"] == "cloudy"])
            st.metric("Cloudy Cities", cloudy_count)
        with col5:
            rainy_count = len(df_filtered[df_filtered["weather_category"] == "rainy"])
            st.metric("Rainy Cities", rainy_count)
        
        # Create tabs
        tab1, tab2, tab3 = st.tabs(["🗺️ Map View", "📊 Weather Details", "🎪 Events"])
        
        with tab1:
            # Create map
            m = folium.Map(
                location=[df_filtered["lat"].mean(), df_filtered["lon"].mean()],
                zoom_start=7,
                tiles="OpenStreetMap"
            )
            
            # Add markers
            for _, row in df_filtered.iterrows():
                weather = row["weather"]
                weather_icon = get_weather_icon(weather.get("weathercode", 0))
                color = get_weather_color(row["weather_category"])
                
                popup_html = f"""
                <div style='font-family: Arial; width: 200px;'>
                    <h4>{row['city']}</h4>
                    <p><b>Temperature:</b> {weather.get('temperature_2m', 'N/A')}°C</p>
                    <p><b>Weather:</b> {weather_icon} {row['weather_category'].title()}</p>
                    <p><b>Cloud Cover:</b> {weather.get('cloudcover', 'N/A')}%</p>
                    <p><b>Wind Speed:</b> {weather.get('windspeed_10m', 'N/A')} km/h</p>
                </div>
                """
                
                folium.Marker(
                    [row["lat"], row["lon"]],
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"{row['city']} - {weather_icon} {weather.get('temperature_2m')}°C",
                    icon=folium.Icon(color=color.replace("#", ""), icon="info-sign")
                ).add_to(m)
            
            # Add marker cluster
            marker_cluster = plugins.MarkerCluster().add_to(m)
            
            st_folium(m, height=600, width=None, returned_objects=["last_clicked"])
        
        with tab2:
            st.subheader("📊 Weather Details")
            
            # Sort by temperature
            df_sorted = df_filtered.copy()
            df_sorted["temp"] = df_sorted["weather"].apply(lambda x: x.get("temperature_2m", 0))
            df_sorted = df_sorted.sort_values("temp", ascending=False)
            
            # Display weather cards
            for _, row in df_sorted.iterrows():
                weather = row["weather"]
                weather_icon = get_weather_icon(weather.get("weathercode", 0))
                
                with st.container():
                    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
                    
                    with col1:
                        st.markdown(f"### {weather_icon} {row['city']}")
                    with col2:
                        st.metric("Temperature", f"{weather.get('temperature_2m', 'N/A')}°C")
                    with col3:
                        st.metric("Cloud Cover", f"{weather.get('cloudcover', 'N/A')}%")
                    with col4:
                        st.metric("Wind", f"{weather.get('windspeed_10m', 'N/A')} km/h")
                    
                    st.markdown("---")
        
        with tab3:
            if include_events:
                st.subheader("🎪 Upcoming Events")
                
                # Event search progress
                event_progress = st.progress(0)
                event_status = st.empty()
                
                all_events = {}
                for idx, (_, row) in enumerate(df_filtered.iterrows()):
                    event_progress.progress((idx + 1) / len(df_filtered))
                    event_status.text(f"Fetching events for {row['city']}...")
                    
                    weather_cat = row["weather_category"] if weather_based_events else None
                    events = fetch_events(
                        row["city"],
                        event_type if event_type != "All" else None,
                        weather_cat,
                        date_range
                    )
                    
                    if events:
                        all_events[row["city"]] = {
                            "events": events,
                            "weather": row["weather"],
                            "weather_category": row["weather_category"]
                        }
                
                event_progress.empty()
                event_status.empty()
                
                if all_events:
                    for city, data in all_events.items():
                        weather_icon = get_weather_icon(data["weather"].get("weathercode", 0))
                        
                        st.markdown(f"### 📍 {city} {weather_icon}")
                        
                        for event in data["events"]:
                            with st.container():
                                st.markdown(f"""
                                <div class="event-card">
                                    <h4>🎭 <a href="{event['url']}" target="_blank">{event['name']}</a></h4>
                                    <p>📅 <b>Date:</b> {event['date']} at {event['time']}</p>
                                    <p>📍 <b>Venue:</b> {event['venue']}</p>
                                    <p>🎫 <b>Type:</b> {event['type']} {f"- {event['genre']}" if event['genre'] else ""}</p>
                                    {f"<p>💰 <b>Price:</b> €{event['min_price']}-€{event['max_price']}</p>" if event['min_price'] else ""}
                                </div>
                                """, unsafe_allow_html=True)
                                if st.button(
                                    "Interested",
                                    key=f"book_{city}_{event['name']}",
                                    help="Click to book this event",
                                    use_container_width=True
                                ):
                                    # 🔁 Send event details to Kafka
                                    producer.send("events", value=event)
                                    producer.flush()
                                    st.success(f"✅ Your interest has been saved")
                else:
                    st.info("No events found for the selected criteria.")
            else:
                st.info("Enable 'Include Events' in the sidebar to see upcoming events.")
