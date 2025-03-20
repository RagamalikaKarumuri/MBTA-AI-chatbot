import requests
import json
import time
import logging
from kafka import KafkaProducer

# ✅ MBTA API Configuration
MBTA_API_KEY = "6426e442cb644cae82e86def2e03ecb3"
MBTA_API_URL = "https://api-v3.mbta.com"

# ✅ Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TOPICS = {
    "alerts": "mbta_alerts",
    "routes": "mbta_routes",
    "stops": "mbta_stops",
    "vehicles": "mbta_vehicles",
    "predictions": "mbta_predictions",
}

# ✅ Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Initialize Kafka Producer
def create_kafka_producer():
    for _ in range(5):  # Retry up to 5 times
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                linger_ms=500,
                batch_size=16384,
                max_request_size=10485760,
                acks="all",
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            logging.info("✅ Kafka Producer connected successfully.")
            return producer
        except Exception as e:
            logging.error(f"❌ Kafka Producer connection failed: {e}")
            time.sleep(5)
    exit(1)

producer = create_kafka_producer()

# ✅ Define API Endpoints
ENDPOINTS = {
    "alerts": f"/alerts?api_key={MBTA_API_KEY}",
    "routes": f"/routes?api_key={MBTA_API_KEY}",
    "stops": f"/stops?api_key={MBTA_API_KEY}",
    "vehicles": f"/vehicles?api_key={MBTA_API_KEY}",
    "predictions": f"/predictions?filter[route]=Red&filter[stop]=place-sstat&include=route,stop,vehicle&api_key={MBTA_API_KEY}",
}

# ✅ Function to fetch & send data to Kafka
def fetch_and_produce(topic, endpoint):
    url = f"{MBTA_API_URL}{endpoint}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "data" in data and data["data"]:
            producer.send(topic, data["data"])
            logging.info(f"✅ Data sent to Kafka topic: {topic} ({len(data['data'])} records)")
        else:
            logging.warning(f"⚠️ No data available for {topic}")

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error fetching {endpoint}: {e}")

# ✅ Main Loop
logging.info("🚀 Starting MBTA Data Pipeline")

try:
    while True:
        for key, endpoint in ENDPOINTS.items():
            fetch_and_produce(TOPICS[key], endpoint)

        logging.info("⏳ Waiting for the next batch...")
        time.sleep(30)  # Adjust interval if needed
except KeyboardInterrupt:
    logging.info("🛑 Stopping MBTA Data Pipeline...")
    producer.close()
