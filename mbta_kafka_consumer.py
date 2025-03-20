import json
import logging
import time
from kafka import KafkaConsumer
import snowflake.connector

# ✅ Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TOPICS = ["mbta_alerts", "mbta_routes", "mbta_stops", "mbta_vehicles", "mbta_predictions"]

# ✅ Snowflake Configuration
SNOWFLAKE_CONFIG = {
    "user": "CATFISH",
    "password": "Welcome1234#",
    "account": "pdb57018.us-west-2",
    "warehouse": "MBTA_WH2",
    "database": "MBTA_DB2",
    "schema": "MBTA_SCHEMA2",
}

# ✅ Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Establish Snowflake Connection
def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        logging.info("✅ Connected to Snowflake successfully.")
        return conn
    except Exception as e:
        logging.error(f"❌ Error connecting to Snowflake: {e}")
        exit(1)

conn = connect_to_snowflake()

# ✅ Initialize Kafka Consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# ✅ Function to insert data into Snowflake
def insert_into_snowflake(table, data):
    try:
        cursor = conn.cursor()

        if table == "MBTA_ALERTS":
            query = """INSERT INTO MBTA_ALERTS (ALERT_ID, HEADER, DESCRIPTION, EFFECT, SEVERITY, CREATED_AT) 
                       VALUES (%s, %s, %s, %s, %s, %s)"""
            records = [(d["id"], d["attributes"]["header"], d["attributes"].get("description", ""),
                        d["attributes"]["effect"], d["attributes"]["severity"], d["attributes"]["created_at"]) 
                       for d in data]

        elif table == "MBTA_ROUTES":
            query = """INSERT INTO MBTA_ROUTES (ROUTE_ID, ROUTE_NAME, ROUTE_TYPE, ROUTE_COLOR, CREATED_AT) 
                       VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)"""
            records = [(d["id"], d["attributes"]["long_name"], d["attributes"]["type"], 
                        d["attributes"]["color"]) for d in data]

        elif table == "MBTA_STOPS":
            query = """INSERT INTO MBTA_STOPS (STOP_ID, STOP_NAME, LATITUDE, LONGITUDE, CREATED_AT) 
                       VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)"""
            records = [(d["id"], d["attributes"]["name"], d["attributes"]["latitude"], 
                        d["attributes"]["longitude"]) for d in data]

        elif table == "MBTA_VEHICLES":
            query = """INSERT INTO MBTA_VEHICLES (VEHICLE_ID, ROUTE_ID, LATITUDE, LONGITUDE, BEARING, SPEED, STATUS, CREATED_AT) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"""
            records = [(d["id"], d["relationships"]["route"]["data"]["id"], d["attributes"]["latitude"],
                        d["attributes"]["longitude"], d["attributes"].get("bearing", None),
                        d["attributes"].get("speed", None), d["attributes"]["current_status"]) 
                       for d in data]

        elif table == "MBTA_PREDICTIONS":
            query = """INSERT INTO MBTA_PREDICTIONS (PREDICTION_ID, ROUTE_ID, STOP_ID, VEHICLE_ID, DIRECTION_ID, ARRIVAL_TIME, DEPARTURE_TIME, STATUS, CREATED_AT) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"""
            records = [(d["id"], d["relationships"]["route"]["data"]["id"], d["relationships"]["stop"]["data"]["id"],
                        d["relationships"]["vehicle"]["data"]["id"], d["attributes"]["direction_id"],
                        d["attributes"].get("arrival_time", None), d["attributes"].get("departure_time", None),
                        d["attributes"]["status"]) for d in data]

        cursor.executemany(query, records)
        conn.commit()
        cursor.close()

        logging.info(f"✅ Inserted {len(records)} records into Snowflake table: {table}")

    except Exception as e:
        logging.error(f"❌ Error inserting into Snowflake: {e}")

# ✅ Start consuming messages
logging.info("🚀 Starting MBTA Kafka Consumer...")
try:
    for message in consumer:
        topic = message.topic
        data = message.value

        insert_into_snowflake(topic.upper(), data)

except KeyboardInterrupt:
    logging.info("🛑 Stopping MBTA Kafka Consumer...")
    consumer.close()
    conn.close()
