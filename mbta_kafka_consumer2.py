import json
import logging
import time
from kafka import KafkaConsumer
import snowflake.connector
import pandas as pd
from datetime import datetime
import numpy as np

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


# ✅ Function to connect to Snowflake
def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        logging.info("✅ Connected to Snowflake successfully.")
        return conn
    except Exception as e:
        logging.error(f"❌ Error connecting to Snowflake: {e}")
        exit(1)


# ✅ Initialize Kafka Consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)


# ✅ Data Cleaning Function
def clean_data(df, table_name):
    """
    Cleans the DataFrame before inserting into Snowflake.
    - Handles missing values.
    - Converts NaN values to None.
    - Ensures column types match Snowflake schema.
    """

    if not isinstance(df, pd.DataFrame):
        raise ValueError(f"Expected DataFrame, but got {type(df)} for table {table_name}")

    df = df.replace({np.nan: None})

    # Table-specific cleaning logic
    if table_name == "MBTA_VEHICLES":
        df["VEHICLE_ID"] = df["VEHICLE_ID"].fillna("UNKNOWN")
        df["ROUTE_ID"] = df["ROUTE_ID"].fillna("UNKNOWN")
        df["STATUS"] = df["STATUS"].fillna("UNKNOWN")
        df["SPEED"] = df["SPEED"].astype(float).fillna(0.0)
        df["CREATED_AT"] = df["CREATED_AT"].fillna(datetime.utcnow())

    elif table_name == "MBTA_STOPS":
        df["STOP_ID"] = df["STOP_ID"].fillna("UNKNOWN")
        df["STOP_NAME"] = df["STOP_NAME"].fillna("UNKNOWN")
        df["LATITUDE"] = df["LATITUDE"].astype(float).fillna(0.0)
        df["LONGITUDE"] = df["LONGITUDE"].astype(float).fillna(0.0)
        df["CREATED_AT"] = df["CREATED_AT"].fillna(datetime.utcnow())

    elif table_name == "MBTA_PREDICTIONS":
        df["PREDICTION_ID"] = df["PREDICTION_ID"].fillna("UNKNOWN")
        df["VEHICLE_ID"] = df["VEHICLE_ID"].fillna("UNKNOWN")
        df["ARRIVAL_TIME"] = df["ARRIVAL_TIME"].fillna(datetime.utcnow())
        df["DEPARTURE_TIME"] = df["DEPARTURE_TIME"].fillna(datetime.utcnow())

    elif table_name == "MBTA_ALERTS":
        df["ALERT_ID"] = df["ALERT_ID"].fillna("UNKNOWN")
        df["HEADER"] = df["HEADER"].fillna("No Header Provided")
        df["DESCRIPTION"] = df["DESCRIPTION"].fillna("No Description")
        df["EFFECT"] = df["EFFECT"].fillna("UNKNOWN")
        df["SEVERITY"] = df["SEVERITY"].fillna(0)
        df["CREATED_AT"] = df["CREATED_AT"].fillna(datetime.utcnow())

    elif table_name == "MBTA_ROUTES":
        df["ROUTE_ID"] = df["ROUTE_ID"].fillna("UNKNOWN")
        df["ROUTE_NAME"] = df["ROUTE_NAME"].fillna("UNKNOWN")
        df["ROUTE_COLOR"] = df["ROUTE_COLOR"].fillna("#FFFFFF")
        df["CREATED_AT"] = df["CREATED_AT"].fillna(datetime.utcnow())

    # Convert dictionary values (if any) to JSON string before inserting
    df = df.applymap(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    return df


# ✅ Insert Data into Snowflake
def insert_into_snowflake(conn, table, data):
    if not data:
        logging.warning(f"⚠️ No records received for {table}, skipping insert.")
        return

    try:
        cursor = conn.cursor()

        if table == "MBTA_ALERTS":
            query = """INSERT INTO MBTA_ALERTS (ALERT_ID, HEADER, DESCRIPTION, EFFECT, SEVERITY, CREATED_AT) 
                       VALUES (%s, %s, %s, %s, %s, %s)"""
            records = [(d["id"], d["attributes"]["header"], d["attributes"].get("description", ""),
                        d["attributes"]["effect"], d["attributes"]["severity"], d["attributes"]["created_at"])
                       for d in data if d.get("id")]

        elif table == "MBTA_ROUTES":
            query = """INSERT INTO MBTA_ROUTES (ROUTE_ID, ROUTE_NAME, ROUTE_TYPE, ROUTE_COLOR, CREATED_AT) 
                       VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)"""
            records = [(d["id"], d["attributes"]["long_name"], d["attributes"]["type"],
                        d["attributes"]["color"])
                       for d in data if d.get("id")]

        elif table == "MBTA_STOPS":
            query = """INSERT INTO MBTA_STOPS (STOP_ID, STOP_NAME, LATITUDE, LONGITUDE, CREATED_AT) 
                       VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)"""
            records = [(d["id"], d["attributes"]["name"], d["attributes"].get("latitude"),
                        d["attributes"].get("longitude"))
                       for d in data if d.get("id")]

        elif table == "MBTA_VEHICLES":
            query = """INSERT INTO MBTA_VEHICLES (VEHICLE_ID, ROUTE_ID, LATITUDE, LONGITUDE, BEARING, SPEED, STATUS, CREATED_AT) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"""
            records = [(d["id"], d["relationships"]["route"]["data"]["id"], d["attributes"].get("latitude"),
                        d["attributes"].get("longitude"), d["attributes"].get("bearing", None),
                        d["attributes"].get("speed", None), d["attributes"]["current_status"])
                       for d in data if d.get("id")]

        elif table == "MBTA_PREDICTIONS":
            query = """INSERT INTO MBTA_PREDICTIONS (PREDICTION_ID, ROUTE_ID, STOP_ID, VEHICLE_ID, DIRECTION_ID, ARRIVAL_TIME, DEPARTURE_TIME, STATUS, CREATED_AT) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"""
            records = [(d["id"], d["relationships"]["route"]["data"]["id"], d["relationships"]["stop"]["data"]["id"],
                        d["relationships"]["vehicle"]["data"]["id"], d["attributes"]["direction_id"],
                        d["attributes"].get("arrival_time", None), d["attributes"].get("departure_time", None),
                        d["attributes"]["status"])
                       for d in data if d.get("id")]

        if records:
            cursor.executemany(query, records)
            conn.commit()
            logging.info(f"✅ Inserted {len(records)} records into Snowflake table: {table}")

        cursor.close()

    except Exception as e:
        logging.error(f"❌ Error inserting into Snowflake ({table}): {e}")


# ✅ Start Consuming Messages
def consume_messages():
    conn = connect_to_snowflake()
    logging.info("🚀 Starting MBTA Kafka Consumer...")

    try:
        for message in consumer:
            topic = message.topic.upper()
            data = message.value

            logging.info(f"📥 Received {len(data)} records from topic: {topic}")

            insert_into_snowflake(conn, topic, data)

    except KeyboardInterrupt:
        logging.info("🛑 Stopping MBTA Kafka Consumer...")
    finally:
        consumer.close()
        conn.close()
        logging.info("✅ Snowflake Connection Closed.")


# ✅ Run the consumer
if __name__ == "__main__":
    consume_messages()
