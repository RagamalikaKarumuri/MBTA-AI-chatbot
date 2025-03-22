import json
import logging
from kafka import KafkaConsumer
import snowflake.connector
import pandas as pd
from datetime import datetime
import numpy as np

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TOPICS = ["mbta_alerts", "mbta_routes", "mbta_stops", "mbta_vehicles", "mbta_predictions"]

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    "user": "CATFISH",
    "password": "Welcome1234#",
    "account": "pdb57018.us-west-2",
    "warehouse": "MBTA_WH2",
    "database": "MBTA_DB2",
    "schema": "MBTA_SCHEMA2",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        logging.info("✅ Connected to Snowflake successfully.")
        return conn
    except Exception as e:
        logging.error(f"❌ Error connecting to Snowflake: {e}")
        exit(1)

def clean_data(df, table_name):
    df = df.applymap(
        lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip().lower() in ("", "none", "nan", "null")) else x
    )
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)
    return df

def merge_into_snowflake(conn, table, df, keys):
    try:
        cursor = conn.cursor()
        columns = df.columns.tolist()
        values = df.values.tolist()

        key_clause = " AND ".join([f"target.{k} = source.{k}" for k in keys])
        set_clause = ", ".join([f"{col} = source.{col}" for col in columns if col not in keys])
        column_list = ", ".join(columns)
        placeholder_list = ", ".join(["%s"] * len(columns))

        logging.info(f"🔍 Sample data for {table}:\n{df.head()}")

        for row in values:
            merge_sql = f"""
                MERGE INTO {table} AS target
                USING (SELECT {placeholder_list}) AS source ({column_list})
                ON {key_clause}
                WHEN MATCHED THEN UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN INSERT ({column_list}) VALUES ({placeholder_list})
            """
            cursor.execute(merge_sql, row + row)

        conn.commit()
        logging.info(f"✅ Merged {len(values)} records into Snowflake table: {table}")
        cursor.close()
    except Exception as e:
        logging.error(f"❌ Error merging into Snowflake ({table}): {e}")

def handle_table_insert(conn, topic, data):
    if not data:
        logging.warning(f"⚠️ No data received for topic: {topic}")
        return

    try:
        if topic == "MBTA_ALERTS":
            df = pd.DataFrame([
                {
                    "ALERT_ID": d.get("id"),
                    "HEADER": d.get("attributes", {}).get("header"),
                    "DESCRIPTION": d.get("attributes", {}).get("description", ""),
                    "EFFECT": d.get("attributes", {}).get("effect"),
                    "SEVERITY": d.get("attributes", {}).get("severity"),
                    "CREATED_AT": d.get("attributes", {}).get("created_at")
                }
                for d in data if d.get("id")
            ])
            df = clean_data(df, topic)
            merge_into_snowflake(conn, "MBTA_ALERTS", df, ["ALERT_ID"])

        elif topic == "MBTA_ROUTES":
            df = pd.DataFrame([
                {
                    "ROUTE_ID": d.get("id"),
                    "ROUTE_NAME": d.get("attributes", {}).get("long_name"),
                    "ROUTE_TYPE": d.get("attributes", {}).get("type"),
                    "ROUTE_COLOR": d.get("attributes", {}).get("color"),
                    "CREATED_AT": datetime.utcnow()
                }
                for d in data if d.get("id")
            ])
            df = clean_data(df, topic)
            merge_into_snowflake(conn, "MBTA_ROUTES", df, ["ROUTE_ID"])

        elif topic == "MBTA_STOPS":
            df = pd.DataFrame([
                {
                    "STOP_ID": d.get("id"),
                    "STOP_NAME": d.get("attributes", {}).get("name"),
                    "LATITUDE": d.get("attributes", {}).get("latitude"),
                    "LONGITUDE": d.get("attributes", {}).get("longitude"),
                    "CREATED_AT": datetime.utcnow()
                }
                for d in data if d.get("id")
            ])
            df = clean_data(df, topic)
            merge_into_snowflake(conn, "MBTA_STOPS", df, ["STOP_ID"])

        elif topic == "MBTA_VEHICLES":
            df = pd.DataFrame([
                {
                    "VEHICLE_ID": d.get("id"),
                    "ROUTE_ID": d.get("relationships", {}).get("route", {}).get("data", {}).get("id"),
                    "LATITUDE": d.get("attributes", {}).get("latitude"),
                    "LONGITUDE": d.get("attributes", {}).get("longitude"),
                    "BEARING": d.get("attributes", {}).get("bearing"),
                    "SPEED": d.get("attributes", {}).get("speed"),
                    "STATUS": d.get("attributes", {}).get("current_status"),
                    "CREATED_AT": datetime.utcnow()
                }
                for d in data if d.get("id")
            ])
            df = clean_data(df, topic)
            merge_into_snowflake(conn, "MBTA_VEHICLES", df, ["VEHICLE_ID"])

        elif topic == "MBTA_PREDICTIONS":
            df = pd.DataFrame([
                {
                    "PREDICTION_ID": d.get("id"),
                    "ROUTE_ID": d.get("relationships", {}).get("route", {}).get("data", {}).get("id"),
                    "STOP_ID": d.get("relationships", {}).get("stop", {}).get("data", {}).get("id"),
                    "VEHICLE_ID": d.get("relationships", {}).get("vehicle", {}).get("data", {}).get("id"),
                    "DIRECTION_ID": d.get("attributes", {}).get("direction_id"),
                    "ARRIVAL_TIME": d.get("attributes", {}).get("arrival_time"),
                    "DEPARTURE_TIME": d.get("attributes", {}).get("departure_time"),
                    "STATUS": d.get("attributes", {}).get("status"),
                    "CREATED_AT": datetime.utcnow()
                }
                for d in data if d.get("id")
            ])
            df = clean_data(df, topic)
            merge_into_snowflake(conn, "MBTA_PREDICTIONS", df, ["PREDICTION_ID"])

    except Exception as e:
        logging.error(f"❌ Error processing topic {topic}: {e}")

def consume_messages():
    conn = connect_to_snowflake()
    if not conn:
        return

    logging.info("🚀 Starting MBTA Kafka Consumer...")
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    try:
        for message in consumer:
            topic = message.topic.upper()
            data = message.value
            logging.info(f"📥 Received {len(data)} records from topic: {topic}")
            handle_table_insert(conn, topic, data)
    except KeyboardInterrupt:
        logging.info("🛑 Stopping MBTA Kafka Consumer...")
    finally:
        consumer.close()
        conn.close()
        logging.info("✅ Snowflake Connection Closed.")

if __name__ == "__main__":
    consume_messages()
