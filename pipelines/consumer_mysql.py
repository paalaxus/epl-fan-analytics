from kafka import KafkaConsumer
import json
import mysql.connector
from mysql.connector import Error

# ---------------------
# Kafka Setup
# ---------------------
consumer = KafkaConsumer(
    "FanSalesTopic",
    bootstrap_servers=["localhost:9092"],   # or kafka:29092 inside containers
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# ---------------------
# MySQL Setup
# ---------------------
try:
    connection = mysql.connector.connect(
        host="localhost",          # or mysql if using docker-compose
        user="root",
        password="root",
        database="epl"
    )
    cursor = connection.cursor()
    print("Connected to MySQL.")
except Error as e:
    print(f"MySQL connection error: {e}")
    exit()

insert_query = """
INSERT INTO fan_sales (
    transaction_id, event_ts, fan_id, team, city, country,
    product_name, unit_price, quantity
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

print("📥 Listening for Kafka events...")

# ---------------------
# Start Consuming
# ---------------------
for msg in consumer:
    event = msg.value
    print(f"Received event: {event}")

    try:
        cursor.execute(insert_query, (
            event["transaction_id"],
            event["event_ts"],
            event["fan_id"],
            event["team"],
            event["city"],
            event["country"],
            event["product_name"],
            event["unit_price"],
            event["quantity"],
        ))
        connection.commit()
        print(f"Inserted transaction {event['transaction_id']} into MySQL")

    except Error as e:
        print(f"MySQL Insert Error: {e}")

