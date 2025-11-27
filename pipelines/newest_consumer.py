from kafka import KafkaConsumer
import json
import mysql.connector
from mysql.connector import Error
from cassandra.cluster import Cluster

# -----------------------------
# Kafka Setup
# -----------------------------
consumer = KafkaConsumer(
    "FanSalesTopic",
    bootstrap_servers=["localhost:9092"],  # change to "kafka:29092" if inside Docker
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# -----------------------------
# MySQL Setup
# -----------------------------
try:
    mysql_conn = mysql.connector.connect(
        host="localhost",  # or "mysql" in Docker
        user="root",
        password="root",
        database="epl"
    )
    mysql_cursor = mysql_conn.cursor()
    print("Connected to MySQL.")
except Error as e:
    print(f"MySQL connection error: {e}")
    exit()

mysql_insert = """
INSERT INTO fan_sales (
    transaction_id, event_ts, fan_id, team, city, country,
    product_name, unit_price, quantity, source
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

# -----------------------------
# Cassandra Setup
# -----------------------------
try:
    cluster = Cluster(["localhost"])  # or "cassandra" in Docker
    cass_session = cluster.connect("epl")

    cass_insert = cass_session.prepare("""
        INSERT INTO fan_sales (
            transaction_id, event_ts, fan_id, team, city, country,
            product_name, unit_price, quantity, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    print("Connected to Cassandra.")
except Exception as e:
    print(f"Cassandra connection error: {e}")
    exit()

print("📥 Listening for Kafka events (MySQL + Cassandra)...")

# -----------------------------
# Consume + Write to Both DBs
# -----------------------------
for msg in consumer:
    event = msg.value
    print(f"\n📦 Received event: {event}")

    # ensure `source` exists
    source = event.get("source", "organic")

    # -----------------------------
    # Write to MySQL
    # -----------------------------
    try:
        mysql_cursor.execute(mysql_insert, (
            event["transaction_id"],
            event["event_ts"],
            event["fan_id"],
            event["team"],
            event["city"],
            event["country"],
            event["product_name"],
            float(event["unit_price"]),
            int(event["quantity"]),
            source,
        ))
        mysql_conn.commit()
        print(f" → ✅ MySQL OK: {event['transaction_id']}")
    except Error as e:
        print(f" ❌ MySQL Insert Error: {e}")

    # -----------------------------
    # Write to Cassandra
    # -----------------------------
    try:
        cass_session.execute(
            cass_insert,
            (
                event["transaction_id"],
                event["event_ts"],
                event["fan_id"],
                event["team"],
                event["city"],
                event["country"],
                event["product_name"],
                float(event["unit_price"]),
                int(event["quantity"]),
                source,
            )
        )
        print(f" → 🟣 Cassandra OK: {event['transaction_id']}")
    except Exception as e:
        print(f" ❌ Cassandra Insert Error: {e}")

