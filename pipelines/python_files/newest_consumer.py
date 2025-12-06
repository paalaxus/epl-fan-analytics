from kafka import KafkaConsumer
import json
import uuid
import mysql.connector
from mysql.connector import Error
from cassandra.cluster import Cluster


# ======================================================
#                 KAFKA CONSUMER
# ======================================================
consumer = KafkaConsumer(
    "FanSalesTopic",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)


# ======================================================
#                 MYSQL SETUP
# ======================================================
try:
    mysql_conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="epl"
    )
    mysql_cursor = mysql_conn.cursor()
    print("Connected to MySQL.")
except Error as e:
    print(f"❌ MySQL connection error: {e}")
    exit()

mysql_insert = """
INSERT IGNORE INTO fan_sales (
    transaction_id, event_ts, fan_id, team, country,
    product_name, unit_price, quantity, image_url, source
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


# ======================================================
#                 CASSANDRA SETUP
# ======================================================
try:
    cluster = Cluster(["localhost"])
    cass_session = cluster.connect("epl")

    cass_insert = cass_session.prepare("""
        INSERT INTO fan_sales (
            transaction_id, event_ts, fan_id, team, country,
            product_name, unit_price, quantity, image_url, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    print("Connected to Cassandra.")
except Exception as e:
    print(f"❌ Cassandra connection error: {e}")
    exit()


# ======================================================
#                 LOCAL FILE LOGGING (NO HDFS)
# ======================================================

LOCAL_LOG = "/home/kofi/Downloads/EPL_Pipeline/pipelines/local_fan_sales.json"


def write_to_local(event):
    """
    Write event to a local file instead of HDFS.
    This avoids all WebHDFS, CLI, and Datanode hostname issues.
    """
    try:
        with open(LOCAL_LOG, "a") as f:
            f.write(json.dumps(event) + "\n")

        print(f"  → 🟡 LOCAL LOG OK: {event['transaction_id']}")
    except Exception as e:
        print(f"  ❌ Local Write Error: {e}")


# ======================================================
#                 MAIN LOOP
# ======================================================
print("\n📥 Listening for Kafka events... (MySQL + Cassandra + LOCAL FILE)\n")

for msg in consumer:
    event = msg.value
    print(f"📦 Received event: {event}")

    # Ensure unique transaction ID
    if not event.get("transaction_id"):
        event["transaction_id"] = uuid.uuid4().hex[:12]

    event["source"] = event.get("source", "organic")
    event["image_url"] = event.get("image_url", "")

    # -----------------------------
    # MySQL Insert
    # -----------------------------
    try:
        mysql_cursor.execute(mysql_insert, (
            event["transaction_id"],
            event["event_ts"],
            event["fan_id"],
            event["team"],
            event["country"],
            event["product_name"],
            float(event["unit_price"]),
            int(event["quantity"]),
            event["image_url"],
            event["source"]
        ))
        mysql_conn.commit()
        print(f"  → ✅ MySQL OK: {event['transaction_id']}")
    except Error as e:
        print(f"  ❌ MySQL Insert Error: {e}")

    # -----------------------------
    # Cassandra Insert
    # -----------------------------
    try:
        cass_session.execute(
            cass_insert,
            (
                event["transaction_id"],
                event["event_ts"],
                event["fan_id"],
                event["team"],
                event["country"],
                event["product_name"],
                float(event["unit_price"]),
                int(event["quantity"]),
                event["image_url"],
                event["source"]
            )
        )
        print(f"  → 🟣 Cassandra OK: {event['transaction_id']}")
    except Exception as e:
        print(f"  ❌ Cassandra Insert Error: {e}")

    # -----------------------------
    # LOCAL WRITE
    # -----------------------------
    write_to_local(event)

    print()  # spacing
