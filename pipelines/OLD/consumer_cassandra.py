from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster

# ---------------------
# Kafka Setup
# ---------------------
consumer = KafkaConsumer(
    "FanSalesTopic",
    bootstrap_servers=["localhost:9092"],   # or kafka:29092 inside docker
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# ---------------------
# Cassandra Setup
# ---------------------
cluster = Cluster(["localhost"])  # or "cassandra" if using docker-compose
session = cluster.connect("epl")

insert_query = session.prepare("""
    INSERT INTO fan_sales (
        event_id, event_type, event_version, event_ts,
        fan_id, team, city, country, amount
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

print("📥 Listening for Kafka events...")

for msg in consumer:
    event = msg.value

    session.execute(
        insert_query,
        (
            event["event_id"],
            event["event_type"],
            event["event_version"],
            event["event_ts"],
            event["fan_id"],
            event["team"],
            event["city"],
            event["country"],
            event["amount"]
        )
    )

    print(f"Inserted event {event['event_id']}")

