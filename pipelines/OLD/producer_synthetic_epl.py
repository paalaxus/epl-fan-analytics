import json
import time
import uuid
import random
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "FanSalesTopic"

teams = ["Man Utd", "Chelsea", "Arsenal", "Liverpool"]
products = ["Jersey", "Scarf", "Ticket", "Poster"]
cities = ["London", "Manchester", "Liverpool"]
countries = ["UK"]

def generate_event():
    return {
        "transaction_id": str(uuid.uuid4()),    # ← UNIQUE ALWAYS
        "event_ts": datetime.utcnow().isoformat(),
        "fan_id": random.randint(1, 9999),
        "team": random.choice(teams),
        "city": random.choice(cities),
        "country": random.choice(countries),
        "product_name": random.choice(products),
        "unit_price": round(random.uniform(10, 100), 2),
        "quantity": random.randint(1, 5)
    }

def main():
    print(f"Connecting to Kafka on {KAFKA_BOOTSTRAP} ...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )
    print("Connected! Sending events...")

    while True:
        event = generate_event()
        producer.send(TOPIC, value=event)
        print("[EVENT]", event)
        time.sleep(1)

if __name__ == "__main__":
    main()
