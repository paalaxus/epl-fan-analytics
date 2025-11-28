import json
import time
import uuid
import random
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "FanSalesTopic"

# ----------------------------------------------
# TEAMS
# ----------------------------------------------
teams = ["Man Utd", "Chelsea", "Arsenal", "Liverpool"]

# ----------------------------------------------
# PRODUCT LIST (same names as Cassandra table)
# ----------------------------------------------
products = [
    "Home Jersey",
    "Away Jersey",
    "Third Kit Jersey",
    "Scarf",
    "Hoodie",
    "Signed Jersey",
    "Water Bottle",
    "Backpack",
]

# ----------------------------------------------
# PRODUCT IMAGES (matches Cassandra exactly)
# ----------------------------------------------
product_images = {
    "Scarf": "https://i.imgur.com/8gF1jtk.jpeg",

    "Home Jersey": "https://i.imgur.com/8y6H4pE.jpeg",
    "Away Jersey": "https://i.imgur.com/5qF8HW7.jpeg",
    "Third Kit Jersey": "https://i.imgur.com/4mpeVvH.jpeg",

    "Hoodie": "https://i.imgur.com/joelmWB.png",
    "Signed Jersey": "https://i.imgur.com/tWGbEHt.jpeg",

    "Water Bottle": "https://i.imgur.com/yn0oamC.jpeg",
    "Backpack": "https://i.imgur.com/lbtd3K9.jpeg",
}

# fallback placeholder (if new products added later)
DEFAULT_IMAGE = "https://via.placeholder.com/200?text=No+Image"

# ----------------------------------------------
# PRICE RANGES
# ----------------------------------------------
price_map = {
    "Home Jersey": (60, 120),
    "Away Jersey": (60, 120),
    "Third Kit Jersey": (60, 120),

    "Scarf": (10, 30),
    "Hoodie": (40, 80),

    "Signed Jersey": (150, 400),

    "Water Bottle": (10, 20),
    "Backpack": (25, 55),
}

countries = ["UK"]

# ----------------------------------------------
# EVENT GENERATOR
# ----------------------------------------------
def generate_event():
    product = random.choice(products)
    low, high = price_map[product]

    return {
        "transaction_id": str(uuid.uuid4()),
        "event_ts": datetime.utcnow().isoformat(),
        "fan_id": random.randint(1, 9999),
        "team": random.choice(teams),
        "country": "UK",

        "product_name": product,
        "unit_price": round(random.uniform(low, high), 2),
        "quantity": random.randint(1, 4),

        "source": "organic",

        # NEW — image included in event (optional but useful!)
        "image_url": product_images.get(product, DEFAULT_IMAGE)
    }

# ----------------------------------------------
# MAIN LOOP
# ----------------------------------------------
def main():
    print(f"Connecting to Kafka on {KAFKA_BOOTSTRAP} ...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )

    print("Connected! Streaming events...\n")

    while True:
        event = generate_event()
        producer.send(TOPIC, value=event)
        print("[EVENT]", event)
        time.sleep(1)


if __name__ == "__main__":
    main()
