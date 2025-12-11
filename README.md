# EPL Fan Analytics

EPL Fan Analytics is a full end‑to‑end big‑data pipeline that simulates fan purchase behavior for the Premier League (EPL). The system demonstrates real‑time streaming analytics, distributed storage, and a recommendation‑powered fan storefront. It is built using Kafka, MySQL, Cassandra, Docker, Streamlit, and Python.

## Project Overview

- Simulated Fan Store — A mock “EPL merchandise store” where fans can browse products, add items to a shopping cart, and checkout.
- Real-time Streaming Pipeline — Synthetic purchase events flow through a scalable data pipeline: from event generation to storage and analytics.
- Live & Historical Storage — Use MySQL for live transactional data and Cassandra for append‑only historical storage.
- Analytics Dashboard — A Streamlit‑powered dashboard to monitor sales, fan behavior, revenue, and product performance.
- Recommendation Engine — “People Also Bought” style recommendations based on co‑purchase patterns.

## Technologies Used

- Python
- Apache Kafka
- MySQL
- Cassandra
- Docker & Docker Compose
- Streamlit
- Pandas

## Data Pipeline Components

### Kafka Producer (`new_producer.py`)
Generates synthetic fan purchase events.

### Kafka Consumer (`newest_consumer.py`)
Consumes events and writes them to MySQL and Cassandra.

### MySQL
Stores live transactional sales data.

### Cassandra
Stores append-only historical sales data.

## Features

### Retail Analytics Dashboard (`retail_dashboard.py`)
- Transaction metrics
- Revenue visualizations
- Product performance
- Historical summaries

### Fan Storefront (`fan_store.py`)
- Browse merchandise
- Shopping cart system
- “People Also Bought” recommendations
- Checkout writes transactions to databases

## File Structure

```
.
├── new_producer.py
├── newest_consumer.py
├── retail_dashboard.py
├── fan_store.py
├── utils.py
├── run_producer.sh
├── run_consumer.sh
├── run_dashboard.sh
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Setup & Usage

1. Clone the repository  
2. Start containers:
   ```
   docker compose up -d
   ```
3. Run services:
   ```
   ./run_producer.sh
   ./run_consumer.sh
   ./run_dashboard.sh
   ```

## Future Improvements

- ML-based recommendation system
- Dynamic pricing
- Inventory simulation
- Fan personalization
- User authentication

## License

MIT License
