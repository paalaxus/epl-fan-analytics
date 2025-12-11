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
--- 
```

EPL_Pipeline/
├── docker-compose.yml # Orchestrates Kafka, MySQL, Cassandra, Hadoop, Streamlit
├── requirements.txt 
├── README.md 

│
└── pipelines/
├── new_producer.py # Kafka producer (fan purchase event generator)
├── newest_consumer.py # Kafka consumer → MySQL + Cassandra writer
├── retail_dashboard.py # Retail operations analytics (Streamlit)
├── fan_store.py # Fan-facing storefront dashboard (Streamlit)
├── newest_dashboard.py # Experimental unified dashboard
├── recommender.py # Co-occurrence recommendation engine
├── utils.py # Shared helper utilities
│
├── local_fan_sales.json # Sample offline sales data for testing
│
├── run_producer.sh # Start Kafka producer
├── run_consumer.sh # Start Kafka consumer
├── run_dashboard.sh # Launch dashboard(s)
├── enter_mysql.sh # Quick access to MySQL CLI inside container
├── enter_cassandra.sh # Quick access to Cassandra CQLSH
├── sync_to_hdfs.sh # Push historical data → HDFS
├── view_hdfs.sh # Inspect contents of Hadoop HDFS
├── push_to_git.sh # Auto-commit/push tool for development
├── setup.sh # Initial setup script
├── venv.sh # Creates/activates virtual environment
│
├── epl_consumer_env/ # Local Python virtual environment (optional)

```
---
## Setup & Usage


#needed changes and troubleshooting
- Check all Scripts and change folder names to match yours. ex home/name/downloads.. etc - it can affect hadoop
- Check that cassandra and mysql have databases and tables created after you do step one.
   ```
   ./enter_mysql.sh
   ./enter_cassandra.sh
   
   ```
  - Install python depencies in the virtual environment.  ``` pip install -r requirements.txt ```
  - You might have to create your own virtual environment if you have issues. Edit the venv.sh and then run with ``` source venv.sh ```
    
1. Run Setup
    ```
    #Creates all databases, tables, seeds product data.
    ./setup.sh
    ```
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
- User authentication


