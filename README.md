# EPL Fan Sales Big Data Pipeline
A full end-to-end big data project simulating English Premier League (EPL) fan purchase behavior using **Kafka, MySQL, Cassandra, Docker, Streamlit**, and a custom real-time **fan storefront with recommendation engine**.

This project demonstrates real-time data engineering, distributed storage, streaming analytics, and applied machine learning-style pattern mining for recommendations.

## 📌 Project Overview
This system simulates a live online store for EPL merchandise and processes transactions through a scalable big-data pipeline. The data flows through:

1. **Kafka Producer** — generates synthetic sales events  
2. **Kafka Consumer** — processes and writes events into MySQL + Cassandra  
3. **MySQL** — holds the real-time "live sales" table  
4. **Cassandra** — stores historical, append-only sales  
5. **Streamlit Retail Dashboard** — used by analysts to track sales, fan behavior, and recommendation conversions  
6. **Streamlit Fan Storefront** — a customer-facing store with a real shopping cart and “People Also Bought” recommendation engine  

This project models a production-like ecosystem for streaming analytics and fan commerce.

## 🧰 Technologies Used
- Python  
- Apache Kafka  
- MySQL  
- Cassandra  
- Docker / Docker Compose  
- Streamlit  
- Pandas  

## 📡 Data Pipeline Components

### 🔹 Kafka Producer — `new_producer.py`
Generates synthetic EPL fan purchase events including fan ID, product, team, region, quantity, and price.

Run:
```
./run_producer.sh
```

### 🔹 Kafka Consumer — `newest_consumer.py`
Writes events into:
- **MySQL** (real-time table)
- **Cassandra** (historical storage)

Run:
```
./run_consumer.sh
```

### 🔹 MySQL (Live Sales)
Used for revenue tracking, real-time dashboards, and transaction monitoring.

### 🔹 Cassandra (Historical Sales)
Used for long-term trend analysis and recommendations.

## 📊 Retail Analytics Dashboard
Located in:
```
retail_dashboard.py
```

Features:
- Live sales metrics  
- Revenue timelines  
- Product performance  
- Historical summaries  

Run:
```
./run_dashboard.sh
```

## 🛒 Fan Storefront
Located in:
```
fan_store.py
```

Features:
- Browse EPL merchandise  
- Real shopping cart  
- Cart-based recommendation engine ("People Also Bought")  
- Checkout writes back to MySQL/Cassandra  

## 🤖 Recommendation Engine
Uses **co-purchase frequency modeling**:
1. Look at items in cart  
2. Find fans who bought similar items  
3. Count what else they bought  
4. Recommend the top items (top 2)

## 📂 File Structure
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
└── README.md
```

## 🚀 How to Run

Start services:
```
docker compose up -d
```

Run components:
```
./run_producer.sh
./run_consumer.sh
./run_dashboard.sh
```

## 📈 Future Improvements
- ML-based recommendations  
- Pricing logic  
- Inventory simulation  
- Fan-level personalization  

## 📜 License
MIT License.
