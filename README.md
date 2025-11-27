# EPL Pipeline - Full Big Data Stack

This project gives you a full EPL fan-sales big data stack with:

- Kafka + Zookeeper
- Spark Master + Worker
- HDFS NameNode + DataNode
- Cassandra
- MySQL
- Grafana
- Python recommender container
- Local Python producer
- Local Streamlit dashboard

## 1. Prereqs

- Docker & Docker Compose installed
- Python 3.9+ installed
- Recommended: a virtualenv for Python code (e.g. `epl_venv`)

## 2. Start the Docker stack

From the project root:

```bash
cd EPL_Pipeline
docker compose up -d
```

Verify containers:

```bash
docker ps
```

You should see containers for:
- kafka
- zookeeper
- namenode / datanode
- mysql
- cassandra
- spark-master / spark-worker
- recommender
- visualization

## 3. Set up your Python environment (on the host)

```bash
python3 -m venv epl_venv
source epl_venv/bin/activate
pip install kafka-python streamlit
```

## 4. Run the Kafka producer

```bash
cd pipelines
source ../epl_venv/bin/activate   # if not already active
./run_producer.sh
```

This will generate synthetic fan sales events into Kafka topic `FanSalesTopic`.

## 5. Run the Streamlit dashboard

In a new terminal:

```bash
cd EPL_Pipeline/pipelines
source ../epl_venv/bin/activate
./run_dashboard.sh
```

Then open: <http://localhost:8501>

The example dashboard currently visualizes synthetic data and gives you a template to connect to Cassandra/MySQL later.

## 6. Stop the stack

From the project root:

```bash
docker compose down
```

You now have a clean, reproducible EPL big data environment.
