import pandas as pd
import mysql.connector
from hdfs import InsecureClient

# -----------------------------
# MySQL Connection
# -----------------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="epl"
)

df = pd.read_sql("SELECT * FROM fan_sales", conn)
conn.close()

# -----------------------------
# HDFS Connection (WORKING)
# -----------------------------
# IMPORTANT: Use the NameNode web port (9870)
client = InsecureClient("http://namenode:9870", user="root")

# Force client URL override (fixes datanode host resolution)
client._url = "http://localhost:9870"

# -----------------------------
# Write to HDFS
# -----------------------------
print("Writing to HDFS...")

with client.write("/epl/raw/fan_sales.json", overwrite=True, encoding="utf-8") as writer:
    df.to_json(writer, orient="records")

print("Saved to HDFS successfully!")
