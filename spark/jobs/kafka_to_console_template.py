from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "FanSalesTopic"

spark = (
    SparkSession.builder.appName("KafkaToConsoleTemplate")
    .getOrCreate()
)

schema = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("fan_id", IntegerType(), True),
        StructField("team", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ]
)

df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

df_parsed = (
    df_raw.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

query = (
    df_parsed.writeStream.outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
