from pyspark.sql import SparkSession
from pyspark.sql.functions import col


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "vehicle-data"


spark = (
    SparkSession.builder
    .appName("NYC-Vehicle-Streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Read stream from Kafka
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)


# Kafka stores key/value as binary, so convert value to string
messages = raw_stream.select(
    col("value").cast("string").alias("json")
)


query = (
    messages.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()