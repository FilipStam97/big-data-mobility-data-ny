import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    approx_count_distinct,
    from_json,
    timestamp_seconds,
    window,
    lit,
    radians,
    sin,
    cos,
    asin,
    sqrt,
    struct,
    to_json,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "vehicle-data"

LANDMARKS = [
    ("Times Square", 40.7580, -73.9855),
    ("Empire State Building", 40.7484, -73.9857),
    ("Grand Central Terminal", 40.7527, -73.9772),
    ("Bryant Park", 40.7536, -73.9832),
]

RADIUS_METERS = 200

#Haversine formula
def distance_from(latitude, longitude):
    earth_radius = 6371000

    lat1 = radians(col("latitude"))
    lon1 = radians(col("longitude"))

    lat2 = radians(lit(latitude))
    lon2 = radians(lit(longitude))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        sin(dlat / 2) ** 2
        + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    )

    return 2 * earth_radius * asin(sqrt(a))


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--window",
        type=int,
        default=60,
        help="Window size in simulation seconds",
    )

    parser.add_argument(
        "--slide",
        type=int,
        default=60,
        help="Slide interval in simulation seconds",
    )

    return parser.parse_args()


args = parse_args()

spark = (
    SparkSession.builder
    .appName("NYC-Vehicle-Analytics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# -------------------------
# Kafka
# -------------------------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)


# -------------------------
# JSON schema
# -------------------------

schema = StructType([
    StructField("timestamp", DoubleType()),
    StructField("vehicle_id", StringType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("speed", DoubleType()),
    StructField("lane", StringType()),
    StructField("co2", DoubleType()),
    StructField("co", DoubleType()),
    StructField("hc", DoubleType()),
    StructField("nox", DoubleType()),
    StructField("pmx", DoubleType()),
    StructField("noise", DoubleType()),
])


vehicles = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
)


# SUMO gives us simulation seconds:
# 0, 1, 2, 3...
#
# Spark's window() expects a TimestampType,
# so interpret those seconds as seconds after Unix epoch.
vehicles = (
    vehicles
    .withColumn(
        "event_time",
        timestamp_seconds(col("timestamp"))
    )
    .withWatermark("event_time", "10 seconds")
)


landmark_streams = []

for name, latitude, longitude in LANDMARKS:

    nearby = (
        vehicles
        .withColumn(
            "distance",
            distance_from(latitude, longitude)
        )
        .filter(col("distance") <= RADIUS_METERS)
        .withColumn("location", lit(name))
    )

    landmark_streams.append(nearby)

spatial_vehicles = landmark_streams[0]

for stream in landmark_streams[1:]:
    spatial_vehicles = spatial_vehicles.unionByName(stream)

# -------------------------
# Window analytics
# -------------------------

window_size = f"{args.window} seconds"
slide_size = f"{args.slide} seconds"

analytics = (
    spatial_vehicles
    .groupBy(
        window(
            col("event_time"),
            window_size,
            slide_size
        ),
        col("location")
    )
    .agg(
        approx_count_distinct("vehicle_id").alias("vehicle_count"),
        avg("speed").alias("avg_speed"),
        avg("co2").alias("avg_co2"),
        avg("co").alias("avg_co"),
        avg("hc").alias("avg_hc"),
        avg("nox").alias("avg_nox"),
        avg("pmx").alias("avg_pmx"),
        avg("noise").alias("avg_noise"),
    )
)


kafka_output = analytics.select(
    to_json(
        struct(
            lit("spark").alias("processor"),

            col("window.start")
                .cast("string")
                .alias("window_start"),

            col("window.end")
                .cast("string")
                .alias("window_end"),

            col("location"),
            col("vehicle_count"),
            col("avg_speed"),
            col("avg_co2"),
            col("avg_co"),
            col("avg_hc"),
            col("avg_nox"),
            col("avg_pmx"),
            col("avg_noise"),
        )
    ).alias("value")
)


# -------------------------
# Console output
# -------------------------

query = (
    kafka_output.writeStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option(
        "topic",
        "analytics-results"
    )
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/spark-analytics"
    )
    .outputMode("append")
    .start()
)

query.awaitTermination()