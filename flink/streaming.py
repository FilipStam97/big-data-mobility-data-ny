from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
import json

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "vehicle-data"


env = StreamExecutionEnvironment.get_execution_environment()


source = (
    KafkaSource.builder()
    .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
    .set_topics(KAFKA_TOPIC)
    .set_group_id("flink-vehicle-consumer")
    .set_starting_offsets(
        KafkaOffsetsInitializer.latest()
    )
    .set_value_only_deserializer(
        SimpleStringSchema()
    )
    .build()
)


vehicles = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Vehicle Kafka Source"
)


def parse_vehicle(value):
    data = json.loads(value)

    return (
        float(data["timestamp"]),
        data["vehicle_id"],
        float(data["longitude"]),
        float(data["latitude"]),
        float(data["speed"]),
        data["lane"],
        float(data["co2"]),
        float(data["co"]),
        float(data["hc"]),
        float(data["nox"]),
        float(data["pmx"]),
        float(data["noise"]),
    )


vehicles = vehicles.map(parse_vehicle)

vehicles.print()

env.execute("NYC Vehicle Streaming")