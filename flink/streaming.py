from pyflink.common import SimpleStringSchema, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
import json
from datetime import timedelta

from pyflink.common.watermark_strategy import (
    WatermarkStrategy,
    TimestampAssigner,
)
from math import radians, sin, cos, sqrt, asin
from pyflink.common import Types, Time
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
import argparse

from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    SlidingEventTimeWindows,
)

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
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


class VehicleTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        # value[0] = SUMO timestamp in seconds
        return int(value[0] * 1000)


class VehicleAggregate(AggregateFunction):

    def create_accumulator(self):
        # count, vehicle_ids, speed, co2, co, hc, nox, pmx, noise
        return (0, set(), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    def add(self, value, acc):
        count, vehicle_ids, speed, co2, co, hc, nox, pmx, noise = acc

        vehicle_ids.add(value[2])

        return (
            count + 1,
            vehicle_ids,
            speed + value[5],
            co2 + value[7],
            co + value[8],
            hc + value[9],
            nox + value[10],
            pmx + value[11],
            noise + value[12],
        )

    def get_result(self, acc):
        count, vehicle_ids, speed, co2, co, hc, nox, pmx, noise = acc

        return (
            len(vehicle_ids),
            speed / count,
            co2 / count,
            co / count,
            hc / count,
            nox / count,
            pmx / count,
            noise / count,
        )

    def merge(self, a, b):
        return (
            a[0] + b[0],
            a[1] | b[1],
            a[2] + b[2],
            a[3] + b[3],
            a[4] + b[4],
            a[5] + b[5],
            a[6] + b[6],
            a[7] + b[7],
            a[8] + b[8],
        )

class WindowResult(ProcessWindowFunction):

    def process(self, key, context, aggregates):
        result = next(iter(aggregates))

        yield (
            key,
            context.window().start,
            context.window().end,
            *result
        )


def haversine(lat1, lon1, lat2, lon2):
    earth_radius = 6371000

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        sin(dlat / 2) ** 2
        + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    )

    return 2 * earth_radius * asin(sqrt(a))

def match_landmarks(vehicle):
    latitude = vehicle[3]
    longitude = vehicle[2]

    matches = []

    for name, landmark_lat, landmark_lon in LANDMARKS:
        distance = haversine(
            latitude,
            longitude,
            landmark_lat,
            landmark_lon
        )

        if distance <= RADIUS_METERS:
            matches.append((name,) + vehicle)

    return matches

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

def analytics_to_json(value):
    return json.dumps({
        "processor": "flink",
        "window_start": value[1],
        "window_end": value[2],
        "location": value[0],
        "vehicle_count": value[3],
        "avg_speed": value[4],
        "avg_co2": value[5],
        "avg_co": value[6],
        "avg_hc": value[7],
        "avg_nox": value[8],
        "avg_pmx": value[9],
        "avg_noise": value[10],
    })

###-----------------------------------------------------------------

parser = argparse.ArgumentParser()

parser.add_argument(
    "--window",
    type=int,
    default=60,
    help="Window size in seconds"
)

parser.add_argument(
    "--slide",
    type=int,
    default=60,
    help="Slide interval in seconds"
)

args = parser.parse_args()

WINDOW_SECONDS = args.window
SLIDE_SECONDS = args.slide

print(
    f"Window: {WINDOW_SECONDS}s, "
    f"Slide: {SLIDE_SECONDS}s"
)

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


vehicles = vehicles.map(parse_vehicle)

watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(10))
    .with_timestamp_assigner(VehicleTimestampAssigner())
)

vehicles = vehicles.assign_timestamps_and_watermarks(
    watermark_strategy
)

spatial_vehicles = vehicles.flat_map(match_landmarks)

if WINDOW_SECONDS == SLIDE_SECONDS:
    window_assigner = TumblingEventTimeWindows.of(
        Time.seconds(WINDOW_SECONDS)
    )
else:
    window_assigner = SlidingEventTimeWindows.of(
        Time.seconds(WINDOW_SECONDS),
        Time.seconds(SLIDE_SECONDS)
    )

analytics = (
    spatial_vehicles
    .key_by(lambda x: x[0])
    .window(window_assigner)
    .aggregate(
        VehicleAggregate(),
        WindowResult()
    )
)

analytics_json = analytics.map(
    analytics_to_json,
    output_type=Types.STRING()
)

sink = (
    KafkaSink.builder()
    .set_bootstrap_servers("kafka:29092")
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic("analytics-results")
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    .build()
)

analytics_json.sink_to(sink)

env.execute("NYC Vehicle Streaming")