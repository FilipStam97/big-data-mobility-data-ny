import json
import xml.etree.ElementTree as ET
import argparse
import time
from kafka import KafkaProducer


FCD_FILE = "data/fcd.xml"
EMISSIONS_FILE = "data/emissions.xml"

TOPIC = "vehicle-data"
BOOTSTRAP_SERVERS = "localhost:9092"


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def read_timesteps(filename):
    """
    Streams one timestep at a time from a SUMO XML output file.

    Yields:
        time, {vehicle_id: attributes}
    """

    for event, elem in ET.iterparse(filename, events=("end",)):
        if elem.tag == "timestep":
            timestamp = float(elem.attrib["time"])

            vehicles = {}

            for vehicle in elem.findall("vehicle"):
                vehicles[vehicle.attrib["id"]] = vehicle.attrib.copy()

            yield timestamp, vehicles

            elem.clear()


def stream_vehicle_data(delay=0):
    fcd_stream = read_timesteps(FCD_FILE)
    emissions_stream = read_timesteps(EMISSIONS_FILE)

    count = 0

    for (fcd_time, fcd_vehicles), (emission_time, emission_vehicles) in zip(
        fcd_stream,
        emissions_stream
    ):
        
        # The two SUMO outputs should represent the same simulation.
        if fcd_time != emission_time:
            print(
                f"Warning: timestep mismatch "
                f"FCD={fcd_time}, emissions={emission_time}"
            )
            continue

        for vehicle_id, fcd in fcd_vehicles.items():

            emission = emission_vehicles.get(vehicle_id)

            if emission is None:
                continue

            message = {
                "timestamp": fcd_time,
                "vehicle_id": vehicle_id,

                # Geographic position from FCD
                "longitude": float(fcd["x"]),
                "latitude": float(fcd["y"]),

                # Vehicle movement
                "speed": float(fcd["speed"]),
                "lane": fcd["lane"],

                # Pollution from emission output
                "co2": float(emission["CO2"]),
                "co": float(emission["CO"]),
                "hc": float(emission["HC"]),
                "nox": float(emission["NOx"]),
                "pmx": float(emission["PMx"]),
                "noise": float(emission["noise"])
            }

            producer.send(TOPIC, value=message)

            count += 1

            if count % 10000 == 0:
                print(f"Sent {count:,} records")

            print(f"Timestep {fcd_time} sent")
            if delay > 0:
                time.sleep(delay)

   
    producer.flush()

    print(f"Finished. Sent {count:,} records.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--delay",
        type=float,
        default=0,
        help="Delay in seconds between simulation timesteps"
    )

    args = parser.parse_args()

    stream_vehicle_data(args.delay)