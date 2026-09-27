import json
import folium
from kafka import KafkaConsumer

TOPIC = "analytics-results"
BOOTSTRAP_SERVERS = "localhost:9092"
MAP_FILE = "consumer/analytics_map.html"

LOCATIONS = {
    "Times Square": (40.7580, -73.9855),
    "Empire State Building": (40.7484, -73.9857),
    "Grand Central Terminal": (40.7527, -73.9772),
    "Bryant Park": (40.7536, -73.9832),
}

latest_results = {}


def generate_map():
    map_ = folium.Map(
        location=[40.7545, -73.9850],
        zoom_start=15
    )

    for location, coordinates in LOCATIONS.items():
        results = [
            result
            for (processor, result_location), result in latest_results.items()
            if result_location == location
        ]

        if not results:
            continue

        popup = f"<h4>{location}</h4>"

        METRIC_LABELS = {
            "avg_speed": "Avg speed",
            "avg_co2": "CO2",
            "avg_co": "CO",
            "avg_hc": "HC",
            "avg_nox": "NOx",
            "avg_pmx": "PMx",
            "avg_noise": "Noise",
        }

        for result in results:
            popup += f"""
            <b>{result['processor'].upper()}</b><br>
            Window: {result['window_start']} - {result['window_end']}<br>
            Vehicles: {result['vehicle_count']}<br>
            """

            for key, label in METRIC_LABELS.items():
                if key in result:
                    popup += f"{label}: {result[key]:.2f}<br>"

            popup += "<hr>"

        folium.Marker(
            location=coordinates,
            popup=folium.Popup(popup, max_width=350),
            tooltip=location
        ).add_to(map_)

    map_.save(MAP_FILE)


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Listening to {TOPIC}...")

for message in consumer:
    data = message.value

    key = (data["processor"], data["location"])
    latest_results[key] = data

    print(
        f"[{data['processor'].upper()}] "
        f"{data['location']} | "
        f"vehicles={data['vehicle_count']} | "
        f"speed={data['avg_speed']:.2f} | "
        f"CO2={data['avg_co2']:.2f}"
    )

    generate_map()