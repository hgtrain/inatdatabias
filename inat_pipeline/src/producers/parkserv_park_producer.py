#parkserve_parks_producer.py
#Publishes ParkServe park reference records (dimension stream) to Kafka.


import json
import os
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "parkserve_parks"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Minimal reference data - to be replaced later with real exports
PARKS = [
    {
        "park_id": "NJ-0001",
        "park_name": "Branch Brook Park",
        "county": "Essex",
        "state": "New Jersey",
        "geometry_wkt": None,
        "source": "ParkServe"
    },
    {
        "park_id": "NJ-0002",
        "park_name": "Liberty State Park",
        "county": "Hudson",
        "state": "New Jersey",
        "geometry_wkt": None,
        "source": "ParkServe"
    },
    {
        "park_id": "NJ-0003",
        "park_name": "Washington Rock State Park",
        "county": "Somerset",
        "state": "New Jersey",
        "geometry_wkt": None,
        "source": "ParkServe"
    },
]

if __name__ == "__main__":
    print("Publishing park records to Kafka...")
    for park in PARKS:
        producer.send(KAFKA_TOPIC, value=park)
        print(f"Published park_id={park['park_id']}")
    producer.flush()
    producer.close()
    print("ParkServe producer finished.")