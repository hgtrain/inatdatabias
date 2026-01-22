# inat_observations_producer.py

# Acts as an upstream ingestion service that polls the iNaturalist Observations API and publishes observation events to a Kafka topic

# Responsibilities:
#Call iNaturalist API (polling-based ingestion)
#Transform API responses into event-shaped JSON records
#Publish events to Kafka for downstream processing 


import json
import time
import os
import requests
from kafka import KafkaProducer

# Kafka Configuration 
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC = "inat_observations"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

#iNaturalist API Configuration
API_URL = "https://api.inaturalist.org/v1/observations"

API_PARAMS = {
    "place_id": 51,          # New Jersey
    "per_page": 5,           # small volume for pipeline validation
    "order": "desc",
    "order_by": "observed_on"
}

POLL_INTERVAL_SECONDS = 60

# Functions
def fetch_observations():
    """Fetch recent observations from the iNaturalist API."""
    response = requests.get(API_URL, params=API_PARAMS)
    response.raise_for_status()
    return response.json()["results"]

def transform_observation(obs: dict) -> dict:
    """
    Transform raw API response into a Kafka event payload
    that matches the Bronze ingestion schema.
    """
    return {
        "observation_id": obs["id"],
        "observed_at": obs.get("observed_on"),
        "observed_date": obs.get("observed_on"),
        "latitude": obs["geojson"]["coordinates"][1] if obs.get("geojson") else None,
        "longitude": obs["geojson"]["coordinates"][0] if obs.get("geojson") else None,
        "county": obs.get("place_guess"),  # temporary placeholder
        "state": "New Jersey",
        "taxon_id": obs["taxon"]["id"] if obs.get("taxon") else None,
        "iconic_taxon_name": obs["taxon"]["iconic_taxon_name"] if obs.get("taxon") else None,
        "quality_grade": obs.get("quality_grade"),
        "source_year": int(obs["observed_on"][:4]) if obs.get("observed_on") else None
    }
    
# Main Loop  (Polling ingestion)
if __name__ == "__main__":
    print("Starting iNaturalist Kafka producer...")
    while True:
        observations = fetch_observations()
        for obs in observations:
            event = transform_observation(obs)
            producer.send(KAFKA_TOPIC, value=event)
            print(f"Published observation_id={event['observation_id']}")
        time.sleep(POLL_INTERVAL_SECONDS)