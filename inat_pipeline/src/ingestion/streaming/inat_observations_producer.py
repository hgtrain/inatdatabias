"""
iNaturalist Observations Kafka Producer

Supports two modes:
1) historical --year YYYY   (finite backfill)
2) stream --since YYYY-MM-DD (incremental polling)

Note:
Historical mode is supported for completeness, but this project uses batch ingestion for historical backfills. 
Kafka streaming is used only for live/incremental observations. 

"""

import argparse
import json
import os
import time
from datetime import datetime
from typing import Optional

import requests
from kafka import KafkaProducer


# Configuration

API_URL = "https://api.inaturalist.org/v1/observations"
PLACE_ID_NJ = 51
PER_PAGE = 200
POLL_INTERVAL_SECONDS = 60

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = "inat_observations"

if not KAFKA_BOOTSTRAP_SERVERS:
    raise RuntimeError(
        "KAFKA_BOOTSTRAP_SERVERS is required (e.g. ip-172-31-x-x.ec2.internal:9092)"
    )

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Helpers

def fetch_page(params: dict) -> list[dict]:
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()["results"]


def transform_observation(obs: dict) -> dict:
    observed_on = obs.get("observed_on")

    return {
    "observation_id": obs.get("id"),
    "taxon_id": obs.get("taxon", {}).get("id"),
    "observed_date": observed_on,
    "latitude": obs.get("geojson", {}).get("coordinates", [None, None])[1],
    "longitude": obs.get("geojson", {}).get("coordinates", [None, None])[0],
    "iconic_taxon_name": obs.get("taxon", {}).get("iconic_taxon_name"),
    "place_guess": obs.get("place_guess"),
    "quality_grade": obs.get("quality_grade"),
    "created_at": obs.get("created_at"),
}



def publish(observations: list[dict]) -> None:
    for obs in observations:
        event = transform_observation(obs)
        producer.send(KAFKA_TOPIC, value=event)
        print(f"Published observation_id={event['observation_id']}")


# Modes

def run_historical(year: int) -> None:
    print(f"Starting historical backfill for year {year}")

    page = 1
    while True:
        params = {
            "place_id": PLACE_ID_NJ,
            "per_page": PER_PAGE,
            "page": page,
            "order": "asc",
            "order_by": "observed_on",
            "year": year,
        }

        results = fetch_page(params)
        if not results:
            break

        publish(results)
        page += 1

    print(f"Historical backfill complete for year {year}")


def run_stream(since_date: str) -> None:
    print(f"Starting streaming mode since {since_date}")

    last_seen: Optional[str] = since_date

    while True:
        params = {
            "place_id": PLACE_ID_NJ,
            "per_page": PER_PAGE,
            "order": "asc",
            "order_by": "observed_on",
            "d1": last_seen,
        }

        results = fetch_page(params)
        if results:
            publish(results)
            last_seen = results[-1].get("observed_on", last_seen)

        time.sleep(POLL_INTERVAL_SECONDS)


# Entrypoint

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=["historical", "stream"])
    parser.add_argument("--year", type=int)
    parser.add_argument("--since")

    args = parser.parse_args()

    if args.mode == "historical":
        if not args.year:
            raise ValueError("--year is required for historical mode")
        run_historical(args.year)

    if args.mode == "stream":
        if not args.since:
            raise ValueError("--since is required for stream mode")
        run_stream(args.since)
