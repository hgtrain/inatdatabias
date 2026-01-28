# batch_bronze_parkserve_emr.py
#
# PURPOSE
# -------
# One-time / infrequent Bronze batch ingestion for ParkServe park data.
# Pulls spatial reference data from the ArcGIS FeatureServer REST API
# and writes raw JSONL records to S3.
#
# DESIGN NOTES
# ------------
# - Batch-only (dimension dataset)
# - No Spark used
# - Geometry stored as serialized ESRI geometry (string) in Bronze
# - Minimal transformation (Bronze contract)
# - Safe to re-run (append-only files)
#
# OUTPUT
# ------
# s3://bhj-analytics/bronze/parkserve_parks/

import json
import time
import requests
import boto3


# Configuration

PARKSERVE_URL = (
    "https://services.arcgis.com/8DFDwzK6jYQXnL5s/ArcGIS/rest/services/"
    "ParkServe_Parks/FeatureServer/0/query"
)

BUCKET = "bhj-analytics"
BASE_PREFIX = "bronze/parkserve_parks"

s3 = boto3.client("s3")


# Helpers

def upload_jsonl(records: list[dict], batch_id: int) -> None:
    if not records:
        return

    key = (
        f"{BASE_PREFIX}/"
        f"parkserve_batch_{batch_id}_{int(time.time())}.json"
    )

    body = "\n".join(json.dumps(r) for r in records)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8")
    )


def fetch_all_features() -> dict:
    """
    Fetch ALL ParkServe park features using ArcGIS-supported POST + pjson.
    This service returns empty results for GET-based bulk queries.
    """
    data = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": 4326,
        "f": "pjson",
    }

    response = requests.post(PARKSERVE_URL, data=data, timeout=60)

    if response.status_code >= 400:
        print("\n[ERROR] ArcGIS request failed")
        print("Status:", response.status_code)
        print("Response body (first 1000 chars):")
        print(response.text[:1000])
        print()
        response.raise_for_status()

    return response.json()


# Main ingestion

def ingest_parkserve() -> None:
    batch_id = 1

    payload = fetch_all_features()
    features = payload.get("features", [])

    if not features:
        print("[DONE] No ParkServe features returned.")
        return

    records = []

    for feature in features:
        props = feature.get("attributes", {})
        geometry = feature.get("geometry")

        record = {
            "park_id": str(props.get("ParkServeID") or props.get("OBJECTID")),
            "park_name": props.get("ParkName"),
            "county": props.get("County"),
            "state": props.get("State"),
            # store raw ESRI geometry as string (Bronze-safe)
            "geometry_wkt": json.dumps(geometry) if geometry else None,
            "source": "ParkServe",
        }

        records.append(record)

    upload_jsonl(records, batch_id)

    print(
        f"[INGESTED] Batch {batch_id} | "
        f"Records: {len(records)}"
    )
    print(f"[DONE] Total ParkServe records ingested: {len(records)}")


if __name__ == "__main__":
    ingest_parkserve()
