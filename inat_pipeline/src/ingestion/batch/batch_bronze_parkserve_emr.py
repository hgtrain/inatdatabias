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
# - Geometry stored as WKT string
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

S3_CLIENT = boto3.client("s3")

PAGE_SIZE = 1000  # ArcGIS default max is often 1000


# Helpers


def upload_jsonl(records: list[dict], batch_id: int) -> None:
    """
    Write a list of dicts as JSON Lines to S3.
    """
    if not records:
        return

    key = (
        f"{BASE_PREFIX}/"
        f"parkserve_batch_{batch_id}_{int(time.time())}.json"
    )

    body = "\n".join(json.dumps(r) for r in records)

    S3_CLIENT.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8")
    )


def fetch_page(offset: int) -> dict:
    """
    Fetch a single page from the ArcGIS FeatureServer.
    """
    params = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": 4326,
        "f": "geojson",
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
    }

    response = requests.get(PARKSERVE_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def geojson_to_wkt(geometry):
    """
    Very lightweight GeoJSON -> WKT conversion.
    Not validating geometry deeply in Bronze.
    """
    if not geometry:
        return None

    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")

    if not geom_type or not coords:
        return None

    # This is intentionally simple and safe for Bronze.
    # Full spatial correctness is deferred to Silver/PostGIS.
    return f"{geom_type.upper()} {json.dumps(coords)}"



# Main ingestion
def ingest_parkserve() -> None:
    offset = 0
    batch_id = 1
    total_records = 0

    while True:
        payload = fetch_page(offset)
        features = payload.get("features", [])

        if not features:
            print(f"[DONE] Total ParkServe records ingested: {total_records}")
            break

        records = []

        for feature in features:
            props = feature.get("properties", {})
            geometry = feature.get("geometry")

            record = {
                "park_id": str(props.get("ParkServeID") or props.get("OBJECTID")),
                "park_name": props.get("ParkName"),
                "county": props.get("County"),
                "state": props.get("State"),
                "geometry_wkt": geojson_to_wkt(geometry),
                "source": "ParkServe",
            }

            records.append(record)

        upload_jsonl(records, batch_id)

        total_records += len(records)
        print(
            f"[INGESTED] Batch {batch_id} | "
            f"Records: {len(records)} | "
            f"Total: {total_records}"
        )

        offset += PAGE_SIZE
        batch_id += 1
        time.sleep(0.5)  

if __name__ == "__main__":
    ingest_parkserve()
