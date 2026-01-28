# src/ingestion/batch/batch_bronze_inat_emr.py

import argparse
import json
import time
import requests
import boto3
from datetime import datetime

INAT_URL = "https://api.inaturalist.org/v1/observations"
PER_PAGE = 200
BUCKET = "bhj-analytics"
BASE_PREFIX = "bronze/inat_observations"

s3 = boto3.client("s3")


def upload_jsonl(records, year, page):
    if not records:
        return

    key = (
        f"{BASE_PREFIX}/year={year}/"
        f"inat_{year}_page_{page}_{int(time.time())}.json"
    )

    body = "\n".join(json.dumps(r) for r in records)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8")
    )


def ingest_year(year: int):
    page = 1
    total_records = 0

    while True:
        params = {
            "year": year,
            "per_page": PER_PAGE,
            "page": page,
            "order": "asc",
            "order_by": "created_at"
        }

        response = requests.get(INAT_URL, params=params, timeout=30)
        response.raise_for_status()

        payload = response.json()
        results = payload.get("results", [])

        if not results:
            print(f"[DONE] Year {year} | Total records: {total_records}")
            break

        upload_jsonl(results, year, page)
        total_records += len(results)

        print(
            f"[INGESTED] Year {year} | Page {page} | "
            f"Records this page: {len(results)} | Total: {total_records}"
        )

        page += 1
        time.sleep(1)  # polite rate limiting


def main():
    parser = argparse.ArgumentParser(
        description="Batch Bronze ingestion for iNaturalist observations"
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year to ingest (e.g. 2019 or 2020)"
    )

    args = parser.parse_args()
    ingest_year(args.year)


if __name__ == "__main__":
    main()
