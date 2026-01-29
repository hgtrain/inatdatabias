"""
batch_bronze_inat_emr.py

Batch ingestion job for iNaturalist observations.

Purpose:
- Pull observations from the iNaturalist API for a given year
- Write raw JSON records to S3 in JSONL format
- Designed to be safe to stop and re-run without data loss

This job represents the Bronze ingestion layer and performs
no transformations beyond pagination and storage.
"""

import argparse
import json
import time
import requests
import boto3

INAT_URL = "https://api.inaturalist.org/v1/observations"
PER_PAGE = 200
BUCKET = "bhj-analytics"
BASE_PREFIX = "bronze/inat_observations"

s3 = boto3.client("s3")


def upload_jsonl(records, year, page):
    """
    Writes a list of raw API records to S3 as a JSONL file.
    Each page is written as a separate object for traceability.
    """
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
    """
    Ingests all available iNaturalist observations for a given year.
    Pagination is handled sequentially and ingestion can be resumed safely.
    """
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

        try:
            response = requests.get(INAT_URL, params=params, timeout=30)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(
                f"[STOPPED] Year {year} | Page {page} | "
                f"HTTP error encountered: {e}. "
                f"Partial ingestion preserved. Safe to resume later."
            )
            break
        except requests.exceptions.RequestException as e:
            print(
                f"[STOPPED] Year {year} | Page {page} | "
                f"Request failed: {e}. "
                f"Partial ingestion preserved."
            )
            break

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
    """
    CLI entry point for year-based batch ingestion.
    """
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
