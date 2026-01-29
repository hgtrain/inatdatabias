# bronze_json_to_parquet_inat.py
#
# PURPOSE
# -------
# Canonicalize raw Bronze iNaturalist JSON into Parquet.
# This job converts API JSON pages into a Spark-readable,
# schema-enforced Parquet Bronze dataset.
#
# INPUT (Bronze - raw)
# --------------------
# s3://bhj-analytics/bronze/inat_observations/year=YYYY/*.json
#
# OUTPUT (Bronze - canonical)
# ---------------------------
# s3://bhj-analytics/bronze_parquet/inat_observations/year=YYYY/
#
# NOTES
# -----
# - No deduplication
# - No analytics
# - No enrichment
# - Normalizes event date fields for downstream correctness

import argparse
import os
import sys

from pyspark.sql import functions as F

# Ensure src/ is on PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark_jobs.spark_session import create_spark_session
from common.schemas import BRONZE_INAT_OBSERVATIONS_SCHEMA


def main():
    parser = argparse.ArgumentParser(
        description="Convert Bronze iNaturalist JSON to Parquet"
    )
    parser.add_argument(
        "--bronze-json-path",
        required=True,
        help="S3 path to raw Bronze JSON (year partition)"
    )
    parser.add_argument(
        "--bronze-parquet-path",
        required=True,
        help="S3 path to write canonical Bronze Parquet"
    )

    args = parser.parse_args()

    spark = create_spark_session("BronzeJsonToParquetInat")

    # Read raw JSON with enforced schema
    raw_df = (
        spark.read
        .schema(BRONZE_INAT_OBSERVATIONS_SCHEMA)
        .json(args.bronze_json_path)
    )

    # Normalize event timestamp (critical fix)
    bronze_df = (
        raw_df
        .withColumn(
            "event_date",
            F.to_date(
                F.coalesce(
                    F.col("observed_on"),
                    F.col("observed_at"),
                    F.col("time_observed_at"),
                    F.col("created_at")
                )
            )
        )
        .withColumn(
            "source_year",
            F.year(F.col("event_date"))
        )
    )

    # Write canonical Bronze Parquet
    (
        bronze_df.write
        .mode("overwrite")
        .parquet(args.bronze_parquet_path)
    )

    print(
        f"[DONE] Converted Bronze JSON -> Parquet\n"
        f"Input:  {args.bronze_json_path}\n"
        f"Output: {args.bronze_parquet_path}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
