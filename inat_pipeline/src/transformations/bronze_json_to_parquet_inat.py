# bronze_json_to_parquet_inat.py
#
# PURPOSE
# -------
# Canonicalize raw Bronze iNaturalist JSON into Parquet.
# Converts API JSON pages into a Spark-readable, schema-stable
# Bronze Parquet dataset with canonical time fields.
#
# INPUT (Bronze - raw JSON)
# ------------------------
# s3://bhj-analytics/bronze/inat_observations/year=YYYY/*.json
#
# OUTPUT (Bronze - canonical Parquet)
# ----------------------------------
# s3://bhj-analytics/bronze_parquet/inat_observations/year=YYYY/
#
# DESIGN CONTRACT
# ---------------
# - No deduplication
# - No enrichment
# - Minimal normalization ONLY
# - Guarantees event_date + source_year for downstream Silver/Gold

import argparse
import os
import sys

from pyspark.sql import functions as F

# Ensure src/ is on PYTHONPATH for spark-submit on EMR
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark_jobs.spark_session import create_spark_session
from common.schemas import BRONZE_INAT_OBSERVATIONS_SCHEMA


def main():
    parser = argparse.ArgumentParser(
        description="Convert Bronze iNaturalist JSON to canonical Parquet"
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


    #    Canonical time normalization
    #    iNaturalist provides multiple time fields inconsistently.
    #    We coalesce them into a single event_time.
-
    df = (
        raw_df
        .withColumn(
            "event_time",
            F.coalesce(
                F.col("time_observed_at"),
                F.col("observed_on"),
                F.col("created_at")
            )
        )
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("source_year", F.year("event_date"))
    )

    # 3) Select canonical Bronze contract

    bronze_df = df.select(
        F.col("id").alias("observation_id"),
        "event_date",
        "source_year",
        F.col("geojson.coordinates").getItem(1).alias("latitude"),
        F.col("geojson.coordinates").getItem(0).alias("longitude"),
        "quality_grade",
        F.col("taxon.id").alias("taxon_id"),
        F.col("taxon.iconic_taxon_name").alias("iconic_taxon_name"),
        "place_guess"
    )


    # 4) Write canonical Bronze Parquet
    (
        bronze_df.write
        .mode("overwrite")
        .parquet(args.bronze_parquet_path)
    )

    print(
        "[DONE] Converted Bronze JSON -> canonical Parquet\n"
        f"Input:  {args.bronze_json_path}\n"
        f"Output: {args.bronze_parquet_path}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
