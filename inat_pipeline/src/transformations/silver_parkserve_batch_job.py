# silver_parkserve_batch_job.py
#
# PURPOSE
# -------
# Silver batch job for ParkServe spatial reference data.
# Reads the Bronze GeoJSON (FeatureCollection) from S3, normalizes a small set
# of fields, and writes Silver parquet back to S3.
#
# INPUT (Bronze)
# --------------
# s3://bhj-analytics/bronze/parkserve_parks/parkserve_raw.geojson
#
# OUTPUT (Silver)
# ---------------
# s3://bhj-analytics/silver/parkserve_parks/
#
# NOTES
# -----
# - Bronze GeoJSON is large and nested: {"type":"FeatureCollection","features":[...]}
# - Spark does not natively understand GeoJSON without extra libraries
# - We read the file as JSON and explode the "features" array
# - Geometry is intentionally preserved as serialized JSON
#   Spatial transformations are deferred to downstream PostGIS/GIS tooling

import argparse
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.functions import col

# Ensure src/ is on PYTHONPATH (required for spark-submit on EMR)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark_jobs.spark_session import create_spark_session


def normalize_string(c):
    """Trim whitespace and convert empty strings to null."""
    return F.when(F.trim(c) == "", None).otherwise(F.trim(c))


def main():
    parser = argparse.ArgumentParser(
        description="Silver batch job for ParkServe GeoJSON"
    )
    parser.add_argument(
        "--bronze-path",
        default="s3://bhj-analytics/bronze/parkserve_parks/parkserve_raw.geojson",
        help="S3 path to Bronze ParkServe GeoJSON"
    )
    parser.add_argument(
        "--silver-path",
        default="s3://bhj-analytics/silver/parkserve_parks/",
        help="S3 path to write Silver parquet"
    )
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=["overwrite", "append"],
        help="Write mode for Silver output"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit for dev testing (e.g., 1000). Processes all features if omitted."
    )

    args = parser.parse_args()

    spark = create_spark_session("SilverParkServeBatchJob")

    # 1) Read GeoJSON as JSON (multiline FeatureCollection)
    raw_df = (
        spark.read
        .option("multiline", "true")
        .json(args.bronze_path)
    )

    # 2) Explode features array -> one row per park
    features_df = raw_df.select(
        F.explode(col("features")).alias("feature")
    )

    if args.limit:
        features_df = features_df.limit(args.limit)

    # 3) Extract properties and geometry
    props = col("feature.properties")
    geom = col("feature.geometry")

    # 4) Normalize attribute fields (robust across ParkServe exports)
    park_id = F.coalesce(
        props.getItem("ParkID"),
        props.getItem("GISTrkrID"),
        props.getItem("SourceID"),
    )

    park_name = props.getItem("Park_Name")


    county = props.getItem("Park_County")

    state = props.getItem("Park_State")

    # 5) Preserve geometry as serialized JSON
    geometry_json = F.to_json(geom)

    silver_df = (
        features_df
        .select(
            park_id.cast("string").alias("park_id"),
            park_name.cast("string").alias("park_name"),
            county.cast("string").alias("county"),
            state.cast("string").alias("state"),
            geometry_json.alias("geometry_json"),
            F.lit("ParkServe").alias("source")
        )
        # 6) Light cleanup
        .withColumn("park_id", normalize_string(col("park_id")))
        .withColumn("park_name", normalize_string(col("park_name")))
        .withColumn("county", normalize_string(col("county")))
        .withColumn("state", normalize_string(col("state")))
        # 7) Drop invalid / duplicate dimension rows
        .filter(col("park_id").isNotNull())
        .dropDuplicates(["park_id"])
    )

    # 8) Write Silver parquet (partitioned by state for downstream filtering)
    (
        silver_df.write
        .mode(args.mode)
        .partitionBy("state")
        .parquet(args.silver_path)
    )

    print(f"[DONE] Wrote Silver ParkServe parquet to: {args.silver_path}")
    spark.stop()


if __name__ == "__main__":
    main()
