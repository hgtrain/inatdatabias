"""
silver_parkserve_batch_job.py

Silver batch job for ParkServe spatial reference data.

Purpose:
- Read Bronze ParkServe GeoJSON (FeatureCollection)
- Normalize a small set of reference attributes
- Preserve park geometry for downstream spatial analysis
- Write Silver parquet for use as a dimension dataset

Input (Bronze):
- s3://bhj-analytics/bronze/parkserve_parks/parkserve_raw.geojson

Output (Silver):
- s3://bhj-analytics/silver/parkserve_parks/

Notes:
- ParkServe data is delivered as a large, nested GeoJSON FeatureCollection
- Spark does not natively process GeoJSON geometry
- Geometry is preserved as serialized JSON
- Spatial transformations are intentionally deferred to PostGIS / GIS tooling
"""

import argparse
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.functions import col

# Ensure src/ is on PYTHONPATH for spark-submit
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark_jobs.spark_session import create_spark_session


def normalize_string(c):
    """
    Normalize string attributes by trimming whitespace
    and converting empty values to null.
    """
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

    # Read GeoJSON as JSON (multiline FeatureCollection)
    raw_df = (
        spark.read
        .option("multiline", "true")
        .json(args.bronze_path)
    )

    # Explode features array -> one row per park
    features_df = raw_df.select(
        F.explode(col("features")).alias("feature")
    )

    if args.limit:
        features_df = features_df.limit(args.limit)

    # Extract properties and geometry
    props = col("feature.properties")
    geom = col("feature.geometry")

    # Park ideentifiers vary across ParkServe exports
    park_id = F.coalesce(
        props.getItem("ParkID"),
        props.getItem("GISTrkrID"),
        props.getItem("SourceID"),
    )

    park_name = props.getItem("Park_Name")


    county = props.getItem("Park_County")

    state = props.getItem("Park_State")

    # Preserve geometry as serialized JSON
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
        # Light cleanup
        .withColumn("park_id", normalize_string(col("park_id")))
        .withColumn("park_name", normalize_string(col("park_name")))
        .withColumn("county", normalize_string(col("county")))
        .withColumn("state", normalize_string(col("state")))
        # Drop invalid / duplicate dimension rows
        .filter(col("park_id").isNotNull())
        .dropDuplicates(["park_id"])
    )

    # Write Silver parquet (partitioned by state for downstream filtering)
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
