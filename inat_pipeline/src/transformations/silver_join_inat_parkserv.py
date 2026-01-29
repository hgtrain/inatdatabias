# silver_join_inat_parkserve.py
#
# PURPOSE
# -------
# Silver enrichment job that joins iNaturalist observation data (fact)
# with ParkServe park reference data (dimension) at the county level.
#
# This join provides spatial context while preserving latitude/longitude
# and full park geometries for downstream spatial analysis.
#
# INPUT (Silver)
# --------------
# iNaturalist: s3://bhj-analytics/silver/inat_observations/
# ParkServe:   s3://bhj-analytics/silver/parkserve_parks/
#
# OUTPUT (Silver)
# ---------------
# s3://bhj-analytics/silver/inat_observations_enriched/

import argparse
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.functions import col

# Ensure src/ is on PYTHONPATH for spark-submit
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark_jobs.spark_session import create_spark_session


def main():
    parser = argparse.ArgumentParser(
        description="Silver join: iNaturalist observations enriched with ParkServe data"
    )
    parser.add_argument(
        "--inat-path",
        default="s3://bhj-analytics/silver/inat_observations/",
        help="S3 path to Silver iNaturalist observations"
    )
    parser.add_argument(
        "--parkserve-path",
        default="s3://bhj-analytics/silver/parkserve_parks/",
        help="S3 path to Silver ParkServe parks"
    )
    parser.add_argument(
        "--silver-path",
        default="s3://bhj-analytics/silver/inat_observations_enriched/",
        help="S3 path to write enriched Silver output"
    )
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=["overwrite", "append"],
        help="Write mode for output"
    )

    args = parser.parse_args()

    spark = create_spark_session("SilverJoinInatParkServe")

    # Read Silver datasets
    inat_df = spark.read.parquet(args.inat_path).alias("inat")
    park_df = spark.read.parquet(args.parkserve_path).alias("park")

    # Normalize join keys defensively
    inat_df = inat_df.withColumn(
        "county_norm", F.upper(F.trim(col("inat.county")))
    )
    park_df = park_df.withColumn(
        "county_norm", F.upper(F.trim(col("park.county")))
    )

    # Left join to preserve all observation events
    joined_df = (
        inat_df
        .join(
            park_df,
            on="county_norm",
            how="left"
        )
    )

    # Select final Silver schema
    silver_enriched_df = joined_df.select(
        # iNaturalist (fact)
        col("observation_id"),
        col("observed_date"),
        col("latitude"),
        col("longitude"),
        col("county"),
        col("state"),
        col("taxon_id"),
        col("iconic_taxon_name"),
        col("quality_grade"),
        col("source_year"),

        # ParkServe (dimension)
        col("park.park_id"),
        col("park.park_name"),
        col("park.geometry_json"),
        F.lit("ParkServe").alias("park_source")
    )

    # Write enriched Silver dataset
    (
        silver_enriched_df.write
        .mode(args.mode)
        .partitionBy("state", "source_year")
        .parquet(args.silver_path)
    )

    print(f"[DONE] Wrote enriched Silver dataset to: {args.silver_path}")
    spark.stop()


if __name__ == "__main__":
    main()
