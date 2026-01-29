# gold_state_year_observation_summary.py
#
# PURPOSE
# -------
# Gold aggregation job.
# Combines Silver iNaturalist observations with Silver ParkServe parks
# to produce state-year level biodiversity metrics enriched with park counts.
#
# INPUT (Silver)
# --------------
# iNat:      s3://bhj-analytics/silver/inat_observations/
# ParkServe: s3://bhj-analytics/silver/parkserve_parks/
#
# OUTPUT (Gold)
# -------------
# s3://bhj-analytics/gold/state_year_observation_summary/
#
# NOTES
# -----
# - Join is state-level (not spatial)
# - Geometry is intentionally not used at Gold
# - Designed for BI dashboards and downstream marts

import os
import sys
import argparse
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from spark_jobs.spark_session import create_spark_session


def main():
    parser = argparse.ArgumentParser(description="Gold state-year biodiversity summary")
    parser.add_argument(
        "--inat-path",
        default="s3://bhj-analytics/silver/inat_observations/",
        help="Silver iNaturalist path"
    )
    parser.add_argument(
        "--parkserve-path",
        default="s3://bhj-analytics/silver/parkserve_parks/",
        help="Silver ParkServe path"
    )
    parser.add_argument(
        "--gold-path",
        default="s3://bhj-analytics/gold/state_year_observation_summary/",
        help="Gold output path"
    )
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=["overwrite", "append"]
    )

    args = parser.parse_args()
    spark = create_spark_session("GoldStateYearObservationSummary")

    # Load Silver tables
    inat = spark.read.parquet(args.inat_path)
    parks = spark.read.parquet(args.parkserve_path)

    # --- Aggregate iNat ---
    inat_agg = (
        inat
        .groupBy("state", "source_year")
        .agg(
            F.count("*").alias("total_observations"),
            F.countDistinct("taxon_id").alias("distinct_taxa"),
            F.sum(
                F.when(F.col("quality_grade") == "research", 1).otherwise(0)
            ).alias("research_grade_obs")
        )
    )

    # --- Aggregate ParkServe ---
    parks_agg = (
        parks
        .groupBy("state")
        .agg(
            F.countDistinct("park_id").alias("park_count")
        )
    )

    # --- Join ---
    gold_df = (
        inat_agg
        .join(parks_agg, on="state", how="left")
        .fillna({"park_count": 0})
    )

    # --- Write Gold ---
    (
        gold_df
        .write
        .mode(args.mode)
        .partitionBy("source_year")
        .parquet(args.gold_path)
    )

    print(f"[DONE] Gold data written to {args.gold_path}")
    spark.stop()


if __name__ == "__main__":
    main()
