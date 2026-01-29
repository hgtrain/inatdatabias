# gold_observation_counts.py
#
# PURPOSE
# -------
# Gold aggregation job that computes daily observation counts
# by taxonomic group from enriched Silver data.
#
# INPUT (Silver)
# --------------
# s3://bhj-analytics/silver/inat_observations_enriched/
#
# OUTPUT (Gold)
# -------------
# s3://bhj-analytics/gold/gold_observation_counts/
#
# LOGIC
# -----
# Group by:
#   - iconic_taxon_name
#   - observed_date
#   - source_year
#
# Metric:
#   - observation_count = countDistinct(observation_id)

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
        description="Gold aggregation: observation counts by taxon and date"
    )
    parser.add_argument(
        "--silver-path",
        default="s3://bhj-analytics/silver/inat_observations_enriched/",
        help="S3 path to enriched Silver observations"
    )
    parser.add_argument(
        "--gold-path",
        default="s3://bhj-analytics/gold/gold_observation_counts/",
        help="S3 path to write Gold observation counts"
    )
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=["overwrite", "append"],
        help="Write mode for Gold output"
    )

    args = parser.parse_args()

    spark = create_spark_session("GoldObservationCounts")

    # Read enriched Silver dataset
    silver_df = spark.read.parquet(args.silver_path)

    # Aggregate to Gold
    gold_df = (
        silver_df
        .groupBy(
            col("iconic_taxon_name"),
            col("observed_date"),
            col("source_year")
        )
        .agg(
            F.countDistinct("observation_id").alias("observation_count")
        )
    )

    # Write Gold dataset
    (
        gold_df.write
        .mode(args.mode)
        .partitionBy("source_year")
        .parquet(args.gold_path)
    )

    print(f"[DONE] Wrote Gold observation counts to: {args.gold_path}")
    spark.stop()


if __name__ == "__main__":
    main()
