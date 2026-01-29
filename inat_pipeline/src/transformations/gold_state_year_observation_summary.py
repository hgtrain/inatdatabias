"""
gold_state_year_observation_summary.py

Gold aggregation summarizing iNaturalist observation volume by year.

Purpose:
- Aggregate total observation counts by source_year
- Support high-level comparison of pre-COVID (2019) vs COVID-era (2020) activity

Input (Silver):
- s3://bhj-analytics/silver_v2/inat_observations/

Output (Gold):
- s3://bhj-analytics/gold/state_year_observation_summary/
"""

import argparse
import os
import sys

from pyspark.sql import functions as F

# Ensure src/ is on PYTHONPATH for spark-submit
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark_jobs.spark_session import create_spark_session


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--silver-path",
        default="s3://bhj-analytics/silver_v2/inat_observations/",
        help="Silver iNaturalist path"
    )
    parser.add_argument(
        "--gold-path",
        default="s3://bhj-analytics/gold/state_year_observation_summary/",
        help="Gold output path"
    )

    args = parser.parse_args()
    spark = create_spark_session("GoldStateYearObservationSummary")

    silver = spark.read.parquet(args.silver_path)

    gold = (
        silver
        .groupBy("source_year")
        .agg(
            F.count("*").alias("observation_count")
        )
        .orderBy("source_year")
    )

    (
        gold.write
        .mode("overwrite")
        .parquet(args.gold_path)
    )

    gold.show()
    spark.stop()


if __name__ == "__main__":
    main()
