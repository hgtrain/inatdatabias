"""
gold_taxa_diversity.py

Gold aggregation job that computes taxonomic diversity over time.

Purpose:
- Measure biodiversity diversity by counting distinct taxa per day
- Compare diversity patterns across years (e.g., 2019 vs 2020)

Input (Silver):
- Canonical iNaturalist observation data

Output (Gold):
- Daily distinct taxon counts, partitioned by source_year
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def main(input_path: str, output_path: str):
    spark = create_spark_session("gold_taxa_diversity")

    # Read Silver iNaturalist data
    df = spark.read.parquet(input_path)

    # Defensive check: required columns
    required_cols = {"observed_date", "source_year", "taxon_id"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Gold aggregation
    gold_df = (
        df
        .groupBy("observed_date", "source_year")
        .agg(
            countDistinct("taxon_id").alias("distinct_taxa_count")
        )
    )

    # Write Gold output
    (
        gold_df
        .write
        .mode("overwrite")
        .partitionBy("source_year")
        .parquet(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold taxa diversity aggregation")
    parser.add_argument(
        "--input",
        required=True,
        help="Input Silver iNat S3 path"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output Gold S3 path"
    )

    args = parser.parse_args()

    main(args.input, args.output)
