# bronze_json_to_parquet_inat.py

import argparse
import os
import sys
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark_jobs.spark_session import create_spark_session


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-json-path", required=True)
    parser.add_argument("--bronze-parquet-path", required=True)
    args = parser.parse_args()

    spark = create_spark_session("BronzeJsonToParquetInat")

    # Read RAW JSON — NO SCHEMA
    raw_df = spark.read.json(args.bronze_json_path)

    # Canonicalize dates explicitly
    bronze_df = (
        raw_df
        .withColumn(
            "event_date",
            F.to_date(
                F.coalesce(
                    F.col("observed_on_details.date"),
                    F.col("observed_on"),
                    F.col("observed_at"),
                    F.col("created_at")
                )
            )
        )
        .withColumn("source_year", F.year("event_date"))
        .withColumnRenamed("id", "observation_id")
    )

    # Minimal Bronze projection
    bronze_df = bronze_df.select(
        "observation_id",
        "event_date",
        "source_year",
        F.col("geojson.coordinates")[1].alias("latitude"),
        F.col("geojson.coordinates")[0].alias("longitude"),
        "place_guess"
    )

    # Write canonical Bronze
    (
        bronze_df
        .write
        .mode("overwrite")
        .parquet(args.bronze_parquet_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()
