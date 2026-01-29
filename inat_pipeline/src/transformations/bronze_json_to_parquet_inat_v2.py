import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


import argparse
from pyspark.sql import functions as F
from spark_jobs.spark_session import create_spark_session


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-json-path", required=True)
    parser.add_argument("--bronze-parquet-path", required=True)
    args = parser.parse_args()

    spark = create_spark_session("BronzeInatJsonToParquetV2")

    raw_df = spark.read.json(args.bronze_json_path)

    # ---- CANONICAL DATE (FIXED) ----
    observed_date = F.coalesce(
        F.to_date(F.col("observed_on_details.date")),
        F.to_date(F.col("observed_on")),
        F.to_date(F.to_timestamp(F.col("time_observed_at"))),
        F.to_date(F.to_timestamp(F.col("created_at")))
    )

    bronze_df = (
        raw_df
        .withColumn("observation_id", F.col("id").cast("long"))
        .withColumn("observed_date", observed_date)
        .withColumn("source_year", F.year(observed_date))
        .withColumn("year", F.year(observed_date))
        .withColumn(
            "longitude",
            F.when(F.col("geojson").isNotNull(),
                   F.col("geojson.coordinates")[0])
             .otherwise(F.split(F.col("location"), ",").getItem(1).cast("double"))
        )
        .withColumn(
            "latitude",
            F.when(F.col("geojson").isNotNull(),
                   F.col("geojson.coordinates")[1])
             .otherwise(F.split(F.col("location"), ",").getItem(0).cast("double"))
        )
        .withColumn(
            "county",
            F.regexp_replace(
                F.split(F.col("place_guess"), ",\\s*").getItem(-3),
                "\\s+County$", ""
            )
        )
        .withColumn(
            "state",
            F.split(F.col("place_guess"), ",\\s*").getItem(-2)
        )
        .withColumn("taxon_id", F.col("taxon.id").cast("long"))
        .withColumn("iconic_taxon_name", F.col("taxon.iconic_taxon_name"))
        .withColumn("quality_grade", F.col("quality_grade"))
        .select(
            "observation_id",
            "observed_date",
            "latitude",
            "longitude",
            "county",
            "state",
            "taxon_id",
            "iconic_taxon_name",
            "quality_grade",
            "source_year",
            "year"
        )
    )

    bronze_df.write.mode("overwrite").parquet(args.bronze_parquet_path)

    spark.stop()

if __name__ == "__main__":
    main()
