import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_date,
    year,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
    TimestampType
)

def create_spark():
    return (
        SparkSession.builder
        .appName("bronze_kafka_inat_stream")
        .getOrCreate()
    )

def main(args):
    spark = create_spark()

    # Minimal schema for streaming parse
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("observed_on", StringType(), True),
        StructField("quality_grade", StringType(), True),
        StructField("taxon", StructType([
            StructField("id", LongType(), True),
            StructField("iconic_taxon_name", StringType(), True)
        ]), True),
        StructField("geojson", StructType([
            StructField("coordinates", 
                StructType([
                    StructField("_1", DoubleType(), True),
                    StructField("_2", DoubleType(), True)
                ]), True)
        ]), True)
    ])

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select(
            col("data.id").alias("observation_id"),
            to_date(col("data.observed_on")).alias("observed_date"),
            col("data.geojson.coordinates._1").alias("longitude"),
            col("data.geojson.coordinates._2").alias("latitude"),
            col("data.taxon.id").alias("taxon_id"),
            col("data.taxon.iconic_taxon_name").alias("iconic_taxon_name"),
            col("data.quality_grade"),
            year(to_date(col("data.observed_on"))).alias("source_year"),
            current_timestamp().alias("ingest_ts")
        )
        .where(col("observation_id").isNotNull())
    )

    (
        parsed.writeStream
        .format("parquet")
        .option("path", args.output)
        .option("checkpointLocation", args.checkpoint)
        .partitionBy("source_year")
        .outputMode("append")
        .start()
        .awaitTermination()
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--checkpoint", required=True)

    args = parser.parse_args()
    main(args)
