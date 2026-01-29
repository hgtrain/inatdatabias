"""
silver_batch_job.py

Batch Spark job that converts Bronze observation data into Silver format.

Purpose:
- Enforce data types and schema consistency
- Deduplicate observations by observation_id
- Support optional date-based backfills

Input (Bronze):
- S3 path provided via --bronze-path

Output (Silver):
- S3 path provided via --silver-path
"""

import argparse
import os
import sys

# Ensure src/ is on PYTHONPATH for spark-submit
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import col, to_date
from spark_jobs.spark_session import create_spark_session


parser = argparse.ArgumentParser(description="Bronze to Silver batch job")

parser.add_argument(
    "--bronze-path",
    required=True,
    help="S3 path to Bronze data"
)

parser.add_argument(
    "--silver-path",
    required=True,
    help="S3 path to write Silver data"
)

parser.add_argument(
    "--start-date",
    required=False,
    help="Optional filter start date (YYYY-MM-DD)"
)

args = parser.parse_args()

spark = create_spark_session("SilverBatchJob")

bronze_df = spark.read.parquet(args.bronze_path)

# Enforce observed_date as date type in Silver
bronze_df = bronze_df.withColumn(
    "observed_date",
    to_date(col("observed_date"))
)

# Optional date filtering (batch / backfill logic)
if args.start_date:
    bronze_df = bronze_df.filter(
        col("observed_date") >= args.start_date
    )

# Silver transformations 
#deduplication - if two rows have same observation id, only one is kept
silver_df = (
    bronze_df
    .dropna(subset=["observation_id"])
    .dropDuplicates(["observation_id"])
)

# Explicit Silver schema
silver_df = silver_df.select(
    "observation_id",
    "observed_date",
    "latitude",
    "longitude",
    "county",
    "state",
    "taxon_id",
    "iconic_taxon_name",
    "quality_grade",
    "source_year"
)

(
    silver_df
    .write
    .mode("overwrite")
    .parquet(args.silver_path)
)
