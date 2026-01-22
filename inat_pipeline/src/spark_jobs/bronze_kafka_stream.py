# bronze_kafka_stream.py
# Consumes observation events from Kafka using Spark Structured Streaming, applies an explicit Bronze schema, and materializes a structured streaming DataFrame for downstream processing.
# Kafka -> Spark ingestion only
# No Silver or Gold transformations

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import col, from_json
from common.schemas import BRONZE_INAT_OBSERVATIONS_SCHEMA
from spark_jobs.spark_session import create_spark_session

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

# Spark Session
spark = create_spark_session("BronzeKafkaIngestion")

# Read from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", "inat_observations")
    .load()
)

#Parse JSON payload
bronze_df = (
    kafka_df
    .select(from_json(col("value").cast("string"), BRONZE_INAT_OBSERVATIONS_SCHEMA).alias("data"))
    .select("data.*")
)

#Output (temporarily: console)
query = (
  bronze_df.writeStream 
  .format("parquet") 
  .outputMode("append") 
  .option("path", 
          "s3://bhj-analytics/bronze/inat_observations/"
          )
  .option("checkpointLocation", 
          "s3://bhj-analytics/checkpoints/bronze/inat_observations/"
          )
  .start()
)


query.awaitTermination()