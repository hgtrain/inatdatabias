# bronze_kafka_stream.py
# Kafka -> Spark Structured Streaming -> S3 Bronze (inat observations)
# Intended for incremental ingestion (2026+), so default offsets = latest.

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import col, from_json
from common.schemas import BRONZE_INAT_OBSERVATIONS_SCHEMA
from spark_jobs.spark_session import create_spark_session

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
if not KAFKA_BOOTSTRAP_SERVERS:
    raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required, e.g. 172.31.x.x:9092")

RUN_ID = os.getenv("RUN_ID", "run_001")

TOPIC = os.getenv("KAFKA_TOPIC", "inat_observations")

BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "s3://bhj-analytics/bronze_stream/inat_observations/"
)

CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    f"s3://bhj-analytics/checkpoints/bronze/inat_observations/{RUN_ID}/"
)

spark = create_spark_session("BronzeKafkaIngestion")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")      # incremental mode
    .option("failOnDataLoss", "false")
    .option("kafkaConsumer.pollTimeoutMs", "60000")
    .load()
)

parsed_df = (
    kafka_df
    .select(from_json(col("value").cast("string"), BRONZE_INAT_OBSERVATIONS_SCHEMA).alias("data"))
    .select("data.*")
)

# Drop malformed JSON (data = null) and invalid rows
bronze_df = (
    parsed_df
    .filter(col("observation_id").isNotNull())
)

query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()
