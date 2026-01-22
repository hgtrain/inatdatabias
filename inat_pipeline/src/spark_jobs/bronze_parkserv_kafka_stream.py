#bronze_parkserve_kafka_stream.py

#Consumes ParkServe park reference records from Kafka using Spark Structured Streaming and writes them to the Bronze layer in S3.
# Kafka -> Spark ingestion only
# No transformations


import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import col, from_json
from common.schemas import BRONZE_PARKSERVE_PARKS_SCHEMA
from spark_jobs.spark_session import create_spark_session

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

spark = create_spark_session("BronzeParkServeKafkaIngestion")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", "parkserve_parks")
    .load()
)

bronze_df = (
    kafka_df
    .select(from_json(col("value").cast("string"), BRONZE_PARKSERVE_PARKS_SCHEMA).alias("data"))
    .select("data.*")
)

query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "s3://bhj-analytics/bronze/parkserve_parks/")
    .option("checkpointLocation", "s3://bhj-analytics/checkpoints/bronze/parkserve_parks/")
    .start()
)

query.awaitTermination()
