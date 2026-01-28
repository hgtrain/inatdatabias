# # bronze_parkserve_kafka_stream.py
# # Kafka -> Spark Structured Streaming -> S3 Bronze (parkserve parks)
"""
ARCHIVED — DO NOT USE

WHY THIS WAS ARCHIVED
---------------------
This file represents an EARLY design attempt to stream ParkServe
data through Kafka.

After reviewing the project specification, this approach was
intentionally abandoned.

RATIONALE
---------
- ParkServe is static / reference (dimension-style) data
- It does NOT change frequently
- Streaming it adds unnecessary infrastructure complexity
- Batch ingestion is more appropriate and aligns with spec

CURRENT APPROACH
----------------
ParkServe is ingested via:
- Batch Spark job on EMR
- Written once to Bronze
- Joined downstream with iNat observations

STATUS
------
Archived for reference only.
Not part of the active pipeline.
"""

# import os
# import sys

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# from pyspark.sql.functions import col, from_json
# from common.schemas import BRONZE_PARKSERVE_PARKS_SCHEMA
# from spark_jobs.spark_session import create_spark_session

# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
# if not KAFKA_BOOTSTRAP_SERVERS:
#     raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required, e.g. 172.31.x.x:9092")

# RUN_ID = os.getenv("RUN_ID", "run_001")
# TOPIC = os.getenv("KAFKA_TOPIC", "parkserve_parks")

# BRONZE_PATH = os.getenv(
#     "BRONZE_PATH",
#     "s3://bhj-analytics/bronze/parkserve_parks/"
# )

# CHECKPOINT_PATH = os.getenv(
#     "CHECKPOINT_PATH",
#     f"s3://bhj-analytics/checkpoints/bronze/parkserve_parks/{RUN_ID}/"
# )

# spark = create_spark_session("BronzeParkServeKafkaIngestion")

# kafka_df = (
#     spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
#     .option("subscribe", TOPIC)
#     .option("startingOffsets", "latest")
#     .option("failOnDataLoss", "false")
#     .load()
# )

# bronze_df = (
#     kafka_df
#     .select(from_json(col("value").cast("string"), BRONZE_PARKSERVE_PARKS_SCHEMA).alias("data"))
#     .select("data.*")
#     .filter(col("park_id").isNotNull())
# )

# query = (
#     bronze_df.writeStream
#     .format("parquet")
#     .outputMode("append")
#     .option("path", BRONZE_PATH)
#     .option("checkpointLocation", CHECKPOINT_PATH)
#     .start()
# )

# query.awaitTermination()
