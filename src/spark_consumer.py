"""
Docstring for spark_consumer

export PYSPARK_PYTHON=""
export PYSPARK_DRIVER_PYTHON=""

export HADOOP_HOME=""
export SPARK_LOCAL_DIRS=""

"""

import logging
from pyspark.sql import SparkSession

from root import ROOT_PATH

SERVER = "localhost:9092"
OBS_TOPIC_NAME = "observations"

log = logging.getLogger(__name__)
logging.basicConfig(
    filename=f"{ROOT_PATH}/logs/spark_consumer.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s [at %(filename)s:%(funcName)s:%(lineno)s]",
    filemode="w"
)

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("inat_pipeline") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", SERVER) \
    .option("subscribe", OBS_TOPIC_NAME) \
    .load()
log.info("Dataframe loaded")

print(df)

spark.stop()
