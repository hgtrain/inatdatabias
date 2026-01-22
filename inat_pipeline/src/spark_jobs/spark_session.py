#spark_session.py
#Centralized SparkSession factory for the project. Used by all Spark jobs (streaming and batch).
# On EMR, Spark automatically connects to the cluster manager so we don't set .master("local[*]") here.

from pyspark.sql import SparkSession

# Create and return a SparkSession
def create_spark_session(app_name: str) -> SparkSession:

    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
