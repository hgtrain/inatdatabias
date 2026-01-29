"""
spark_session.py

Provides a centralized SparkSession factory for the project.

All Spark jobs (batch and streaming) should create their session
using this helper to ensure consistent configuration.
"""

from pyspark.sql import SparkSession

def create_spark_session(app_name: str) -> SparkSession:
    """
    Creates and returns a SparkSession with the given application name.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
