#seed_bronze_inat.py

"""
PURPOSE
-------
Local development SMOKE TEST for Bronze iNat ingestion logic.

WHAT THIS IS
------------
- Local-only version of the EMR smoke test
- Used to validate API parsing and schema mapping
- Does NOT represent production-scale ingestion

WHAT THIS IS NOT
----------------
- NOT used in AWS
- NOT part of the deployed pipeline
- NOT historical ingestion

WHY THIS EXISTS
---------------
Used during early development to:
- Validate API response structure
- Build schema before deploying to EMR
- Debug logic without incurring AWS cost

STATUS
------
Development-only artifact.
May be deleted once EMR ingestion is stable.
"""



import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType,
    DoubleType, IntegerType
)


# Spark session (local)
spark = (
    SparkSession.builder
    .appName("SeedBronzeInat")
    .getOrCreate()
)


# iNaturalist API config
API_URL = "https://api.inaturalist.org/v1/observations"

params = {
    "place_id": 51,          # New Jersey
    "per_page": 50,          # small slice
    "page": 1,
    "order": "desc",
    "order_by": "observed_on"
}

response = requests.get(API_URL, params=params)
response.raise_for_status()
results = response.json()["results"]

# Transform to records
records = []

for obs in results:
    records.append({
        "observation_id": obs["id"],
        "observed_at": obs.get("observed_on"),
        "observed_date": obs.get("observed_on"),
        "latitude": obs["geojson"]["coordinates"][1] if obs.get("geojson") else None,
        "longitude": obs["geojson"]["coordinates"][0] if obs.get("geojson") else None,
        "county": obs.get("place_guess"),
        "state": "New Jersey",
        "taxon_id": obs["taxon"]["id"] if obs.get("taxon") else None,
        "iconic_taxon_name": obs["taxon"]["iconic_taxon_name"] if obs.get("taxon") else None,
        "quality_grade": obs.get("quality_grade"),
        "source_year": int(obs["observed_on"][:4]) if obs.get("observed_on") else None
    })


# Schema (Bronze)
schema = StructType([
    StructField("observation_id", LongType(), False),
    StructField("observed_at", StringType(), True),
    StructField("observed_date", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("county", StringType(), True),
    StructField("state", StringType(), True),
    StructField("taxon_id", LongType(), True),
    StructField("iconic_taxon_name", StringType(), True),
    StructField("quality_grade", StringType(), True),
    StructField("source_year", IntegerType(), True)
])

df = spark.createDataFrame(records, schema=schema)


# Write Bronze Parquet to S3

(
    df.write
    .mode("overwrite")
    .parquet("s3://bhj-analytics/bronze/inat_observations/")
)

print("Bronze seed data written successfully.")
