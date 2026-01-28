#seed_bronze_inat_emr.py
#Prove that EMR can fetch iNaturalist data and write Bronze parquet correctly to S3. 1.28.26

"""
PURPOSE
-------
One-time SMOKE TEST to validate that:
- EMR can reach the iNaturalist API
- EMR can construct the Bronze iNat schema
- EMR can successfully write Parquet to S3 (Bronze layer)

WHAT THIS IS
------------
- A minimal, controlled test job
- Pulls a SMALL SAMPLE (latest observations only)
- Overwrites the Bronze path intentionally

WHAT THIS IS NOT
----------------
- NOT the full historical ingestion
- NOT paginated
- NOT year-parameterized (2019/2020 handled elsewhere)
- NOT part of the final scheduled pipeline

WHY THIS EXISTS
---------------
Before building large batch or streaming jobs, we needed to prove:
1) EMR networking and IAM were correct
2) Spark → S3 writes worked end-to-end
3) Bronze schema matched project spec

STATUS
------
SAFE TO DELETE after:
- Full batch ingestion for 2019/2020 is implemented and verified
"""

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType,
    DoubleType, IntegerType
)

spark = (
    SparkSession.builder
    .appName("SeedBronzeInatEMR")
    .getOrCreate()
)

API_URL = "https://api.inaturalist.org/v1/observations"

params = {
    "place_id": 51,        # New Jersey
    "per_page": 100,
    "page": 1,
    "order": "desc",
    "order_by": "observed_on"
}

response = requests.get(API_URL, params=params, timeout=30)
response.raise_for_status()
results = response.json()["results"]

records = []

for obs in results:
    observed_on = obs.get("observed_on")
    taxon = obs.get("taxon") or {}
    coords = obs.get("geojson", {}).get("coordinates", [None, None])

    records.append({
        "observation_id": obs.get("id"),
        "observed_at": observed_on,
        "observed_date": observed_on,
        "latitude": coords[1],
        "longitude": coords[0],
        "county": obs.get("place_guess"),
        "state": "New Jersey",
        "taxon_id": taxon.get("id"),
        "iconic_taxon_name": taxon.get("iconic_taxon_name"),
        "quality_grade": obs.get("quality_grade"),
        "source_year": int(observed_on[:4]) if observed_on else None
    })




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

df = spark.createDataFrame(records, schema)

(
    df.write
    .mode("overwrite")
    .parquet("s3://bhj-analytics/bronze/inat_observations/")
)

spark.stop()
