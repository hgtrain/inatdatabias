"""
schemas.py

Defines Spark StructType schemas used in the Bronze layer.

IMPORTANT:
- These schemas are NOT raw iNaturalist API schemas.
- Fields here include derived or renamed values.
- Do NOT use these schemas for raw JSON ingestion or canonical Bronze storage.
"""

from pyspark.sql.types import(
  StructType, StructField,
  LongType, StringType,
  DoubleType, IntegerType
)

# Schema for Bronze iNaturalist observations after minimal normalization
BRONZE_INAT_OBSERVATIONS_SCHEMA = StructType([
    StructField("observation_id", LongType(), nullable=False),
    StructField("observed_at", StringType(), nullable=True),
    StructField("observed_date", StringType(), nullable=True),
    StructField("latitude", DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),
    StructField("county", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("taxon_id", LongType(), nullable=True),
    StructField("iconic_taxon_name", StringType(), nullable=True),
    StructField("quality_grade", StringType(), nullable=True),
    StructField("source_year", IntegerType(), nullable=True),
])

# Schema for Bronze ParkServe parks (reference / dimension-style data)
BRONZE_PARKSERVE_PARKS_SCHEMA = StructType([
    StructField("park_id", StringType(), nullable=False),
    StructField("park_name", StringType(), nullable=True),
    StructField("county", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("geometry_wkt", StringType(), nullable=True),
    StructField("source", StringType(), nullable=True),
])