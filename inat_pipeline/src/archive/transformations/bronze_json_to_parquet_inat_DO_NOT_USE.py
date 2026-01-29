"""
bronze_json_to_parquet_inat.py

Bronze transformation job that converts raw iNaturalist JSON
into structured Parquet format.

Purpose:
- Normalize raw API JSON into a tabular Bronze schema
- Derive a canonical observed_date from multiple possible fields
- Extract spatial and taxonomic attributes for downstream processing

Input (Bronze JSON):
- s3://bhj-analytics/bronze/inat_observations/

Output (Bronze Parquet):
- s3://bhj-analytics/bronze/inat_observations_parquet/
"""

import argparse
import os
import sys

from pyspark.sql import functions as F

# Ensure src/ is on PYTHONPATH for spark-submit
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark_jobs.spark_session import create_spark_session


def col_or_null(df, col_name: str):
    """Return a Column if present at top level; otherwise NULL literal."""
    return F.col(col_name) if col_name in df.columns else F.lit(None)


def nested_or_null(df, parent: str, child: str):
    """Return a nested Column if parent exists; otherwise NULL literal."""
    if parent in df.columns:
        return F.col(f"{parent}.{child}")
    return F.lit(None)


def main():
    parser = argparse.ArgumentParser(description="Convert Bronze iNaturalist JSON to Parquet (robust dates)")
    parser.add_argument("--bronze-json-path", required=True, help="S3 path to raw Bronze JSON (year partition)")
    parser.add_argument("--bronze-parquet-path", required=True, help="S3 path to write canonical Bronze Parquet (year partition)")
    args = parser.parse_args()

    spark = create_spark_session("BronzeJsonToParquetInat")

    # Read RAW JSON (schema varies by year; we handle it defensively)
    raw_df = spark.read.json(args.bronze_json_path)

    # Candidate fields (some may not exist depending on year)
    observed_on_details_date = nested_or_null(raw_df, "observed_on_details", "date")  # "YYYY-MM-DD"
    observed_on = col_or_null(raw_df, "observed_on")                                  # sometimes "YYYY-MM-DD"
    time_observed_at = col_or_null(raw_df, "time_observed_at")                        # timestamp string
    created_at = col_or_null(raw_df, "created_at")                                    # timestamp string

    # observed_at: not reliable for all years, but keep column for schema compatibility
    observed_at = col_or_null(raw_df, "observed_at")

    # Build observed_date robustly:
    # - if we already have a date string, to_date handles it
    # - if we have timestamp strings, to_date will extract date
    observed_date_col = F.to_date(
        F.coalesce(
            observed_on_details_date,
            observed_on,
            time_observed_at,
            created_at
        )
    )

    # Lat/Long:
    # Prefer geojson.coordinates [lon, lat]
    geojson = col_or_null(raw_df, "geojson")
    has_geojson = "geojson" in raw_df.columns

    if has_geojson:
        longitude_col = F.col("geojson.coordinates")[0]
        latitude_col = F.col("geojson.coordinates")[1]
    else:
        # Fallback: "location" string like "lat,lon"
        location = col_or_null(raw_df, "location")
        latitude_col = F.split(location, ",").getItem(0).cast("double")
        longitude_col = F.split(location, ",").getItem(1).cast("double")

    # place_guess is useful for county/state parsing
    place_guess = col_or_null(raw_df, "place_guess")

    # Simple county/state parse from place_guess:
    # Many values look like: "Somewhere, County Name, State, USA" (varies)
    # We'll take last 3 tokens for safety: "... , <county>, <state>, <country>"
    place_parts = F.split(place_guess, ",\\s*")
    parts_len = F.size(place_parts)

    county_raw = F.when(parts_len >= 3, place_parts.getItem(parts_len - 3)).otherwise(F.lit(None))
    state_raw = F.when(parts_len >= 2, place_parts.getItem(parts_len - 2)).otherwise(F.lit(None))

    # Normalize county by removing trailing "County" if present
    county_col = F.regexp_replace(county_raw, "\\s+County$", "")

    # IDs and other fields
    observation_id_col = F.col("id").cast("long") if "id" in raw_df.columns else col_or_null(raw_df, "observation_id").cast("long")
    taxon_id_col = col_or_null(raw_df, "taxon.id").cast("long") if "taxon" in raw_df.columns else col_or_null(raw_df, "taxon_id").cast("long")
    iconic_taxon_name_col = col_or_null(raw_df, "taxon.iconic_taxon_name") if "taxon" in raw_df.columns else col_or_null(raw_df, "iconic_taxon_name")
    quality_grade_col = col_or_null(raw_df, "quality_grade")

    bronze_df = (
        raw_df
        .withColumn("observation_id", observation_id_col)
        .withColumn("observed_at", observed_at)  # may be null for some years
        .withColumn("observed_date", observed_date_col.cast("string"))  # keep as string to match current parquet schema
        .withColumn("source_year", F.year(F.to_date(F.col("observed_date"))))
        .withColumn("year", F.col("source_year"))
        .withColumn("latitude", latitude_col.cast("double"))
        .withColumn("longitude", longitude_col.cast("double"))
        .withColumn("county", county_col)
        .withColumn("state", state_raw)
        .withColumn("taxon_id", taxon_id_col)
        .withColumn("iconic_taxon_name", iconic_taxon_name_col)
        .withColumn("quality_grade", quality_grade_col)
        .select(
            "observation_id",
            "observed_at",
            "observed_date",
            "latitude",
            "longitude",
            "county",
            "state",
            "taxon_id",
            "iconic_taxon_name",
            "quality_grade",
            "source_year",
            "year",
        )
    )

    (
        bronze_df.write
        .mode("overwrite")
        .parquet(args.bronze_parquet_path)
    )

    print(f"[DONE] Wrote Bronze Parquet to: {args.bronze_parquet_path}")

    spark.stop()


if __name__ == "__main__":
    main()
