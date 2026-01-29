# iNaturalist Stream Analytics Pipeline

A learning-focused data engineering pipeline using AWS, Spark, and medallion architecture.

This repository contains a data pipeline built to analyze biodiversity observation activity using iNaturalist data, with additional park reference data from ParkServe.

The main goal of this project is to compare **observation activity over time (2019 vs 2020)** and to practice building a real-world **Bronze → Silver → Gold** data pipeline using AWS and Spark.

The project is designed to be correct, transparent, and easy to reason about, rather than overly complex.

---

## High-Level Architecture

### Data Sources

- **iNaturalist API**
  - Biodiversity observation events
  - Provides timestamps, taxonomic data, and latitude/longitude
- **ParkServe (Trust for Public Land)**
  - U.S. park reference dataset
  - Provides park identifiers, county names, and park geometries

### Processing

- Apache Spark (PySpark)
- AWS EMR for batch processing

### Storage

- Amazon S3 is used for all Bronze, Silver, and Gold datasets

### Streaming (Validation / Future Use)

- Apache Kafka hosted on EC2
- Included to demonstrate multi-topic ingestion and future validation

Local development was used only for testing.  
All production-style runs are designed for **AWS EMR**.

---

## Data Model Overview

### Observation Grain

Each row represents **one biodiversity observation**.

This grain is preserved across the pipeline.  
Aggregations and analysis are only performed in the Gold layer.

---

## Kafka Topics (Validation Phase)

Kafka is included mainly to demonstrate streaming concepts and multi-topic ingestion.

### `inat_observations`

Represents individual biodiversity observations.

Key fields:

- observation_id
- observed_date
- latitude
- longitude
- taxon_id
- iconic_taxon_name
- quality_grade
- source_year

### `parkserve_parks`

Represents park reference information.

Key fields:

- park_id
- park_name
- county
- geometry (serialized)
- source

Kafka is **not the main execution path** for this project.

---

## Data Layers

## Bronze Layer

### Purpose

- Store raw data
- Preserve original structure
- Avoid early assumptions or transformations

### Characteristics

- Append-only
- No aggregation
- Geometry preserved as-is
- Minimal transformation

### Outputs

- Raw iNaturalist JSON in S3
- Canonical Bronze Parquet created from JSON

An early issue with missing derived fields was identified and fixed by rebuilding the Bronze Parquet layer correctly.

---

## Silver Layer

### Silver iNaturalist (Canonical)

### Purpose

- Clean and standardize observation data
- Prepare data for reuse and aggregation

Transformations include:

- Casting `observed_date` to a date type
- Removing invalid observation IDs
- Deduplicating observations
- Enforcing a consistent schema

**Note on County and State**

- iNaturalist does not reliably provide county or state fields
- This pipeline does not perform reverse geocoding
- As a result, county and state remain null

This avoids introducing incorrect geographic data.

---

### Silver Enriched (iNaturalist + ParkServe)

### Purpose

- Attempt to enrich observations with park reference data
- Preserve park geometries for future use

### Join Strategy

- Left join using normalized county names
- All observation records are preserved

### Result

- No successful matches between observations and parks
- `park_id` values are null

This outcome is expected because:

- Observations are points (lat/long)
- Parks are geographic areas (polygons)
- Matching by county name alone is not reliable

This limitation is documented rather than hidden.

---

## Gold Layer

### State-Year Observation Summary

### Purpose

- Validate the pipeline end-to-end
- Produce a simple analytics-ready table

### Output

Observation counts by year:

| source_year | observation_count |
| ----------- | ----------------- |
| 2019        | 10000             |
| 2020        | 10000             |

Counts shown are representative; actual values depend on API pagination at runtime.

This confirms:

- Data ingestion worked
- Transformations were applied correctly
- Results are consistent across years

---

## Why Spatial Joins Are Not Implemented (Currently working on**\***)

Correctly joining observations to parks requires a **spatial join** using latitude/longitude and park boundaries.

This requires additional tools such as:

- PostGIS
- Apache Sedona

This project:

- Preserves all required spatial data
- Avoids inaccurate joins
- Documents spatial joins as future work

---

## Local Development Notes

- Kafka producers were tested locally
- Spark streaming from Kafka was not fully run locally
- Local Spark does not support Kafka by default
- Spark jobs are written for EMR execution

Local testing focused on correctness rather than scale.

---

## How to Run (Target Environment)

1. Launch an AWS EMR cluster
2. Submit Spark jobs using `spark-submit`
3. Use S3 paths for all inputs and outputs
4. Verify outputs using S3 `_SUCCESS` files and Spark queries

Kafka and Airflow are optional and not required for core execution.

---

## Current Status

- iNaturalist Bronze ingestion complete (2019, 2020)
- ParkServe Bronze ingestion complete
- Bronze Parquet rebuilt and validated
- Silver transformations implemented
- Silver enrichment attempted and documented
- Gold aggregation completed
- Pipeline runs successfully on AWS EMR

---

## Data Scope and Limitations

This project is designed to demonstrate the architecture, correctness, and scalability of a cloud-based data engineering pipeline rather than to produce statistically definitive ecological conclusions.

### Sampling Scope

Historical iNaturalist observations were ingested using a fixed-size sample per year (10,000 observations for 2019 and 2020). Sampling was intentionally used to control cost, runtime, and development complexity while validating ingestion, transformation, and aggregation logic. The pipeline is parameterized and designed to scale to larger or complete historical datasets if required.

### Temporal Coverage

Because the dataset is sampled, temporal coverage does not span all calendar days in each year. Aggregations performed at the daily level reflect the observed dates present in the sampled data rather than full-year coverage. For example, in the sampled dataset, 2020 observations only span a narrow window early in the year, which explains the smaller number of observed days appearing in downstream Gold aggregates.

As a result, values in the Gold tables should be interpreted as illustrative of how biodiversity metrics can be computed and compared across years using the pipeline, rather than as statistically representative measures of full-year biodiversity trends.

### Spatial Considerations

iNaturalist observations are point-based (latitude and longitude), while ParkServe data represents polygonal park boundaries. Attribute-based geographic joins were evaluated and intentionally rejected due to data sparsity and the risk of inaccurate spatial inference.

The pipeline preserves latitude/longitude for observations and geometry data for parks, enabling future spatial analysis using a dedicated spatial engine if required.
