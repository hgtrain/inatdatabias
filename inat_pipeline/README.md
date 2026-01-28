# iNaturalist Stream Analytics Pipeline

This repository contains the core data pipeline for a cloud-based stream analytics platform analyzing biodiversity observation patterns in New Jersey.

The pipeline is designed using a Bronze–Silver–Gold architecture and supports historical batch processing with optional streaming validation.

---

## High-Level Architecture

- **Data Sources**
  - iNaturalist API (biodiversity observations)
  - ParkServe dataset (park spatial reference data)

- **Processing**
  - Apache Spark (PySpark) running on AWS EMR

- **Storage**
  - Amazon S3 for all Bronze, Silver, and Gold datasets

- **Streaming (Planned / Validation Phase)**
  - Apache Kafka hosted on EC2

Local development (WSL / laptop) is used for testing and validation only. The target execution environment is AWS EMR.

---

## Data Model Overview

### Observation Grain

Each record represents **one biodiversity observation event**. This is the fundamental analytical unit used throughout the pipeline.

Spatial aggregation and analytical grouping are intentionally deferred to later layers.

---

## Kafka Topics (Planned / Partial)

Kafka is used to satisfy the multi-topic streaming requirement and to support future validation of live data patterns.

### `inat_observations` (Fact Stream)

Represents individual biodiversity observation events.

Fields:

- observation_id
- observed_at
- observed_date
- latitude
- longitude
- county
- state
- taxon_id
- iconic_taxon_name
- quality_grade
- source_year

### `parkserve_parks` (Dimension Stream)

Represents park reference and spatial metadata.

Fields:

- park_id
- park_name
- county
- state
- geometry (serialized geometry string)
- source

---

## Data Layers

### Bronze Layer

Raw, minimally transformed data stored in Amazon S3.

Sources:

- iNaturalist historical batch ingestion (2019, 2020)
- ParkServe reference dataset (batch ingestion)
- Optional Kafka-based ingestion for live validation

Characteristics:

- Append-only
- No aggregation
- Geometry preserved in raw serialized form

---

### Silver Layer

Cleaned, structured, and reusable datasets derived from Bronze.

Transformations include:

- Casting observed_date to proper date type
- Removing null or invalid observation IDs
- Deduplicating by observation_id
- Normalizing schema for downstream analytics

Silver datasets are designed to be reusable across batch and streaming workflows.

---

### Gold Layer (Planned)

Analytics-ready, denormalized datasets derived from Silver.

Intended metrics include:

- Observation counts by county and date
- Distinct taxa counts
- Year-over-year comparisons (2019 vs 2020)
- Aggregations supporting BI and visualization tools

---

## Local Development Notes

- Kafka producers were tested locally
- Spark Structured Streaming from Kafka is not fully executed locally
- Local Spark does not include Kafka support by default
- Spark jobs are written to be submitted directly to AWS EMR

Local testing focuses on:

- Schema correctness
- Producer logic
- Pipeline parameterization

---

## How to Run (Target Environment)

### Spark on EMR

- Launch EMR cluster
- Submit Spark jobs using `spark-submit`
- Use S3 paths for all inputs and outputs
- Verify Bronze and Silver Parquet outputs in S3

Kafka execution and Airflow orchestration are intended for later stages and may be partially demonstrated.

---

## Current Status

- iNaturalist historical Bronze ingestion completed (2019, 2020)
- ParkServe Bronze ingestion completed
- Silver batch transformation implemented for iNaturalist
- Spark jobs written and EMR-compatible
- Kafka producers implemented (used for validation phase)

---

## Next Steps

- Implement Silver transformation for ParkServe data
- Join iNaturalist and ParkServe data in Silver
- Build Gold aggregation tables
- Add optional Kafka-based streaming validation
- Integrate orchestration using Apache Airflow
