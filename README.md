# bhj-dataeng


## High Level Architecture
* **Kafka (EC2)**: Ingests streaming data through multiple topics
* **Spark (EMR)**: Processes streaming and batch data
* **S3**: Stores Bronze and Silver data layers
* **Local (WSL / Laptop)**: Development and testing only

Kafka and Spark are separate roles. Kafka runs on EC2. Spark runs on EMR. Local Spark is not intended to fully mirror EMR.

---

## Kafka Topics

Two Kafka topics are used to satisfy the multi topic requirement and separate fact and dimension data.

### inat_observations

Fact stream representing individual biodiversity observation events.

Fields include:

* observation_id
* observed_at
* observed_date
* latitude
* longitude
* county
* state
* taxon_id
* iconic_taxon_name
* quality_grade
* source_year

### parkserve_parks

Dimension stream representing park reference data.

Fields include:

* park_id
* park_name
* county
* state
* geometry_wkt
* source

---

## Data Layers

### Bronze

Raw structured data ingested from Kafka or batch API pulls. Minimal transformation. Stored as Parquet in S3.

Sources:

* Kafka streaming from inat_observations
* Kafka streaming from parkserve_parks
* One time batch seed job for iNaturalist data

### Silver

Cleaned and reusable data layer.

Transformations include:

* Casting observed_date to a proper date type
* Removing null observation IDs
* Deduplicating by observation_id
* Optional date based filtering for backfills

---

## Local Development Notes

* Kafka producers were tested locally and verified using Kafka console consumers
* Spark Structured Streaming from Kafka was not fully run locally
* Local Spark does not include Kafka support by default
* Spark jobs are written to be executed on EMR, where Kafka support is available

Local validation focused on:

* Producer correctness
* Schema correctness
* Kafka topic wiring

---

## How to Run (Target Environment)

### Kafka on EC2

1. Launch EC2 instance
2. Install Kafka
3. Create topics:

   * inat_observations
   * parkserve_parks
4. Run Kafka broker
5. Run producers pointing to the EC2 Kafka broker

### Spark on EMR

1. Launch EMR cluster
2. Submit Spark jobs:

   * bronze_kafka_stream.py
   * bronze_parkserve_kafka_stream.py
   * silver_batch_job.py
3. Configure Spark jobs to point to EC2 Kafka bootstrap server
4. Verify Parquet output in S3

---

## Current Status

* Kafka producers implemented and tested locally
* Two Kafka topics implemented
* Bronze ingestion logic completed
* Silver batch transformation completed
* Spark jobs written for EMR execution

---

## Next Steps

* Deploy Kafka to EC2
* Run Spark jobs on EMR
* Decide on visualization approach
* Add Snowflake integration for curated data

---

## Notes

This project intentionally separates local development from cloud execution. Code is written to be deployable on AWS infrastructure even if all components are not run locally.
