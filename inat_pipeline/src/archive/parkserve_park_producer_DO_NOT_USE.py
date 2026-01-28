# """
# ParkServe Park Reference Producer

# One-time producer for park reference (dimension) data.
# This is NOT a streaming producer.

"""
ARCHIVED — DO NOT USE

WHY THIS WAS CREATED
--------------------
Initial experiment to model ParkServe data as a Kafka producer.

WHY THIS WAS ABANDONED
----------------------
- ParkServe data is not an event stream
- Data represents park boundaries and metadata
- Streaming does not provide additional value
- Batch ingestion is simpler and more correct

FINAL DESIGN
------------
ParkServe data is treated as:
- Static reference data
- Ingested once via EMR batch job
- Stored in Bronze as polygon-based geometry

STATUS
------
Archived for documentation purposes only.
"""


# import json
# import os
# from kafka import KafkaProducer

# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
# KAFKA_TOPIC = "parkserve_parks"

# if not KAFKA_BOOTSTRAP_SERVERS:
#     raise RuntimeError(
#         "KAFKA_BOOTSTRAP_SERVERS is required, e.g. 172.31.x.x:9092"
#     )

# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#     key_serializer=lambda k: k.encode("utf-8"),
#     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
# )

# # Temporary reference data (replace later with real ParkServe export)
# PARKS = [
#     {
#         "park_id": "NJ-0001",
#         "park_name": "Branch Brook Park",
#         "county": "Essex",
#         "state": "New Jersey",
#         "geometry_wkt": None,
#         "source": "ParkServe"
#     },
#     {
#         "park_id": "NJ-0002",
#         "park_name": "Liberty State Park",
#         "county": "Hudson",
#         "state": "New Jersey",
#         "geometry_wkt": None,
#         "source": "ParkServe"
#     },
#     {
#         "park_id": "NJ-0003",
#         "park_name": "Washington Rock State Park",
#         "county": "Somerset",
#         "state": "New Jersey",
#         "geometry_wkt": None,
#         "source": "ParkServe"
#     },
# ]

# if __name__ == "__main__":
#     print("Publishing ParkServe park reference data...")

#     for park in PARKS:
#         producer.send(
#             KAFKA_TOPIC,
#             key=park["park_id"],
#             value=park
#         )
#         print(f"Published park_id={park['park_id']}")

#     producer.flush()
#     producer.close()
#     print("ParkServe producer finished.")
