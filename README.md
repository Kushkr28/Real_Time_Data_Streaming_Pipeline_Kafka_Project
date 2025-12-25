🚀 Hands-on Project: Real-Time Data Streaming Pipeline (Apache Kafka | Python | MySQL)

I built an end-to-end real-time data streaming pipeline project using Confluent Kafka, MySQL, Avro, and Python.

📌 Project Summary
The goal was to simulate a real-world e-commerce scenario and stream incremental product updates from a MySQL database to downstream systems for real-time analytics.
💡 What I implemented:

•	✅ Kafka Producer (Python)
o	Incremental data fetch from MySQL using last_updated timestamp
o	Avro serialization for efficient schema-based messaging
o	Published data to a 10-partition Kafka topic using product_id as key

•	✅ Kafka Consumer Group (5 consumers)
o	Avro deserialization
o	Real-time data transformations (business rules, category normalization, discounts)
o	Appended transformed records into separate JSON files

•	✅ Data Engineering Concepts Applied
o	Consumer groups & partitioning
o	Schema design with Avro
o	Offset handling & incremental ingestion
o	Fault-tolerant, scalable streaming design

📦 Tech Stack
•	Apache Kafka (Confluent)
•	Python
•	MySQL
•	Avro
•	VS Code

📌 This project strengthened my understanding of real-time data pipelines, stream processing, and distributed systems.
