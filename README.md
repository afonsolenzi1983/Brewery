##Project: Brewery Data Pipeline with Airflow, Spark, and Delta Lake ##

Architecture:




![Architecture](https://github.com/afonsolenzi1983/Brewery/blob/main/4e4d662f-b045-4256-b85c-c7e9b4cbc8bb.jpg?raw=true)))


This project implements a modern, scalable ETL pipeline to ingest data from the public OpenBreweryDB API. The pipeline is orchestrated by Apache Airflow, processed with Apache Spark, and stored in a MinIO-based data lake using the Delta Lake format. The entire environment is containerized with Docker Compose for portability and ease of deployment.

The pipeline follows the Medallion Architecture to progressively refine and structure the data across Bronze, Silver, and Gold layers, making it suitable for analytics and business intelligence.

Technology Stack

Orchestration: Apache Airflow

Data Processing: Apache Spark

Data Lake Storage: MinIO

Storage Format: Delta Lake

Containerization: Docker Compose




