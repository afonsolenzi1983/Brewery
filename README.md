## Project: Brewery Data Pipeline with Airflow, Spark, and Minio ##

Architecture:




<img width="601" height="449" alt="image" src="https://github.com/user-attachments/assets/e9907c1f-9c1c-4b71-8f80-d4a74bbe29f9" />



This project implements a modern, scalable ETL pipeline to ingest data from the public OpenBreweryDB API. The pipeline is orchestrated by Apache Airflow, processed with Apache Spark, and stored in a MinIO-based data lake using the Delta Lake format. The entire environment is containerized with Docker Compose for portability and ease of deployment.

The pipeline follows the Medallion Architecture to progressively refine and structure the data across Bronze, Silver, and Gold layers, making it suitable for analytics and business intelligence.

Technology Stack

Orchestration: Apache Airflow

Data Processing: Apache Spark

Data Lake Storage: MinIO

Storage Format: Delta Lake

Containerization: Docker Compose

## To execute the solution please:

 -clone the repo 
 
 -docker compose build --no-cache
 
 -docker compose up

 - minio http://localhost:9001/ create a buckets

 - spark master http://localhost:8081/
 
 - airflow http://localhost:8082/ execute the action on the mentioned 'airflow UI - actions.txt'. Variables for minio and Connection for spark.
 
 -once you are done just run docker compose down -v




