Project: Brewery Data Pipeline with Airflow, Spark, and Delta Lake

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

Data Pipeline Workflow
The pipeline executes as a single, cohesive workflow orchestrated by an Airflow DAG.

Scheduled Trigger: An Airflow DAG is configured to run on a daily schedule, initiating the data ingestion process.

Extraction and Load (Bronze Layer): The first task in the DAG triggers a Spark job that connects to the OpenBreweryDB API. The raw JSON data is fetched and loaded directly into the bronze layer in MinIO. This data is saved as a Delta table, providing a versioned, immutable history of the source data.

Cleansing and Transformation (Silver Layer): A second Spark job is triggered, which reads the raw data from the Bronze table. This job performs critical transformations:

Flattens the nested JSON structure.

Enforces a strict schema with correct data types (e.g., converting latitude/longitude to doubles).

Cleans the data by handling null values and standardizing text fields.

The resulting clean, structured dataset is written to the silver layer as a new Delta table.

Aggregation and Analytics (Gold Layer): The final Spark job reads from the cleansed Silver table to create aggregated, business-ready views. For example, it calculates the total number of breweries per state. This aggregated data is saved to the gold layer, serving as the source for downstream analytics, dashboards, or BI tools.

Data Model and Layers
The data is organized in MinIO according to the Medallion Architecture.

Bronze Layer (/bronze/breweries):

Purpose: Raw data landing zone and historical archive.

Format: Unmodified JSON structure from the API, stored as a Delta table.

Schema: Flexible, reflects the source schema.

Silver Layer (/silver/breweries_cleaned):

Purpose: Cleansed, filtered, and structured data ready for querying.

Format: A tabular Delta table with one row per brewery.

Schema: Enforced schema with columns like id, name, brewery_type, city, state, country, latitude, and longitude.

Gold Layer (/gold/breweries_by_state):

Purpose: Business-level aggregates and features for analytics.

Format: An aggregated Delta table.

Schema: Contains columns like state, country, and brewery_count.





