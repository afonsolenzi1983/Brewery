import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import requests
from datetime import datetime
import sys

# --- Configurações ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
DELTA_TABLE_PATH = "s3a://bronze/users" # 'bronze' será o nome do bucket
API_URL = "https://jsonplaceholder.typicode.com/users"

def get_spark_session():
    """Cria e configura uma sessão Spark para interagir com MinIO."""
    return (
        SparkSession.builder.appName("APIToDelta")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def fetch_all_brewery_data(base_url):
    """
    Fetches all data from a paginated API.

    Args:
        base_url (str): The base URL of the API endpoint.

    Returns:
        list: A list of dictionaries containing all records, or None if an error occurs.
    """
    all_breweries = []
    page = 1
    per_page = 200  # As specified by the API documentation
    print("Starting data fetch from API...")

    while True:
        try:
            api_url = f"{base_url}?page={page}&per_page={per_page}"
            response = requests.get(api_url)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            
            data = response.json()
            
            if not data:
                print("No more data found. Fetch complete.")
                break
            
            all_breweries.extend(data)
            print(f"Fetched page {page}, total records so far: {len(all_breweries)}")
            page += 1
            
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            return None
        except Exception as err:
            print(f"An error occurred: {err}")
            return None
            
    return all_breweries

def main():
    """
    Main function to orchestrate the data ingestion pipeline.
    """
    spark = create_spark_session()

    base_api_url = "https://api.openbrewerydb.org/v1/breweries"
    output_path = "s3a://bronze/breweries" # Path in MinIO for the bronze layer
    
    # 1. Read data from the API
    brewery_data = fetch_all_brewery_data(base_api_url)
    
    if not brewery_data:
        print("Could not fetch data from the API. Exiting.")
        spark.stop()
        sys.exit(1)

    # 2. Create a Spark DataFrame
    df = spark.createDataFrame(brewery_data)
    print("DataFrame created successfully.")
    df.printSchema()
    
    # 3. Write data to the Bronze layer in MinIO as a Delta table
    # The Bronze layer stores raw data, so we perform minimal transformations.
    # Strict validation will happen in the next stage (Bronze -> Silver).
    print(f"Writing raw data to Delta table at: {output_path}")
    
    df.write.format("delta").mode("overwrite").save(output_path)
    
    print("Successfully wrote data to the Bronze layer.")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
