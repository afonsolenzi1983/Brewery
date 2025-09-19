import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import requests
from datetime import datetime
import sys

# --- Configurations ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
# --- API URL for Brewery data ---
API_BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
# --- Base path for output in MinIO ---
OUTPUT_BASE_PATH = "s3a://landing" # 'landing' will be the bucket name

def get_spark_session():
    """Creates and configures a Spark session to interact with MinIO."""
    return (
        SparkSession.builder.appName("APIToParquet")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Delta Lake configurations are no longer needed for Parquet format
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
    spark = get_spark_session()
    
    # 1. Read data from the API
    brewery_data = fetch_all_brewery_data(API_BASE_URL)
    
    if not brewery_data:
        print("Could not fetch data from the API. Exiting.")
        spark.stop()
        sys.exit(1)

    # 2. Create a Spark DataFrame
    df = spark.createDataFrame(brewery_data)
    print("DataFrame created successfully.")
    df.printSchema()
    
    # 3. Define the output path with the current date
    # Format the current date as YYYYMMDD
    current_date_str = datetime.now().strftime('%Y%m%d')
    # Create the full output path for the directory
    output_filename = f"breweries-{current_date_str}.parquet"
    output_path = f"{OUTPUT_BASE_PATH}/{output_filename}"
    
    # 4. Write data to the Bronze layer in MinIO as a Parquet file
    # The Bronze layer stores raw data
    print(f"Writing raw data to Parquet at: {output_path}")
    
    # Use coalesce(1) to force Spark to write into a single file
    # Note: Spark writes to a directory. Inside this directory will be the single .parquet file.
    df.coalesce(1).write.format("parquet").mode("overwrite").save(output_path)
    
    print(f"Successfully wrote data to the Bronze layer at {output_path}")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
