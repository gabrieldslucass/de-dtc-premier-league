import os
import logging
import pendulum
import kagglehub

import pyarrow.csv as pv
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from google.cloud import storage
from airflow.decorators import dag, task

# Initialize Spark session
spark = SparkSession.builder.appName("ReadDataset").getOrCreate()

# Load environment variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
DATASET_PATH = "ajaxianazarenka/premier-league"
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
FILENAME = "PremierLeague"

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)

@task()
def download_dataset(dataset_path):
    """Downloads a dataset from kagglehub and returns the path to the downloaded dataset."""
    try:
        logger.info(f"Starting download of dataset: {dataset_path}")
        csv_path = kagglehub.dataset_download(dataset_path, path=f"{FILENAME}.csv")
        logger.info(f"Downloaded dataset to: {csv_path}")
        return csv_path
    except Exception as e:
        logger.error(f"Error downloading dataset: {e}")
        raise

@task()
def format_to_parquet(src_file):
    """Converts a CSV file to Parquet format."""
    try:
        if not src_file.endswith('.csv'):
            logger.error("Can only accept source files in CSV format.")
            return
        
        table = pv.read_csv(src_file)
        parquet_file = src_file.replace('.csv', '.parquet')
        pq.write_table(table, parquet_file)
        logger.info("Successfully converted CSV to Parquet.")
        return parquet_file
    except Exception as e:
        logger.error(f"Error converting to Parquet: {e}")
        raise

@task()
def upload_to_gcs(bucket_name, source_file_name, object_name):
    """Uploads a file to Google Cloud Storage."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(object_name)

        blob.upload_from_filename(source_file_name)
        logger.info(f"File uploaded to {object_name}.")
    except Exception as e:
        logger.error(f"Error uploading to GCS: {e}")
        raise

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
    }
)
def main():
    csv_path = download_dataset(DATASET_PATH)
    parquet_path = format_to_parquet(csv_path)
    upload_to_gcs(BUCKET, parquet_path, f"{FILENAME}.parquet")

# Execute the main DAG function
main()
