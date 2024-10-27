import os
import logging
import pendulum
import kagglehub

import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
from google.cloud import storage
from airflow.decorators import dag, task

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
        logger.info(f"download_dataset - Starting download of dataset: {dataset_path}")
        csv_path = kagglehub.dataset_download(dataset_path, path=f"{FILENAME}.csv")
        logger.info(f"download_dataset - Downloaded dataset to: {csv_path}")
        return csv_path
    except Exception as e:
        logger.error(f"download_dataset - Error downloading dataset: {e}")
        raise

@task()
def prepare_dataset(src_file):
    """ Prepare dataset for loading into BigQuery """
    try:
        logger.info("prepare_dataset - Starting preparation of dataset.")
        
        df = pd.read_csv(src_file)
        
        # Column with dots does not work in BigQuery
        df.rename(columns={
            "B365Over2.5Goals": "B365_Over_2_5_Goals",
            "B365Under2.5Goals": "B365_Under_2_5_Goals",
            "MarketMaxOver2.5Goals": "MarketMaxOver2_5Goals",
            "MarketMaxUnder2.5Goals": "MarketMaxUnder2_5Goals",
            "MarketAvgOver2.5Goals": "MarketAvgOver2_5Goals",
            "MarketAvgUnder2.5Goals": "MarketAvgUnder2_5Goals"
        }, inplace=True)
        
        logger.info("prepare_dataset - Removed dots from columns names.")
        
        # Column Time is making problems when I convert to Parquet
        # So I will convert it to string
        df['Time'] = df['Time'].astype(str)
        
        logger.info("prepare_dataset - Converted column Time to string.")
        
        csv_file = f"{PATH_TO_LOCAL_HOME}/{FILENAME}.csv"

        df.to_csv(csv_file, index=False)
        
        logger.info("prepare_dataset - Successfully prepared dataset.")
        return csv_file

    except Exception as e:
        logger.error(f"prepare_dataset - Error removing dots: {e}")
        raise

@task()
def format_to_parquet(csv_file):
    """Converts a CSV file to Parquet format."""
    try:
        if not csv_file.endswith('.csv'):
            logger.error("format_to_parquet - Can only accept source files in CSV format.")
            return
        
        table = pv.read_csv(csv_file)

        parquet_file = f"{PATH_TO_LOCAL_HOME}/{FILENAME}.parquet"
        logger.info(f"format_to_parquet - {parquet_file}")

        pq.write_table(table, parquet_file)
        logger.info("format_to_parquet - Successfully converted CSV to Parquet.")
        return parquet_file
    except Exception as e:
        logger.error(f"format_to_parquet - Error converting to Parquet: {e}")
        raise


@task()
def upload_to_gcs(bucket_name, source_file_name, object_name):
    """Uploads a file to Google Cloud Storage."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(object_name)

        blob.upload_from_filename(source_file_name)
        logger.info(f"upload_to_gcs - File uploaded to {object_name}.")
    except Exception as e:
        logger.error(f"upload_to_gcs - Error uploading to GCS: {e}")
        raise

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["PremierLeague"],
)
def pl_web_to_gcs():
    csv_path = download_dataset(DATASET_PATH)
    csv_prepared_path = prepare_dataset(csv_path)
    parquet_path = format_to_parquet(csv_prepared_path)
    upload_to_gcs(BUCKET, parquet_path, f"{FILENAME}.parquet")

# Execute the main DAG function
pl_web_to_gcs()
