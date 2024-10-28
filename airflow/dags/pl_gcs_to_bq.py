import os
import logging
import pendulum

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.decorators import dag, task

# Load environment variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
DATASET_PATH = "ajaxianazarenka/premier-league"
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
FILENAME = "PremierLeague"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'premier_league')

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)

def create_bq_external_table():
    """ Create external table in BigQuery from Parquet files in GCS """
    try:
        logger.info(f"create_bq_external_table - Create external table in BigQuery")

        return BigQueryCreateExternalTableOperator(
                    task_id="gcs_to_bq_ext_task",
                    table_resource={
                        "tableReference": {
                            "projectId": PROJECT_ID,
                            "datasetId": BIGQUERY_DATASET,
                            "tableId": "pl_external_table",
                        },
                        "externalDataConfiguration": {
                            "sourceFormat": "PARQUET",
                            "sourceUris": [f"gs://{BUCKET}/*.parquet"],
                        },
                    },
                )
        
    except Exception as e:
        logger.error(f"create_bq_external_table - Error: {e}")
        raise

def create_bq_partitioned_table():
    """ Create partitioned table in BigQuery from external table in GCS """
    try:
        logger.info(f"create_bq_partitioned_table - Create partitioned table in BigQuery")
        CREATE_PART_TBL_QUERY = f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.pl_partitioned \
                    PARTITION BY Date AS \
                    SELECT * FROM {BIGQUERY_DATASET}.pl_external_table;"
        
        return BigQueryInsertJobOperator(
            task_id=f"bq_create_partitioned_table",
            configuration={
                "query": {
                    "query": CREATE_PART_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

    except Exception as e:
        logger.error(f"create_bq_partitioned_table - Error: {e}")
        raise

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
    },
    tags=["data_engineering"],
)
def pl_gcs_to_bq():
    task_external_table = create_bq_external_table()
    task_partitioned_table = create_bq_partitioned_table()

    task_external_table >> task_partitioned_table

# Execute the main DAG function
pl_gcs_to_bq()