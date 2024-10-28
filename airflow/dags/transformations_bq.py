import os
import logging
import pendulum

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from google.cloud import storage
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Load environment variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
CREDENTIALS_FILE_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
BUCKET_TRANSFORMED = os.environ.get("GCP_GCS_BUCKET_TRANSFORMED", "de_data_lake_transformed_red-splice-439613-q8")
DATASET_ID = "premier_league"
TABLE_ID = "pl_partitioned"
FILENAME = 'team_wise_season_stats'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TransformationDataset") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0") \
    .config("parentProject", PROJECT_ID) \
    .getOrCreate()

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)

def read_from_bq():
    """ Read table from BigQuery """
    try:
        logger.info(f"read_from_bq - Read from BigQuery")
        table_path = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        df = spark.read \
            .format("bigquery") \
            .option("table", table_path) \
            .option("credentialsFile", CREDENTIALS_FILE_PATH) \
            .load()
        
        logger.info(f"read_from_bq - Successfully read from BigQuery")
        return df
    
    except Exception as e:
        logging.error(f"read_from_bq - Error: {e}")
        raise   

def drop_columns(df):
    """ Drop unnecessary columns from the DataFrame """
    try:
        logger.info("drop_columns - Drop unnecessary columns")
        columns_to_drop = [
            'Time', 'B365HomeTeam', 'B365Draw', 'B365AwayTeam',
            'B365_Over_2_5_Goals', 'B365_Under_2_5_Goals',
            'MarketMaxHomeTeam', 'MarketMaxDraw', 'MarketMaxAwayTeam',
            'MarketAvgHomeTeam', 'MarketAvgDraw', 'MarketAvgAwayTeam',
            'MarketMaxOver2_5Goals', 'MarketMaxUnder2_5Goals',
            'MarketAvgOver2_5Goals', 'MarketAvgUnder2_5Goals'
        ]
        df_dropped = df.drop(*columns_to_drop)
        logger.info("drop_columns Successfully dropped unnecessary columns")
        return df_dropped

    except Exception as e:
        logger.error(f"drop_columns - Error: {e}")
        raise

def fill_missing_values(df):
    """ Fill missing values in the DataFrame """
    try:
        logger.info("fill_missing_values - Fill missing values")
        col_to_fill = [
            'HomeTeamShots', 'AwayTeamShots',
            'HomeTeamShotsOnTarget', 'AwayTeamShotsOnTarget',
            'HomeTeamCorners', 'AwayTeamCorners',
            'HomeTeamFouls', 'AwayTeamFouls',
            'HomeTeamYellowCards', 'AwayTeamYellowCards',
            'HomeTeamRedCards', 'AwayTeamRedCards'
        ]
        
        df_filled = df.fillna({'HalfTimeHomeTeamGoals': 0, 'HalfTimeAwayTeamGoals': 0})\
                    .fillna({col: -1 for col in col_to_fill})
        logger.info("fill_missing_values - Successfully filled missing values")
        return df_filled

    except Exception as e:
        logger.error(f"fill_missing_values - Error: {e}")
        raise

def calculate_team_stats(df, team_column, goals_column, result_column):
    """ Calculate team statistics for either home or away teams """
    try:
        logger.info(f"calculate_team_stats - Calculate team statistics for {team_column}")
        stats = df.groupBy(['Season', team_column]).agg(
            F.sum(goals_column).alias('TotalGoals'),
            F.count(team_column).alias('TotalMatch'),
            F.sum(F.when(F.col(result_column) == 'H', 1).otherwise(0)).alias('TotalWins'),
            F.sum(F.when(F.col(result_column) == 'A', 1).otherwise(0)).alias('TotalLosses'),
            F.sum(F.when(F.col(result_column) == 'D', 1).otherwise(0)).alias('TotalDraw'),
            F.sum(F.when(F.col(f'{team_column}Shots').isNotNull() & (F.col(f'{team_column}Shots') != -1), F.col(f'{team_column}Shots')).otherwise(F.lit(None))).alias('TotalTeamShot'),
            F.sum(F.when(F.col(f'{team_column}ShotsOnTarget').isNotNull() & (F.col(f'{team_column}ShotsOnTarget') != -1), F.col(f'{team_column}ShotsOnTarget')).otherwise(F.lit(None))).alias('TotalShotOnTarget'),
            F.sum(F.when(F.col(f'{team_column}Corners').isNotNull() & (F.col(f'{team_column}Corners') != -1), F.col(f'{team_column}Corners')).otherwise(F.lit(None))).alias('TotalCorners'),
            F.sum(F.when(F.col(f'{team_column}Fouls').isNotNull() & (F.col(f'{team_column}Fouls') != -1), F.col(f'{team_column}Fouls')).otherwise(F.lit(None))).alias('TotalFouls'),
            F.sum(F.when(F.col(f'{team_column}YellowCards').isNotNull() & (F.col(f'{team_column}YellowCards') != -1), F.col(f'{team_column}YellowCards')).otherwise(F.lit(None))).alias('TotalYellowCard'),
            F.sum(F.when(F.col(f'{team_column}RedCards').isNotNull() & (F.col(f'{team_column}RedCards') != -1), F.col(f'{team_column}RedCards')).otherwise(F.lit(None))).alias('TotalRedCard')
        ).withColumnRenamed(team_column, 'Team')
        
        logger.info(f"calculate_team_stats - Successfully calculated statistics for {team_column}")
        return stats
    
    except Exception as e:
        logger.error(f"calculate_team_stats - Error: {e}")
        raise

def calculate_team_wise_season_stats(total_team_stats):
    """ Calculate team-wise statistics by season """
    try:
        logger.info("calculate_team_wise_season_stats - Calculate team-wise statistics by season")
        team_wise_stats = total_team_stats.groupBy(['Season', 'Team']).agg(
            F.sum('TotalGoals').alias('TotalGoals'),
            F.sum('TotalMatch').alias('TotalMatch'),
            F.sum('TotalWins').alias('TotalWins'),
            F.sum('TotalLosses').alias('TotalLosses'),
            F.sum('TotalDraw').alias('TotalDraw'),
            F.when(F.count('TotalTeamShot') == 0, 'Not Available').otherwise(F.sum(F.when(F.col('TotalTeamShot').isNotNull(), F.col('TotalTeamShot')).otherwise(F.lit(None)))).alias('TotalTeamShot'),
            F.when(F.count('TotalShotOnTarget') == 0, 'Not Available').otherwise(F.sum(F.when(F.col('TotalShotOnTarget').isNotNull(), F.col('TotalShotOnTarget')).otherwise(F.lit(None)))).alias('TotalShotOnTarget'),
            F.when(F.count('TotalCorners') == 0, 'Not Available').otherwise(F.sum(F.when(F.col('TotalCorners').isNotNull(), F.col('TotalCorners')).otherwise(F.lit(None)))).alias('TotalCorners'),
            F.when(F.count('TotalFouls') == 0, 'Not Available').otherwise(F.sum(F.when(F.col('TotalFouls').isNotNull(), F.col('TotalFouls')).otherwise(F.lit(None)))).alias('TotalFouls'),
            F.when(F.count('TotalYellowCard') == 0, 'Not Available').otherwise(F.sum(F.when(F.col('TotalYellowCard').isNotNull(), F.col('TotalYellowCard')).otherwise(F.lit(None)))).alias('TotalYellowCard'),
            F.when(F.count('TotalRedCard') == 0, 'Not Available').otherwise(F.sum(F.when(F.col('TotalRedCard').isNotNull(), F.col('TotalRedCard')).otherwise(F.lit(None)))).alias('TotalRedCard')
        )
        
        logger.info("calculate_team_wise_season_stats - Successfully calculated team-wise by season statistics")
        return team_wise_stats

    except Exception as e:
        logger.error(f"calculate_team_wise_season_stats - Error: {e}")
        raise

def save_to_parquet_local(df):
    """ Save DataFrame to Parquet file """
    try:
        logger.info("format_to_parquet - Start converting to Parquet")
        table = pa.Table.from_pandas(df)

        parquet_file = f"{PATH_TO_LOCAL_HOME}/{FILENAME}.parquet"
        logger.info(f"format_to_parquet - {parquet_file}")

        pq.write_table(table, parquet_file, compression='snappy')
        logger.info("format_to_parquet - Successfully converted DataFrame to Parquet.")
        return parquet_file
    except Exception as e:
        logger.error(f"format_to_parquet - Error converting to Parquet: {e}")
        raise

@task()
def transform_data():
    """ Transform data """
    try:
        logger.info("transform_data - Start transform data")
        
        df = read_from_bq()

        df_dropped = drop_columns(df)

        df_filled = fill_missing_values(df_dropped)

        home_team_stats = calculate_team_stats(df_filled, 'HomeTeam', 'FullTimeHomeTeamGoals', 'FullTimeResult')

        away_team_stats = calculate_team_stats(df_filled, 'AwayTeam', 'FullTimeAwayTeamGoals', 'FullTimeResult')

        total_team_stats = home_team_stats.union(away_team_stats)

        team_wise_season_stats = calculate_team_wise_season_stats(total_team_stats)

        team_wise_season_stats_pd = team_wise_season_stats.toPandas()

        parquet_file = save_to_parquet_local(team_wise_season_stats_pd)
        upload_to_gcs(BUCKET_TRANSFORMED, parquet_file, f"{FILENAME}.parquet")
        
        logger.info("transform_data - Successfully transformed data")
    
    except Exception as e:
        logger.error(f"transform_data - Error: {e}")
        raise

def upload_to_gcs(bucket_name, parquet_file, object_name):
    """Uploads a file to Google Cloud Storage."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        
        blob.upload_from_filename(parquet_file)

        logger.info(f"upload_to_gcs - File uploaded to {object_name}.")
    except Exception as e:
        logger.error(f"upload_to_gcs - Error uploading to GCS: {e}")
        raise

def load_team_wise_season_stats_bq():
    """ Load team-wise statistics to BigQuery """
    try:
        logger.info("load_team_wise_season_stats_bq - Load team-wise statistics to BigQuery")

        return BigQueryInsertJobOperator(
            task_id='load_team_wise_season_stats',
            job_id='load_team_wise_season_stats_job',
            configuration={
                "load": {
                    "destinationTable": {
                        "projectId": PROJECT_ID,  
                        "datasetId": DATASET_ID,  
                        "tableId": "team_wise_season_stats", 
                    },
                    "sourceFormat": "PARQUET",
                    "autodetect": True,
                    "writeDisposition": "WRITE_TRUNCATE",
                    "sourceUris": [f'gs://{BUCKET_TRANSFORMED}/{FILENAME}.parquet'],
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
def transformations_bq():
    transform_data_task = transform_data()
    load_team_wise_season_stats_bqtask = load_team_wise_season_stats_bq()

    transform_data_task >> load_team_wise_season_stats_bqtask

transformations_bq()