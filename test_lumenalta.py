from functools import lru_cache
from pathlib import Path

from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType,
)


@lru_cache(maxsize=1)
def get_spark():
    sc = SparkContext(master="local[*]", appName="ML Logs Transformer")
    spark = SparkSession(sc)
    return spark


def load_logs(logs_path: Path) -> DataFrame:
    """
    TODO(Part 1.1): Complete this method
    """

    # Define an explicit schema
    schema = StructType([
        StructField("logId", StringType()),
        StructField("expId", IntegerType()),
        StructField("metricId", IntegerType()),
        StructField("valid", BooleanType()),
        StructField("createdAt", StringType()),
        StructField("ingestedAt", StringType()),
        StructField("step", IntegerType()),
        StructField("value", FloatType()),
    ])

    spark = get_spark()

    # Read json from Path
    return spark.read.schema(schema).json(str(logs_path))


def load_experiments(experiments_path: Path) -> DataFrame:
    """
    TODO(Part 1.2): Complete this method
    """

    # Define an explicit schema
    schema = StructType(
        [StructField("expId", IntegerType()), 
        StructField("expName", StringType())]
        )
    
    spark = get_spark()

    # Read csv from Path
    return spark.read.schema(schema).csv(str(experiments_path), header=True)


def load_metrics() -> DataFrame:
    """
    TODO(Part 1.3): Complete this method
    """
    # Define the static list of metrics
    metrics = [
        (0, "Loss"),
        (1, "Accuracy")
    ]

    # Define an explicit schema
    schema = StructType([
        StructField("metricId", IntegerType(), False),
        StructField("metricName", StringType(), False)
    ])

    spark = get_spark()

    # Create and return the Spark DataFrame
    return spark.createDataFrame(metrics, schema)
 

def join_tables(
    logs: DataFrame, experiments: DataFrame, metrics: DataFrame
) -> DataFrame:
    """
    TODO(Part 2): Complete this method
    """

    # Perform the joins
    joined_df = (
        logs.join(experiments, on="expId", how="inner")
            .join(metrics, on="metricId", how="inner")
    ).select(
        'logId', 'expId', 'expName', 'metricId', 'metricName', 
        'valid', 'createdAt', 'ingestedAt', 'step', 'value'
    )

    return joined_df


def filter_late_logs(data: DataFrame, hours: int) -> DataFrame:
    """
    TODO(Part 3): Complete this method
    """

    # Convert 'createdAt' and 'ingestedAt' to timestamp
    data = data.withColumn("createdAt", F.to_timestamp("createdAt")) \
               .withColumn("ingestedAt", F.to_timestamp("ingestedAt"))
    
    # Calculate time window
    max_ingestion_time = F.expr(f"createdAt + INTERVAL {hours} HOURS")
        
    return data.filter(data.ingestedAt <= max_ingestion_time)


def calculate_experiment_final_scores(data: DataFrame) -> DataFrame:
    """
    TODO(Part 4): Complete this method
    """

    # group by 'expName' and 'metricName' and calculate the min and max values
    final_metrics_df  = data.groupBy('expId', 'expName', 'metricId', 'metricName').agg(
        F.round(F.min('value'), 4).alias('minValue'),
        F.round(F.max('value'), 4).alias('maxValue'),
        F.round(F.avg('value'), 4).alias('avgValue')
    )

    return final_metrics_df


def save(data: DataFrame, output_path: Path):
    """
    TODO(Part 5): Complete this method
    """
    
    data.write.partitionBy("metricId")\
        .mode("overwrite")\
        .parquet(str(output_path))
