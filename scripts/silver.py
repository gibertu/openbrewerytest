"""
Silver Layer: Transform Bronze Parquet to curated Silver Parquet using PySpark

This module handles the transformation phase from Bronze to Silver.
It reads the deduplicated Bronze Parquet data and applies data quality improvements,
type conversions, and partitioning strategies using PySpark for distributed processing.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, current_timestamp, trim, upper
from pathlib import Path
import logging
import shutil

try:
    from airflow.exceptions import AirflowException
except ModuleNotFoundError:  # Allow running outside Airflow container
    class AirflowException(Exception):
        """Fallback exception when Airflow is not installed locally."""


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    """
    Initialize SparkSession with optimized configurations.
    
    Configurations are tuned for local development but can be
    adjusted for cluster deployment.
    
    Returns:
        SparkSession: Configured Spark session
    """
    spark = (
        SparkSession.builder
        .appName("BreweryPipeline-SilverLayer")
        .config("spark.sql.adaptive.enabled", "true")  # Adaptive query execution
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def clean_and_transform(df):
    """
    Apply data quality transformations to raw brewery data.
    
    Transformations include:
    - Handling null values
    - Standardizing text fields
    - Type casting for numeric fields
    - Adding metadata columns
    
    Args:
        df: PySpark DataFrame with raw data
        
    Returns:
        PySpark DataFrame with cleaned and transformed data
    """
    logger.info(f"Starting transformation. Input records: {df.count()}")
    
    # Drop legacy state field; downstream work relies on state_province
    df_clean = df.drop("state")

    # Identify column groups for normalisation
    string_cols = {f.name for f in df_clean.schema.fields if f.dataType.simpleString() == "string"}
    double_cols = {"longitude", "latitude"}

    # Apply trim + upper-case to every string column for consistent casing
    for column_name in string_cols:
        df_clean = df_clean.withColumn(
            column_name,
            upper(trim(col(column_name)))
        )

    # Cast coordinates to double and replace null/NaN with invalid coordinates
    for column_name in double_cols:
        cleaned = trim(col(column_name)).cast("double")
        df_clean = df_clean.withColumn(
            column_name,
            when(F.isnan(cleaned) | cleaned.isNull(), F.lit(-999.0)).otherwise(cleaned)
        )

    # Ensure state_province and brewery_type never return null downstream
   
    df_clean = df_clean.fillna({
        "state_province": "UNKNOWN",
        "brewery_type": "UNKNOWN",
    })

    # Add processing metadata for lineage tracking
    df_clean = df_clean.withColumn("processed_at", current_timestamp())
    
    # Log transformation statistics
    total_records = df_clean.count()
    null_states = df_clean.filter(col("state_province") == "UNKNOWN").count()
    logger.info(f"Transformation complete. Output records: {total_records}")
    logger.info(f"Data quality: {null_states} records with state_province=UNKNOWN")
    
    return df_clean


def transform_to_silver(execution_date: str) -> str:
    """
    Read Bronze data and write to Silver layer with Parquet format and partitioning.
    
    The Silver layer stores curated, cleaned data optimized for analytics.
    Data is partitioned by state_province for efficient querying and processing.
    
    Args:
        execution_date (str): Execution date in YYYY-MM-DD format
        
    Returns:
        str: Path to Silver layer output
        
    Example:
        transform_to_silver("2024-01-15")
        # Creates: /opt/airflow/data/3.silver/2024-01-15/breweries.parquet/state_province=CA/...
    """
    # Initialize Spark session
    spark = get_spark_session()
    
    try:
        # Read curated Parquet from Bronze layer
        bronze_path = f"/opt/airflow/data/2.bronze/{execution_date}/breweries.parquet"
        logger.info(f"Reading Bronze Parquet from: {bronze_path}")

        df_bronze = spark.read.parquet(bronze_path)
        
        # Apply transformations
        df_silver = clean_and_transform(df_bronze)

        df_silver = df_silver.dropDuplicates(["id"])  # ensure uniqueness after transformations

        # Lightweight data-quality gates before persisting the Silver layer
        total_records = df_silver.count()
        if total_records == 0:
            raise AirflowException("Silver layer produced zero rows; aborting write")

        unknown_state_records = df_silver.filter(col("state_province") == "UNKNOWN").count()
        if unknown_state_records / total_records > 0.5:
            raise AirflowException(
                "Silver layer has more than 50% rows with unknown state; investigate Bronze data"
            )
        
        # Prepare Silver layer output path
        silver_dir = Path(f"/opt/airflow/data/3.silver/{execution_date}")
        silver_dir.mkdir(parents=True, exist_ok=True)
        output_path = silver_dir / "breweries.parquet"

        # Remove any previous Silver output to avoid overwrite conflicts
        if output_path.exists():
            logger.info("Removing previous Silver output before overwrite")
            try:
                if output_path.is_dir():
                    shutil.rmtree(output_path)
                else:
                    output_path.unlink()
            except OSError as exc:
                logger.error(f"Failed to remove local Silver output: {exc}")
                raise

        # Clean old outputs through Hadoop FS as well to prevent permission issues
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        output_hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(str(output_path))
        if fs.exists(output_hadoop_path):
            logger.info("Cleaning previous Silver output using Hadoop FS")
            if not fs.delete(output_hadoop_path, True):
                logger.error("Hadoop FS could not remove previous Silver output")
                raise OSError("Hadoop FS deletion failed")

        # Write to Parquet format with state_province partitioning
        logger.info(f"Writing to Silver layer: {output_path}")
        df_silver.write.mode("overwrite").partitionBy("state_province").parquet(str(output_path))
        
        # Also expose a CSV snapshot for reviewers (single file)
        csv_output_path = silver_dir / "breweries.csv"

        if csv_output_path.exists():
            logger.info("Removing previous Silver CSV output before overwrite")
            try:
                if csv_output_path.is_dir():
                    shutil.rmtree(csv_output_path)
                else:
                    csv_output_path.unlink()
            except OSError as exc:
                logger.error(f"Failed to remove local Silver CSV output: {exc}")
                raise

        csv_hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(str(csv_output_path))
        if fs.exists(csv_hadoop_path):
            logger.info("Cleaning previous Silver CSV output using Hadoop FS")
            if not fs.delete(csv_hadoop_path, True):
                logger.error("Hadoop FS could not remove previous Silver CSV output")
                raise OSError("Hadoop FS CSV deletion failed")

        logger.info(f"Writing Silver CSV snapshot to {csv_output_path}")
        (
            df_silver.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(str(csv_output_path))
        )
        
        # Log partition statistics
        partition_count = df_silver.select("state_province").distinct().count()
        logger.info(f"Silver layer: Created {partition_count} state_province partitions")
        logger.info(f"Silver layer: Data saved to {output_path}")
        
        return str(output_path)
        
    finally:
        # Clean up Spark session
        spark.stop()


if __name__ == "__main__":
    """
    Local testing entry point.
    Run directly with: python scripts/silver.py
    """
    from datetime import datetime
    test_date = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Running Silver transformation in test mode for date: {test_date}")
    transform_to_silver(test_date)
