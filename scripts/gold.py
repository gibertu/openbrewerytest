"""
Gold Layer: Aggregate the number of breweries for analytics using PySpark.

Produces a ready-to-query dataset with brewery quantity
grouped by country, state_province, city, and brewery type.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, trim, upper
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    """
    Initialize SparkSession for SQL operations.
    
    Returns:
        SparkSession: Configured Spark session
    """
    spark = (
        SparkSession.builder
        .appName("BreweryPipeline-GoldLayer")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_aggregations(execution_date: str) -> str:
    """
    Produce the Gold aggregate with brewery counts by type and location.

    Args:
        execution_date (str): Execution date in YYYY-MM-DD format

    Returns:
        str: Filesystem path to the aggregated Parquet output

    Example:
        create_aggregations("2024-01-15")
        # Creates: /opt/airflow/data/4.gold/2024-01-15/brewery_counts_by_type_and_location.parquet
    """
    spark = get_spark_session()
    
    try:
        # Read curated data from Silver layer
        silver_path = f"/opt/airflow/data/3.silver/{execution_date}/breweries.parquet"
        logger.info(f"Reading Silver data from: {silver_path}")
        
        df_silver = spark.read.parquet(silver_path)

        logger.info("Building Gold aggregated view (country/state/city/type)")

        df_gold_agg = (
            df_silver
            .withColumn("state_province", upper(trim(col("state_province"))))
            .withColumn("city", upper(trim(F.coalesce(col("city"), F.lit("UNKNOWN")))))
            .withColumn("country", upper(trim(F.coalesce(col("country"), F.lit("UNKNOWN")))))
            .withColumn("brewery_type", upper(trim(F.coalesce(col("brewery_type"), F.lit("UNKNOWN")))))
            .groupBy("country", "state_province", "city", "brewery_type")
            .agg(F.countDistinct("id").alias("qty"))
            .orderBy("state_province", "city", "brewery_type")
        )

        total_result = df_gold_agg.agg(F.sum("qty").alias("total")).collect()[0][0]
        total_breweries = total_result if total_result is not None else 0
        logger.info("Aggregation complete. Total breweries counted: %s", total_breweries)
        
        # Prepare Gold layer output path
        gold_dir = Path(f"/opt/airflow/data/4.gold/{execution_date}")
        gold_dir.mkdir(parents=True, exist_ok=True)
        
        # Save as Parquet for analytical queries
        parquet_path = gold_dir / "brewery_counts_by_type_and_location.parquet"
        logger.info(f"Writing Gold aggregated view to {parquet_path}")
        df_gold_agg.write.mode("overwrite").parquet(str(parquet_path))

        csv_path = gold_dir / "brewery_counts_by_type_and_location.csv"
        logger.info(f"Writing Gold aggregated CSV to {csv_path}")
        df_gold_agg.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(csv_path))

        logger.info(f"Gold layer outputs saved to: {gold_dir}")

        return str(parquet_path)
        
    finally:
        # Clean up Spark session
        spark.stop()


if __name__ == "__main__":
    """
    Local testing entry point.
    Run directly with: python scripts/gold.py
    """
    from datetime import datetime
    test_date = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Running Gold aggregation in test mode for date: {test_date}")
    create_aggregations(test_date)
