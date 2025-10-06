"""
Brewery Data Pipeline - Medallion Architecture DAG

This DAG orchestrates the complete data pipeline from API extraction
to analytical aggregations following the Medallion Architecture:
- Bronze Layer: Raw data ingestion
- Silver Layer: Data cleansing and transformation
- Gold Layer: Business-level aggregations

The pipeline demonstrates:
- Multi-layer data architecture
- Technology diversity (Pandas and PySpark)
- Idempotent processing using execution dates
- Error handling and retries
- Task dependencies and orchestration
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add scripts directory to Python path
# This allows importing custom modules from the scripts folder
sys.path.insert(0, '/opt/airflow/scripts')

# Import transformation functions from each layer
from bronze import save_to_bronze
from silver import transform_to_silver
from gold import create_aggregations


# Default arguments applied to all tasks in the DAG
# These settings ensure reliability and proper error handling
alert_recipients = [
    email.strip()
    for email in os.environ.get('ALERT_EMAILS', 'alerts@example.com').split(',')
    if email.strip()
]

default_args = {
    'owner': 'data-engineering',           # Team or person responsible
    'depends_on_past': False,              # Tasks don't depend on previous runs
    'email_on_failure': True,              # Enable email alerts on failure
    'email_on_retry': True,                # Enable email alerts on retry
    'email': alert_recipients,
    'retries': 2,                          # Retry failed tasks twice
    'retry_delay': timedelta(minutes=5),   # Wait 5 minutes between retries
}


def extract_task(**context):
    """
    Task wrapper for Bronze layer extraction.
    
    Extracts execution_date from Airflow context and passes it
    to the extraction function for data partitioning.
    
    Args:
        **context: Airflow context dictionary containing execution metadata
        
    Returns:
        str: Path to saved Bronze file
    """
    execution_date = context['ds']  # Airflow macro: YYYY-MM-DD format
    return save_to_bronze(execution_date)


def silver_transform_task(**context):
    """
    Task wrapper for Silver layer transformation.
    
    Reads from Bronze layer and applies data quality transformations
    using PySpark.
    
    Args:
        **context: Airflow context dictionary
        
    Returns:
        str: Path to Silver layer output
    """
    execution_date = context['ds']
    return transform_to_silver(execution_date)


def gold_transform_task(**context):
    """
    Task wrapper for Gold layer aggregation.
    
    Creates analytical aggregations from Silver layer using PySpark.
    
    Args:
        **context: Airflow context dictionary
        
    Returns:
        str: Path to Gold layer output
    """
    execution_date = context['ds']
    return create_aggregations(execution_date)


# Define the DAG
with DAG(
    dag_id='brewery_medallion_pipeline',
    default_args=default_args,
    description='Extract brewery data and process through medallion architecture',
    schedule_interval='0 3 * * *',        # Run daily at 03:00 UTC
    start_date=datetime(2024, 1, 1),      # Backfill start date (UTC)
    catchup=False,                        # Don't backfill historical dates
    tags=['brewery', 'medallion', 'etl'], # Tags for DAG organization
    max_active_runs=1,                    # Prevent concurrent runs
) as dag:
    
    # Bronze Layer: Extract raw data from API
    # Uses Pandas for simple HTTP requests and JSON handling
    extract = PythonOperator(
        task_id='bronze',
        python_callable=extract_task,
        provide_context=True,              # Pass Airflow context to function
        sla=timedelta(minutes=10),
    )
    
    # Silver Layer: Transform and clean data
    # Uses PySpark for distributed transformations and Parquet partitioning
    transform_silver = PythonOperator(
        task_id='silver',
        python_callable=silver_transform_task,
        provide_context=True,
        sla=timedelta(minutes=20),
    )
    
    # Gold Layer: Create business aggregations
    # Uses PySpark for analytical queries
    transform_gold = PythonOperator(
        task_id='gold',
        python_callable=gold_transform_task,
        provide_context=True,
        sla=timedelta(minutes=30),
    )
    
    # Define task dependencies (pipeline flow)
    # >> operator defines: left task must complete before right task
    # This creates a linear pipeline: Bronze → Silver → Gold
    extract >> transform_silver >> transform_gold

