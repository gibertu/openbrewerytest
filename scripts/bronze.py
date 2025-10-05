"""
Bronze Layer: Extract raw data from Open Brewery DB API using Pandas

This module handles the extraction phase of the ETL pipeline.
It fetches brewery data from the Open Brewery DB API with pagination
and saves it as raw JSON files in the Raw and Bronze layer.

"""

import json
import logging
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging for pipeline observability
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_TIMEOUT_SECONDS = 30
RETRY_STRATEGY = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    respect_retry_after_header=True,
)


def _build_session() -> requests.Session:
    """Create a requests session with retry/backoff policy."""
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=RETRY_STRATEGY)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_breweries(per_page: int = 200) -> List[Dict]:
    """
    Fetch all breweries from the Open Brewery DB API with pagination.
    
    The API limits results per page, so we iterate through pages until
    no more data is returned.
    
    Args:
        per_page (int): Number of records to fetch per API request
        
    Returns:
        List[Dict]: Complete list of brewery records as dictionaries
        
    Raises:
        requests.exceptions.RequestException: If API request fails
    """
    base_url = "https://api.openbrewerydb.org/v1/breweries"
    all_breweries = []
    page = 1

    logger.info("Starting API extraction from Open Brewery DB...")
    session = _build_session()

    try:
        while True:
            params = {"per_page": per_page, "page": page}
            try:
                response = session.get(
                    base_url,
                    params=params,
                    timeout=API_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
            except requests.exceptions.RequestException as exc:
                logger.error(
                    "Error fetching page %s with params %s: %s", page, params, exc
                )
                raise

            breweries = response.json()

            # Empty response indicates we've reached the last page
            if not breweries:
                logger.info("Reached end of pagination at page %s", page)
                break

            all_breweries.extend(breweries)
            logger.info("Fetched page %s: %s breweries", page, len(breweries))
            page += 1
    finally:
        session.close()
    
    logger.info(f"Total breweries extracted: {len(all_breweries)}")
    return all_breweries


def save_to_bronze(execution_date: str) -> str:
    """
    Extract data from API and persist to Bronze layer.
    
    Persists a raw JSON snapshot in a landing zone for auditing and a
    deduplicated Parquet enriched with an ingestion timestamp for downstream consumption.
    
    Args:
        execution_date (str): Execution date in YYYY-MM-DD format
                             Used for partitioning data by ingestion date
        
    Returns:
        str: Path to the saved Bronze Parquet dataset
        
    Example:
        save_to_bronze("2024-01-15")
        # Creates: /opt/airflow/data/1.raw/2024-01-15/breweries_raw.json
        #          /opt/airflow/data/2.bronze/2024-01-15/breweries.parquet
    """
    # Fetch data from API
    logger.info(f"Starting Bronze layer extraction for date: {execution_date}")
    api_records = fetch_breweries()

    # Remove potential duplicates using brewery ID before persisting
    df_bronze = pd.DataFrame(api_records)
    df_bronze = df_bronze.drop_duplicates(subset=['id'])
    df_bronze['ingested_at'] = pd.Timestamp.utcnow().isoformat()

    # Create landing/raw directory for JSON snapshot
    raw_dir = Path(f"/opt/airflow/data/1.raw/{execution_date}")
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Create Bronze directory with date partition for curated Parquet
    # This allows tracking data lineage and enables reprocessing
    bronze_dir = Path(f"/opt/airflow/data/2.bronze/{execution_date}")
    bronze_dir.mkdir(parents=True, exist_ok=True)
    
    # Save raw JSON to Bronze layer
    # JSON format preserves exact API response structure
    raw_output_path = raw_dir / "breweries_raw.json"
    with open(raw_output_path, 'w', encoding='utf-8') as f:
        json.dump(api_records, f, indent=2, ensure_ascii=False)

    logger.info(
        f"Landing zone: Raw JSON snapshot saved with {len(api_records)} records to {raw_output_path}"
    )

    # Persist curated Bronze dataset in Parquet for downstream Spark reads
    parquet_output_path = bronze_dir / "breweries.parquet"
    df_bronze.to_parquet(parquet_output_path, index=False)
    logger.info(
        f"Bronze layer: {len(df_bronze)} deduplicated records saved to Parquet at {parquet_output_path}"
    )

    csv_output_path = bronze_dir / "breweries.csv"
    df_bronze.to_csv(csv_output_path, index=False)
    logger.info(
        f"Bronze layer: {len(df_bronze)} deduplicated records saved to CSV at {csv_output_path}"
    )

    # Log basic statistics for monitoring
    logger.info(f"Data quality check - Columns: {list(df_bronze.columns)}")
    logger.info(f"Data quality check - Shape: {df_bronze.shape}")

    return str(parquet_output_path)


if __name__ == "__main__":
    """
    Local testing entry point.
    Run directly with: python scripts/bronze.py
    """
    from datetime import datetime
    test_date = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Running extraction in test mode for date: {test_date}")
    save_to_bronze(test_date)
