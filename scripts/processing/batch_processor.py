#!/usr/bin/env python3
"""
Batch market data processor using MinIO.
Reads market data files from MinIO, computes technical analysis metrics,
and stores results in MongoDB.
"""
import os
import io
import logging
import argparse
import tempfile
from datetime import datetime, timezone, timedelta
import pandas as pd
import pyarrow.parquet as pq
import traceback
from minio import Minio
from pymongo import MongoClient
from tenacity import retry, stop_after_attempt, wait_fixed

from ta_utils import calculate_ta_metrics, market_tick_to_dict
from mongo_utils import get_mongo_client, insert_market_data, setup_mongo_collections
from collections import defaultdict

# Configuration from environment variables with defaults
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
MINIO_SECURE = False
MINIO_BUCKET = "quant"

def get_minio_client():
    """Create and return a MinIO client."""
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def list_objects_for_date(minio_client, bucket, prefix, date_str=None):
    """
    List objects in MinIO bucket with specified prefix and date.
    
    Args:
        minio_client: MinIO client
        bucket: Bucket name
        prefix: Object prefix (e.g., 'eod/equities/')
        date_str: Date string in format 'YYYY-MM-DD'
        
    Returns:
        List of object names
    """
    search_prefix = prefix
    if date_str:
        search_prefix = f"{prefix}date={date_str}/"
    
    try:
        objects = list(minio_client.list_objects(bucket, prefix=search_prefix, recursive=True))
        logger.info(f"Found {len(objects)} objects with prefix '{search_prefix}'")
        return [obj.object_name for obj in objects]
    except Exception as e:
        logger.error(f"Error listing objects: {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def read_parquet_from_minio(minio_client, bucket, object_name):
    """
    Read Parquet file from MinIO into pandas DataFrame.
    
    Args:
        minio_client: MinIO client
        bucket: Bucket name
        object_name: Object name (path in bucket)
        
    Returns:
        pandas DataFrame
    """
    try:
        # Get the object
        response = minio_client.get_object(bucket, object_name)
        
        # Read into pandas using pyarrow
        with tempfile.NamedTemporaryFile() as temp_file:
            for d in response.stream(32*1024):
                temp_file.write(d)
            temp_file.flush()
            df = pd.read_parquet(temp_file.name)
        
        # Close the response
        response.close()
        response.release_conn()
        
        logger.info(f"Read {df.shape[0]} rows from {object_name}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading Parquet file {object_name}: {e}")
        raise

def extract_symbol_from_path(path):
    """Extract symbol name from a MinIO path."""
    filename = os.path.basename(path)
    symbol = filename.split('.')[0]  # Remove file extension
    return symbol

def process_object(minio_client, mongo_client, bucket, object_name):
    """Process a single object from MinIO."""
    try:
        df = read_parquet_from_minio(minio_client, bucket, object_name)
        symbol = extract_symbol_from_path(object_name)
        df_enriched = calculate_ta_metrics(df, symbol)

        # Group by symbol
        grouped_data = defaultdict(list)
        for _, row in df_enriched.iterrows():
            mongo_doc = market_tick_to_dict(row)
            mongo_doc['processor_type'] = 'batch'
            mongo_doc['source_object'] = object_name
            grouped_data[symbol].append(mongo_doc)

        # Insert all documents for each symbol
        success_count = 0
        for symbol, data_list in grouped_data.items():
            for data in data_list:
                if insert_market_data(mongo_client, data):
                    success_count += 1

        logger.info(f"Processed {success_count}/{len(df_enriched)} rows from {object_name}")
        return success_count
    except Exception as e:
        logger.error(f"Error processing object {object_name}: {e}")
        logger.error(traceback.format_exc())
        return 0

def process_batch(date_str=None, asset_type=None):
    """
    Process batch data for a specific date and asset type.
    
    Args:
        date_str: Date string in format 'YYYY-MM-DD'
        asset_type: Asset type (equities, bonds, or all)
    """
    start_time = datetime.now()
    logger.info(f"Starting batch processing at {start_time.isoformat()}")
    logger.info(f"Parameters: date={date_str}, asset_type={asset_type}")
    
    # Get default date if not specified (yesterday)
    if not date_str:
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        date_str = yesterday.strftime('%Y-%m-%d')
        logger.info(f"Using default date: {date_str}")
    
    # Determine prefixes based on asset type
    prefixes = []
    if (asset_type == 'equities' or asset_type == 'all'):
        prefixes.append('eod/equities/')
    if (asset_type == 'bonds' or asset_type == 'all'):
        prefixes.append('eod/bonds/')
    if not prefixes:
        logger.error(f"Invalid asset type: {asset_type}")
        return
    
    # Initialize clients
    minio_client = get_minio_client()
    mongo_client = get_mongo_client()
    
    
    total_processed = 0
    total_objects = 0
    
    # Process each prefix
    for prefix in prefixes:
        try:
            # List objects
            objects = list_objects_for_date(minio_client, MINIO_BUCKET, prefix, date_str)
            total_objects += len(objects)
            
            # Process each object
            for obj_name in objects:
                records = process_object(minio_client, mongo_client, MINIO_BUCKET, obj_name)
                total_processed += records
                
        except Exception as e:
            logger.error(f"Error processing prefix {prefix}: {e}")
            logger.error(traceback.format_exc())
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Batch processing completed in {duration:.2f} seconds")
    logger.info(f"Processed {total_processed} records from {total_objects} objects")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch process market data from MinIO to MongoDB")
    parser.add_argument("--date", type=str, help="Date to process in format YYYY-MM-DD")
    parser.add_argument("--asset-type", type=str, choices=["equities", "bonds", "all"], 
                       default="all", help="Asset type to process")
    
    args = parser.parse_args()
    process_batch(args.date, args.asset_type)