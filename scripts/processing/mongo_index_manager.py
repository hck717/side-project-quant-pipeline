#!/usr/bin/env python3
"""
MongoDB index manager for optimizing query performance.
Verifies required indexes exist and adds optional TTL indexes.
"""

import logging
import argparse
from datetime import datetime, timedelta
import pymongo
from pymongo import MongoClient, IndexModel
from pymongo.errors import PyMongoError
import os
import json
import time

# Import our MongoDB utilities
from mongo_utils import get_mongo_client, MONGO_DB, MONGO_COLLECTION_MARKET_DATA

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_indexes(client):
    """
    Verify that all required indexes exist in MongoDB.
    """
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION_MARKET_DATA]
    
    # Get existing indexes
    existing_indexes = list(collection.list_indexes())
    index_names = [idx.get('name') for idx in existing_indexes]
    
    logger.info(f"Found {len(existing_indexes)} indexes on collection {MONGO_COLLECTION_MARKET_DATA}")
    for idx in existing_indexes:
        logger.info(f"Index: {idx.get('name')} - {idx.get('key')}")
    
    # Check for essential indexes
    required_indexes = [
        ("symbol_timestamp_idx", {"symbol": 1, "timestamp": 1}),
        ("symbol_idx", {"symbol": 1}),
        ("timestamp_idx", {"timestamp": 1}),
        ("date_idx", {"date": 1})
    ]
    
    missing_indexes = []
    for name, key_pattern in required_indexes:
        if name not in index_names:
            missing_indexes.append((name, key_pattern))
    
    if missing_indexes:
        logger.warning(f"Missing {len(missing_indexes)} required indexes")
        for name, key_pattern in missing_indexes:
            logger.warning(f"Missing index: {name} - {key_pattern}")
            # Create missing index
            collection.create_index(
                [(field, pymongo.ASCENDING) for field, _ in key_pattern.items()],
                name=name,
                background=True
            )
            logger.info(f"Created index: {name}")
    else:
        logger.info("All required indexes are present.")
    
    return len(missing_indexes) == 0

def add_ttl_index(client, field="ingested_at", expiry_days=30):
    """
    Add a TTL index to automatically expire old data.
    
    Args:
        client: MongoDB client
        field: The datetime field to use for expiration (default: ingested_at)
        expiry_days: Number of days after which data should expire
    """
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION_MARKET_DATA]
    
    # Create TTL index
    index_name = f"{field}_ttl_idx"
    try:
        collection.create_index(
            [(field, pymongo.ASCENDING)],
            name=index_name,
            expireAfterSeconds=expiry_days * 24 * 60 * 60,  # Convert days to seconds
            background=True
        )
        logger.info(f"Created TTL index {index_name} on field {field} with {expiry_days} days expiry")
        return True
    except PyMongoError as e:
        logger.error(f"Failed to create TTL index: {e}")
        return False

def analyze_collection(client):
    """
    Analyze the collection for optimization opportunities.
    """
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION_MARKET_DATA]
    
    # Get collection stats
    stats = db.command("collStats", MONGO_COLLECTION_MARKET_DATA)
    
    logger.info("Collection Statistics:")
    logger.info(f"Document Count: {stats.get('count', 0)}")
    logger.info(f"Average Document Size: {stats.get('avgObjSize', 0)} bytes")
    logger.info(f"Storage Size: {stats.get('storageSize', 0) / (1024*1024):.2f} MB")
    logger.info(f"Index Size: {stats.get('totalIndexSize', 0) / (1024*1024):.2f} MB")
    
    # Get distinct symbols and count per symbol
    distinct_symbols = collection.distinct("symbol")
    logger.info(f"Found {len(distinct_symbols)} distinct symbols")
    
    # Sample a few symbols for document counts
    sample_symbols = distinct_symbols[:5] if len(distinct_symbols) > 5 else distinct_symbols
    for symbol in sample_symbols:
        count = collection.count_documents({"symbol": symbol})
        logger.info(f"Symbol {symbol}: {count} documents")
    
    # Get date range
    pipeline = [
        {"$group": {
            "_id": None,
            "minDate": {"$min": "$timestamp"},
            "maxDate": {"$max": "$timestamp"}
        }}
    ]
    date_range = list(collection.aggregate(pipeline))
    if date_range:
        logger.info(f"Date range: {date_range[0].get('minDate')} to {date_range[0].get('maxDate')}")
    
    return {
        "document_count": stats.get('count', 0),
        "avg_doc_size": stats.get('avgObjSize', 0),
        "storage_size_mb": stats.get('storageSize', 0) / (1024*1024),
        "index_size_mb": stats.get('totalIndexSize', 0) / (1024*1024),
        "distinct_symbols": len(distinct_symbols)
    }

def main():
    parser = argparse.ArgumentParser(description="MongoDB Index Manager")
    parser.add_argument("--verify", action="store_true", help="Verify all required indexes exist")
    parser.add_argument("--add-ttl", action="store_true", help="Add TTL index for data expiration")
    parser.add_argument("--ttl-field", default="ingested_at", help="Field to use for TTL index")
    parser.add_argument("--ttl-days", type=int, default=30, help="Days after which data expires")
    parser.add_argument("--analyze", action="store_true", help="Analyze collection statistics")
    
    args = parser.parse_args()
    
    client = get_mongo_client()
    
    if args.verify:
        verify_indexes(client)
    
    if args.add_ttl:
        add_ttl_index(client, args.ttl_field, args.ttl_days)
    
    if args.analyze:
        analyze_collection(client)
    
    # If no arguments provided, run everything
    if not (args.verify or args.add_ttl or args.analyze):
        verify_indexes(client)
        analyze_collection(client)

if __name__ == "__main__":
    main()