#!/usr/bin/env python3
"""
Consolidates MongoDB data by grouping all data with the same ticker symbol.
"""

import logging
import pymongo
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime
import os

# MongoDB connection parameters
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB = "quant"
MONGO_COLLECTION_MARKET_DATA = "market_data"  # Original collection

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def consolidate_mongodb(client):
    """
    Consolidates MongoDB data by grouping all data with the same ticker symbol.
    """
    try:
        db = client[MONGO_DB]
        source_collection = db[MONGO_COLLECTION_MARKET_DATA]
        
        # Get distinct symbols
        distinct_symbols = source_collection.distinct("symbol")
        logger.info(f"Found {len(distinct_symbols)} distinct symbols")
        
        for symbol in distinct_symbols:
            logger.info(f"Processing symbol: {symbol}")
            
            # Fetch all data for the symbol
            symbol_data = list(source_collection.find({"symbol": symbol}).sort("timestamp", pymongo.ASCENDING))
            
            if not symbol_data:
                logger.warning(f"No data found for symbol: {symbol}")
                continue
            
            # Option 1: Append all data to a 'ticks' array in a single document
            consolidated_document = {
                "symbol": symbol,
                "first_timestamp": symbol_data[0]["timestamp"],
                "last_timestamp": symbol_data[-1]["timestamp"],
                "tick_count": len(symbol_data),
                "ticks": symbol_data  # Store all ticks in an array
            }
            
            # Create or update a new collection for each symbol
            target_collection_name = f"consolidated_data"
            target_collection = db[target_collection_name]
            
            # Upsert the consolidated document
            result = target_collection.update_one(
                {"symbol": symbol},
                {"$set": consolidated_document},
                upsert=True
            )
            
            if result.modified_count > 0 or result.upserted_id:
                logger.info(f"Successfully consolidated data for symbol: {symbol}")
            else:
                logger.warning(f"No changes made for symbol: {symbol}")
        
        logger.info("Consolidation process completed.")
        
    except PyMongoError as e:
        logger.error(f"Error during consolidation: {e}")

def main():
    client = MongoClient(MONGO_URI)
    consolidate_mongodb(client)

if __name__ == "__main__":
    main()