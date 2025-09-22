"""MongoDB utilities for the processing services."""
import logging
import asyncio
from typing import Dict, Any, List, Optional
import pymongo
from pymongo import MongoClient, IndexModel
from pymongo.errors import PyMongoError
import motor.motor_asyncio
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MongoDB connection parameters
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "quant"
MONGO_COLLECTION_MARKET_DATA = "market_data"

def get_mongo_client() -> MongoClient:
    """Create and return a MongoDB client."""
    try:
        client = MongoClient(MONGO_URI)
        # Test connection
        client.admin.command('ping')
        logger.info("MongoDB connection successful")
        return client
    except PyMongoError as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def get_async_mongo_client() -> motor.motor_asyncio.AsyncIOMotorClient:
    """Create and return an async MongoDB client."""
    try:
        client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        logger.info("Async MongoDB client created")
        return client
    except PyMongoError as e:
        logger.error(f"Failed to create async MongoDB client: {e}")
        raise

def setup_mongo_collections(client: MongoClient) -> None:
    """Set up MongoDB collections with appropriate indexes."""
    try:
        db = client[MONGO_DB]
        
        # Create or verify market_data collection
        if MONGO_COLLECTION_MARKET_DATA not in db.list_collection_names():
            logger.info(f"Creating collection: {MONGO_COLLECTION_MARKET_DATA}")
            db.create_collection(MONGO_COLLECTION_MARKET_DATA)
        
        # Create indexes
        collection = db[MONGO_COLLECTION_MARKET_DATA]
        
        indexes = [
            IndexModel([("symbol", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)], 
                      name="symbol_timestamp_idx", unique=True),
            IndexModel([("symbol", pymongo.ASCENDING)], name="symbol_idx"),
            IndexModel([("timestamp", pymongo.ASCENDING)], name="timestamp_idx"),
            IndexModel([("date", pymongo.ASCENDING)], name="date_idx")
        ]
        
        collection.create_indexes(indexes)
        logger.info("MongoDB indexes created or verified")
        
    except PyMongoError as e:
        logger.error(f"Error setting up MongoDB collections: {e}")
        raise

def insert_market_data(client: MongoClient, data: Dict[str, Any]) -> bool:
    """
    Insert market data into MongoDB with upsert on symbol+timestamp.
    
    Args:
        client: MongoDB client
        data: Market data dictionary
        
    Returns:
        True if successful, False otherwise
    """
    try:
        start_time = datetime.now()
        
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION_MARKET_DATA]
        
        # Ensure required fields exist
        if 'symbol' not in data or 'timestamp' not in data:
            logger.error("Missing required fields 'symbol' or 'timestamp' in data")
            return False
        
        # Use symbol and timestamp as the unique identifier
        filter_dict = {
            'symbol': data['symbol'],
            'timestamp': data['timestamp']
        }
        
        result = collection.update_one(
            filter_dict,
            {'$set': data},
            upsert=True
        )
        
        end_time = datetime.now()
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        if result.modified_count > 0:
            logger.debug(f"Updated document for {data['symbol']} at {data['timestamp']} in {duration_ms:.2f}ms")
            return True
        elif result.upserted_id:
            logger.debug(f"Inserted document for {data['symbol']} at {data['timestamp']} in {duration_ms:.2f}ms")
            return True
        else:
            logger.warning(f"Document exists but no changes for {data['symbol']} at {data['timestamp']}")
            return True
            
    except PyMongoError as e:
        logger.error(f"Error inserting market data: {e}")
        return False

async def insert_market_data_async(client: motor.motor_asyncio.AsyncIOMotorClient, data: Dict[str, Any]) -> bool:
    """
    Asynchronously insert market data into MongoDB with upsert on symbol+timestamp.
    
    Args:
        client: Async MongoDB client
        data: Market data dictionary
        
    Returns:
        True if successful, False otherwise
    """
    try:
        start_time = datetime.now()
        
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION_MARKET_DATA]
        
        # Ensure required fields exist
        if 'symbol' not in data or 'timestamp' not in data:
            logger.error("Missing required fields 'symbol' or 'timestamp' in data")
            return False
        
        # Use symbol and timestamp as the unique identifier
        filter_dict = {
            'symbol': data['symbol'],
            'timestamp': data['timestamp']
        }
        
        result = await collection.update_one(
            filter_dict,
            {'$set': data},
            upsert=True
        )
        
        end_time = datetime.now()
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        if result.modified_count > 0:
            logger.debug(f"Updated document for {data['symbol']} at {data['timestamp']} in {duration_ms:.2f}ms")
            return True
        elif result.upserted_id:
            logger.debug(f"Inserted document for {data['symbol']} at {data['timestamp']} in {duration_ms:.2f}ms") 
            return True
        else:
            logger.warning(f"Document exists but no changes for {data['symbol']} at {data['timestamp']}")
            return True
            
    except PyMongoError as e:
        logger.error(f"Error inserting market data: {e}")
        return False

async def insert_many_market_data_async(client: motor.motor_asyncio.AsyncIOMotorClient, 
                                       data_list: List[Dict[str, Any]]) -> bool:
    """
    Insert multiple market data documents asynchronously with individual upserts.
    
    Args:
        client: Async MongoDB client
        data_list: List of market data dictionaries
        
    Returns:
        True if all successful, False if any failed
    """
    if not data_list:
        logger.warning("Empty data list provided to insert_many_market_data_async")
        return True
    
    start_time = datetime.now()
    results = await asyncio.gather(*[
        insert_market_data_async(client, data) 
        for data in data_list
    ], return_exceptions=True)
    
    end_time = datetime.now()
    duration_ms = (end_time - start_time).total_seconds() * 1000
    success_count = sum(1 for r in results if r is True)
    
    logger.info(f"Bulk insert: {success_count}/{len(data_list)} documents in {duration_ms:.2f}ms")
    
    return all(r is True for r in results)