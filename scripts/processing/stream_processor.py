#!/usr/bin/env python3
"""
Real-time market data processor using aiokafka.
Consumes market ticks from Kafka, computes technical analysis metrics, 
and stores results in MongoDB.
"""
import asyncio
import json
import logging
import signal
import sys
import time
from typing import Dict, Any, List, Set
from datetime import datetime, timezone
import pandas as pd
import traceback

from aiokafka import AIOKafkaConsumer
import motor.motor_asyncio

from ta_utils import calculate_ta_metrics, market_tick_to_dict, process_market_tick_json
from mongo_utils import get_async_mongo_client, insert_market_data_async

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "redpanda:9092"
KAFKA_TOPICS = ["crypto_ticks", "equities_ticks", "bonds_data"]
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "quant"
MONGO_COLLECTION = "market_data"

# Symbol cache for accumulating data for technical analysis
symbol_cache = {}
processed_count = 0
start_time = datetime.now()

async def process_message(msg, mongo_client):
    """Process a message from Kafka."""
    global processed_count, start_time
    
    try:
        message_time = time.time()
        value_str = msg.value.decode('utf-8')
        
        # Parse JSON
        tick_data = json.loads(value_str)
        
        # Standardize JSON fields
        tick_data = process_market_tick_json(tick_data)
        
        symbol = tick_data.get('symbol')
        if not symbol:
            logger.warning(f"Message missing symbol field: {value_str[:100]}")
            return
            
        # Keep a small cache of recent ticks per symbol for TA calculations
        if symbol not in symbol_cache:
            symbol_cache[symbol] = []
        
        # Add to symbol cache
        symbol_cache[symbol].append(tick_data)
        
        # Keep cache size manageable - limit to last 100 ticks per symbol
        # This is enough for most common technical indicators
        if len(symbol_cache[symbol]) > 100:
            symbol_cache[symbol] = symbol_cache[symbol][-100:]
        
        # Convert cache to DataFrame for this symbol
        df = pd.DataFrame(symbol_cache[symbol])
        
        # Calculate technical indicators
        enriched_df = calculate_ta_metrics(df, symbol)
        
        # Get the latest tick with calculated metrics
        latest_enriched = enriched_df.iloc[-1]
        
        # Convert to dictionary for MongoDB
        mongo_doc = market_tick_to_dict(latest_enriched)
        
        # Add processing metadata
        mongo_doc['processing_latency_ms'] = round((time.time() - message_time) * 1000, 2)
        mongo_doc['processor_type'] = 'stream'
        
        # Store in MongoDB
        success = await insert_market_data_async(mongo_client, mongo_doc)
        
        processed_count += 1
        if processed_count % 100 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = processed_count / elapsed if elapsed > 0 else 0
            logger.info(f"Processed {processed_count} messages ({rate:.2f} msgs/sec)")
        
        if not success:
            logger.error(f"Failed to insert data for {symbol} at {mongo_doc.get('timestamp')}")
            
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        logger.error(traceback.format_exc())

async def consume_messages():
    """Consume messages from Kafka."""
    logger.info(f"Starting Kafka consumer for topics: {KAFKA_TOPICS}")
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    
    # Create consumer
    consumer = AIOKafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='market-data-processor',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        max_poll_records=500
    )
    
    # Connect to MongoDB
    mongo_client = get_async_mongo_client()
    
    # Handle shutdown gracefully
    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()
    
    def handle_signal():
        logger.info("Received shutdown signal, closing consumer...")
        stop_event.set()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)
    
    # Start consumer
    await consumer.start()
    logger.info("Kafka consumer started successfully")
    
    try:
        async for msg in consumer:
            if stop_event.is_set():
                break
                
            topic = msg.topic
            partition = msg.partition
            offset = msg.offset
            key = msg.key.decode('utf-8') if msg.key else None
            
            logger.debug(f"Received message: topic={topic}, partition={partition}, "
                         f"offset={offset}, key={key}")
            
            await process_message(msg, mongo_client)
            
    finally:
        logger.info("Stopping Kafka consumer...")
        await consumer.stop()
        logger.info("Kafka consumer stopped")

async def main():
    """Main entry point."""
    global start_time
    start_time = datetime.now()
    logger.info(f"Starting market data stream processor at {start_time.isoformat()}")
    
    try:
        await consume_messages()
    except Exception as e:
        logger.error(f"Error in main consumer loop: {e}")
        logger.error(traceback.format_exc())
    
    end_time = datetime.now()
    logger.info(f"Stream processor stopped at {end_time.isoformat()}, "
               f"duration: {end_time - start_time}")

if __name__ == "__main__":
    asyncio.run(main())