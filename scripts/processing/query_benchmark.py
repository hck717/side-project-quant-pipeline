#!/usr/bin/env python3
"""
MongoDB query benchmark tool.
Measures query performance for common market data access patterns.
"""

import logging
import time
import argparse
import json
from datetime import datetime, timedelta
import pandas as pd
import pymongo
from tabulate import tabulate
import statistics
import os

# Import our MongoDB utilities
from mongo_utils import get_mongo_client, MONGO_DB, MONGO_COLLECTION_MARKET_DATA

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_query(collection, query_func, name, iterations=5):
    """Run a query multiple times and measure performance."""
    durations = []
    row_counts = []
    
    for i in range(iterations):
        start_time = time.time()
        result = query_func(collection)
        end_time = time.time()
        
        duration_ms = (end_time - start_time) * 1000
        durations.append(duration_ms)
        
        # Get row count
        if isinstance(result, list):
            row_counts.append(len(result))
        elif hasattr(result, 'count_documents'):
            row_counts.append(result.count_documents())
        else:
            row_counts.append(-1)  # Unknown count
        
        logger.debug(f"Query {name} (run {i+1}/{iterations}): {duration_ms:.2f}ms, {row_counts[-1]} rows")
    
    # Calculate statistics
    avg_duration = statistics.mean(durations)
    median_duration = statistics.median(durations)
    min_duration = min(durations)
    max_duration = max(durations)
    avg_rows = statistics.mean(row_counts) if row_counts[0] != -1 else "Unknown"
    
    return {
        "name": name,
        "avg_duration_ms": avg_duration,
        "median_duration_ms": median_duration,
        "min_duration_ms": min_duration,
        "max_duration_ms": max_duration,
        "avg_rows": avg_rows,
        "iterations": iterations
    }

def get_random_symbol(collection):
    """Get a random symbol from the collection."""
    pipeline = [
        {"$group": {"_id": "$symbol"}},
        {"$sample": {"size": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    if result:
        return result[0]["_id"]
    return None

def get_recent_date_range(collection):
    """Get a recent date range from the collection."""
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$limit": 1},
        {"$project": {"timestamp": 1}}
    ]
    latest = list(collection.aggregate(pipeline))
    
    if latest:
        end_date = latest[0]["timestamp"]
        start_date = end_date - timedelta(days=1)
        return start_date, end_date
    
    # Fallback to last 24 hours from now
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=1)
    return start_date, end_date

def run_benchmarks(client):
    """Run a series of benchmark queries."""
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION_MARKET_DATA]
    
    # Get a random symbol and date range for testing
    test_symbol = get_random_symbol(collection)
    start_date, end_date = get_recent_date_range(collection)
    
    if not test_symbol:
        logger.error("No data found in collection")
        return []
    
    logger.info(f"Using symbol {test_symbol} for benchmarks")
    logger.info(f"Date range: {start_date} to {end_date}")
    
    # Define query functions
    queries = [
        {
            "name": "Get latest tick for symbol",
            "func": lambda coll: list(coll.find(
                {"symbol": test_symbol}
            ).sort("timestamp", -1).limit(1))
        },
        {
            "name": "Get 1-day data for symbol",
            "func": lambda coll: list(coll.find({
                "symbol": test_symbol,
                "timestamp": {"$gte": start_date, "$lte": end_date}
            }).sort("timestamp", 1))
        },
        {
            "name": "Get all symbols at timestamp",
            "func": lambda coll: list(coll.find({
                "timestamp": end_date
            }))
        },
        {
            "name": "Find high RSI (>70) instances",
            "func": lambda coll: list(coll.find({
                "rsi_14": {"$gt": 70}
            }).sort("timestamp", -1).limit(10))
        },
        {
            "name": "Count docs per symbol",
            "func": lambda coll: list(coll.aggregate([
                {"$group": {"_id": "$symbol", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]))
        },
        {
            "name": "VWAP above SMA",
            "func": lambda coll: list(coll.find({
                "vwap_session": {"$gt": "$sma_20"}
            }).sort("timestamp", -1).limit(10))
        },
        {
            "name": "Get symbol with time range and projection",
            "func": lambda coll: list(coll.find(
                {
                    "symbol": test_symbol,
                    "timestamp": {"$gte": start_date, "$lte": end_date}
                },
                {
                    "symbol": 1,
                    "timestamp": 1,
                    "close": 1,
                    "sma_20": 1,
                    "rsi_14": 1,
                    "_id": 0
                }
            ).sort("timestamp", 1))
        },
    ]
    
    # Run all queries
    results = []
    for query in queries:
        try:
            result = run_query(collection, query["func"], query["name"])
            results.append(result)
            
            # Log immediate results
            logger.info(
                f"Query '{query['name']}': "
                f"{result['avg_duration_ms']:.2f}ms avg, "
                f"{result['median_duration_ms']:.2f}ms median, "
                f"~{result['avg_rows']} rows"
            )
            
            # Check if query is fast enough
            if result['median_duration_ms'] > 200:
                logger.warning(f"Query '{query['name']}' is slow (> 200ms)")
        except Exception as e:
            logger.error(f"Error running query '{query['name']}': {e}")
    
    return results

def display_results(results):
    """Display benchmark results in a tabular format."""
    if not results:
        logger.warning("No benchmark results to display")
        return
    
    # Prepare table data
    table_data = []
    for r in results:
        table_data.append([
            r["name"],
            f"{r['avg_duration_ms']:.2f}",
            f"{r['median_duration_ms']:.2f}",
            f"{r['min_duration_ms']:.2f}",
            f"{r['max_duration_ms']:.2f}",
            r["avg_rows"]
        ])
    
    # Print table
    headers = ["Query Name", "Avg (ms)", "Median (ms)", "Min (ms)", "Max (ms)", "Rows"]
    print("\n" + tabulate(table_data, headers=headers, tablefmt="grid"))
    
    # Print summary
    slow_queries = [r["name"] for r in results if r["median_duration_ms"] > 200]
    if slow_queries:
        print(f"\n⚠️ Slow queries (>200ms): {len(slow_queries)}")
        for query in slow_queries:
            print(f"  - {query}")
    else:
        print("\n✅ All queries completed in under 200ms (median)")

def save_results(results, output_file):
    """Save benchmark results to a JSON file."""
    if not results:
        logger.warning("No benchmark results to save")
        return
    
    try:
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"Results saved to {output_file}")
    except Exception as e:
        logger.error(f"Error saving results: {e}")

def main():
    parser = argparse.ArgumentParser(description="MongoDB Query Benchmark Tool")
    parser.add_argument("--iterations", type=int, default=5, help="Number of iterations per query")
    parser.add_argument("--output", type=str, help="Output file for results (JSON)")
    
    args = parser.parse_args()
    
    logger.info("Starting MongoDB query benchmark")
    
    client = get_mongo_client()
    results = run_benchmarks(client)
    display_results(results)
    
    if args.output:
        save_results(results, args.output)

if __name__ == "__main__":
    main()