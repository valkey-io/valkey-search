#!/usr/bin/env python3
"""
Script to setup both Redis and Valkey with the same index and data.
- Connects to Redis Docker on port 6380
- Connects to Valkey on port 7001
- Flushes both databases
- Creates the same index on both
- Inserts the same 10 hash entries on both
"""
import redis
import time
import os
import sys

# Data
INDEX_NAME = "idx"
CREATE_INDEX_CMD = [
    "FT.CREATE", INDEX_NAME, "ON", "HASH", 
    "PREFIX", "1", "hash:", 
    "SCHEMA", 
    "title", "TEXT", "WITHSUFFIXTRIE", "NOSTEM",
    "body", "TEXT", "WITHSUFFIXTRIE", "NOSTEM",
    "color", "TAG",
    "price", "NUMERIC"
]

HASH_DATA = [
    ('hash:00', {'title': 'orange mango cherry', 'body': 'banana grape apple', 'color': 'red', 'price': '32'}),
    ('hash:01', {'title': 'cherry', 'body': 'mango', 'color': 'green', 'price': '47'}),
    ('hash:02', {'title': 'mango', 'body': 'banana', 'color': 'purple', 'price': '11'}),
    ('hash:03', {'title': 'banana grape', 'body': 'banana apple', 'color': 'yellow', 'price': '50'}),
    ('hash:04', {'title': 'orange', 'body': 'apple', 'color': 'red', 'price': '9'}),
    ('hash:05', {'title': 'grape', 'body': 'grape', 'color': 'yellow', 'price': '42'}),
    ('hash:06', {'title': 'apple mango', 'body': 'banana apple', 'color': 'purple', 'price': '44'}),
    ('hash:07', {'title': 'banana', 'body': 'mango', 'color': 'red', 'price': '22'}),
    ('hash:08', {'title': 'banana mango', 'body': 'banana grape', 'color': 'green', 'price': '42'}),
    ('hash:09', {'title': 'apple', 'body': 'mango', 'color': 'red', 'price': '18'}),
    ('hash:10', {'title': 'orange cherry horse horse orange cherry city', 'body':'dog orange cat dog forest orange forest banana apple cat', 'color':'yellow', 'price': 7}),
]

def start_redis_container():
    """Start Redis Stack container"""
    print("Starting Redis Stack container...")
    os.system("docker rm -f test-redis-search 2>/dev/null")
    ret = os.system("docker run -d --name test-redis-search -p 6380:6379 redis/redis-stack-server")
    if ret != 0:
        print("Failed to start Redis Stack server")
        sys.exit(1)
    print("Container started, waiting for Redis to be ready...")
    time.sleep(3)

def setup_database(client, db_name):
    """Setup index and data on a database"""
    print(f"\n{'='*60}")
    print(f"Setting up {db_name}")
    print(f"{'='*60}")
    
    # Flush all data
    print(f"\nFlushing all data in {db_name}...")
    client.flushall()
    
    # Create index
    print(f"\nCreating index: {INDEX_NAME}")
    print(f"Command: {' '.join(CREATE_INDEX_CMD)}")
    result = client.execute_command(*CREATE_INDEX_CMD)
    print(f"Result: {result}")
    
    # Insert data
    print(f"\nInserting {len(HASH_DATA)} hash entries...")
    for key, data in HASH_DATA:
        client.hset(key, mapping=data)
        print(f"  Inserted: {key}")
    
    # Wait for indexing
    print("\nWaiting for indexing to complete...")
    time.sleep(1)
    print(f"{db_name} setup complete!")

def main():
    # Start Redis container
    start_redis_container()
    
    try:
        # Connect to Redis
        print("\n" + "="*60)
        print("Connecting to Redis on localhost:6380...")
        print("="*60)
        redis_client = redis.Redis(host='localhost', port=6380, decode_responses=False)
        
        # Wait for Redis connection
        for i in range(30):
            try:
                redis_client.ping()
                print("Connected to Redis!")
                break
            except redis.ConnectionError:
                if i == 29:
                    print("Failed to connect to Redis")
                    sys.exit(1)
                time.sleep(0.5)
        
        # Setup Redis
        setup_database(redis_client, "Redis")
        
        # Connect to Valkey
        print("\n" + "="*60)
        print("Connecting to Valkey on localhost:7001...")
        print("="*60)
        valkey_client = redis.Redis(host='localhost', port=7001, decode_responses=False)
        
        try:
            valkey_client.ping()
            print("Connected to Valkey!")
        except redis.ConnectionError:
            print("ERROR: Could not connect to Valkey server on localhost:7001")
            print("Please start Valkey server first")
            sys.exit(1)
        
        # Setup Valkey
        setup_database(valkey_client, "Valkey")
        
        print("\n" + "="*60)
        print("SUCCESS: Both databases are set up with the same data!")
        print("="*60)
        
    finally:
        # Stop Redis container
        print("\nStopping Redis Stack container...")
        os.system("docker stop test-redis-search")
        os.system("docker rm test-redis-search")

if __name__ == "__main__":
    main()
