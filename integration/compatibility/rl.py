#!/usr/bin/env python3
"""
Standalone script to test Redis with search queries.
Uses redis/redis-stack-server Docker container.
"""
import redis
import time
import os
import sys

# Data
INDEX_NAME = "hash_idx1"
CREATE_INDEX_CMD = [
    "FT.CREATE", INDEX_NAME, "ON", "HASH", 
    "PREFIX", "1", "hash:", 
    "SCHEMA", 
    "title", "TEXT",
    "body", "TEXT",
    "color", "TAG",
    "price", "NUMERIC"
]

# Configuration - CHANGE QUERY HERE
DIALECT = 2
QUERY = 'three four (two ocean carrot) one'
search_cmd = ["FT.SEARCH", INDEX_NAME, QUERY, "SLOP", "", "DIALECT", str(DIALECT)]

HASH_DATA = [
    ('hash:00', {'title': 'plum', 'body': 'cat slow loud shark ocean eagle tomato', 'color': 'green', 'price': '21'}),
    ('hash:01', {'title': 'kiwi peach apple chair orange door orange melon chair', 'body': 'lettuce', 'color': 'green', 'price': '8'}),
    ('hash:02', {'title': 'plum', 'body': 'river cat slow build eagle fast dog', 'color': 'brown', 'price': '40'}),
    ('hash:03', {'title': 'window smooth apple silent movie chair window puzzle door', 'body': 'desert city desert slow jump drive lettuce forest', 'color': 'blue', 'price': '10'}),
    ('hash:04', {'title': 'kiwi lemon orange chair door kiwi', 'body': 'river fast eagle loud', 'color': 'purple', 'price': '25'}),
    ('hash:05', {'title': 'lamp quick banana plum desk game story window sharp', 'body': 'cold village fly', 'color': 'red', 'price': '0'}),
    ('hash:06', {'title': 'chair apple puzzle', 'body': 'warm jump potato run desert', 'color': 'yellow', 'price': '5'}),
    ('hash:07', {'title': 'silent puzzle lemon window movie apple melon', 'body': 'potato ocean city potato jump carrot warm tomato', 'color': 'green', 'price': '23'}),
    ('hash:08', {'title': 'game quick music game', 'body': 'ocean carrot jump quiet build shark onion', 'color': 'black', 'price': '33'}),
    ('hash:09', {'title': 'music quick', 'body': 'city fly village potato village fly drive', 'color': 'orange', 'price': '19'}),
    ('hash:10', {'title': 'game quick music game', 'body': 'ocean carrot jump quiet build shark onion one two three four five six', 'color': 'black', 'price': '33'}),
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

def stop_redis_container():
    """Stop and remove Redis Stack container"""
    print("\nStopping Redis Stack container...")
    os.system("docker stop test-redis-search")
    os.system("docker rm test-redis-search")

def main():
    start_redis_container()
    
    try:
        # Connect to Redis
        client = redis.Redis(host='localhost', port=6380, decode_responses=False)
        
        # Wait for connection
        for i in range(30):
            try:
                client.ping()
                print("Connected to Redis!")
                break
            except redis.ConnectionError:
                if i == 29:
                    print("Failed to connect to Redis")
                    sys.exit(1)
                time.sleep(0.5)
        
        # Flush all data
        print("\nFlushing all data...")
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
        
        # Execute search query
        print(f"\n{'='*60}")
        print(f"Executing query: {QUERY}")
        print(f"{'='*60}")
        print(len(search_cmd))
        print(search_cmd)
        print(f"Full command: {' '.join(search_cmd)}")
        
        result = client.execute_command(*search_cmd)
        print(f"\nResult: {result}")
        
        # Parse and display results
        if isinstance(result, list) and len(result) > 0:
            count = result[0]
            print(f"\nFound {count} results")
            
            if count > 0:
                print("\nDetailed results:")
                for i in range(1, len(result), 2):
                    key = result[i]
                    fields = result[i+1] if i+1 < len(result) else []
                    print(f"\n  Key: {key}")
                    if isinstance(fields, list):
                        for j in range(0, len(fields), 2):
                            field_name = fields[j]
                            field_value = fields[j+1] if j+1 < len(fields) else ''
                            print(f"    {field_name}: {field_value}")
        else:
            print(f"\nNo results or unexpected format: {result}")
        
    finally:
        stop_redis_container()

if __name__ == "__main__":
    main()