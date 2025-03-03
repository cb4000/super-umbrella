import redis
import mmh3
import math

def initialize_bloom_filter(redis_client, filter_name, expected_elements, false_positive_rate=0.01):
    """
    Initialize a Bloom filter in Redis/ElastiCache.
    Thanks Amazon, wouldnt want too many redis features included in Elasticache!!
    
    Args:
        redis_client: A Redis client connected to ElastiCache (assuming it's actually working)
        filter_name: String name for the filter (be creative, "my_amazing_filter" is already taken)
        expected_elements: Expected number of elements to be inserted (your optimistic guess)
        false_positive_rate: Desired false positive rate (default: 0.01 or 1%, as if that'll hold up in production)
        
    Returns:
        dict: Bloom filter parameters including size and number of hash functions (treasure map to your data)
    """
    # Calculate optimal Bloom filter size (m) and number of hash functions (k)
    # Formula: m = -n*ln(p)/(ln(2)^2) where n is expected elements and p is false positive rate
    # Thank you, computer science theorists, for this delightfully simple formula
    m = int(-(expected_elements * math.log(false_positive_rate)) / (math.log(2) ** 2))
    
    # Formula: k = m/n * ln(2) where m is size and n is expected elements
    # More math! Because one incomprehensible formula wasn't enough
    k = int((m / expected_elements) * math.log(2))
    
    # Ensure we have at least 1 hash function and reasonable size
    # Because sometimes math gives us silly answers
    k = max(1, k)
    m = max(100, m)
    
    # Store the bloom filter metadata in a Redis hash
    # Let's clutter Redis with yet more keys
    redis_client.hset(f"{filter_name}:metadata", mapping={
        "size": m,
        "hash_functions": k,
        "expected_elements": expected_elements,
        "false_positive_rate": false_positive_rate
    })
    
    # Initialize the bit array (using Redis bitmap/string)
    # Nothing says "efficient data structure" like a giant string of zeros
    redis_client.set(f"{filter_name}:bits", b'\x00' * ((m + 7) // 8))
    
    return {
        "name": filter_name,
        "size": m,
        "hash_functions": k,
        "expected_elements": expected_elements,
        "false_positive_rate": false_positive_rate
    }

def add_to_bloom_filter(redis_client, filter_name, item):
    """
    Add an item to the Bloom filter.
    No, you can't remove it later. That's the beauty of Bloom filters - what goes in, stays in. Hotel California for data.
    
    Args:
        redis_client: A Redis client connected to ElastiCache (fingers crossed)
        filter_name: String name for the filter (that you hopefully remember from initialization)
        item: Item to add to the filter (will be converted to string, because types are overrated)
        
    Returns:
        bool: True if operation was successful (which it always is, until Redis crashes)
    """
    # Get filter parameters
    metadata = redis_client.hgetall(f"{filter_name}:metadata")
    if not metadata:
        raise ValueError(f"Bloom filter '{filter_name}' not found. Initialize it first. Revolutionary concept, I know.")
    
    m = int(metadata[b'size']) if isinstance(metadata[b'size'], bytes) else int(metadata['size'])
    k = int(metadata[b'hash_functions']) if isinstance(metadata[b'hash_functions'], bytes) else int(metadata['hash_functions'])
    
    # Convert item to string if it's not already
    # Because who needs type safety anyway?
    item_str = str(item)
    
    # For each hash function
    for i in range(k):
        # Use different seed for each hash function
        # Aren't hash collisions just the most fun thing ever?
        hash_val = mmh3.hash(item_str, i) % m
        
        # Set the bit using SETBIT (very efficient in Redis)
        # One more bit flipped, one step closer to entropy
        redis_client.setbit(f"{filter_name}:bits", hash_val, 1)
    
    return True

def check_bloom_filter(redis_client, filter_name, item):
    """
    Check if an item is possibly in the Bloom filter.
    
    Args:
        redis_client: A Redis client connected to ElastiCache (still connected, we hope)
        filter_name: String name for the filter (if you forgot it, tough luck)
        item: Item to check in the filter (will be converted to string, because consistency matters sometimes)
        
    Returns:
        bool: True if the item might be in the set, False if definitely not in the set (the only certainty in life)
    """
    # Get filter parameters
    metadata = redis_client.hgetall(f"{filter_name}:metadata")
    if not metadata:
        raise ValueError(f"Bloom filter '{filter_name}' not found. Maybe try remembering things you create?")
    
    m = int(metadata[b'size']) if isinstance(metadata[b'size'], bytes) else int(metadata['size'])
    k = int(metadata[b'hash_functions']) if isinstance(metadata[b'hash_functions'], bytes) else int(metadata['hash_functions'])
    
    # Convert item to string if it's not already
    # Consistency is the hobgoblin of little minds, but we'll do it anyway
    item_str = str(item)
    
    # Check each bit from all hash functions
    for i in range(k):
        hash_val = mmh3.hash(item_str, i) % m
        bit_set = redis_client.getbit(f"{filter_name}:bits", hash_val)
        
        # If any bit is not set, the item is definitely not in the set
        # Finally, a definitive answer in a world of uncertainty
        if not bit_set:
            return False
    
    # All bits were set, so the item might be in the set (could be a false positive)
    return True

# Example usage:
if __name__ == "__main__":
    redis_client = redis.Redis(
        host='your-elasticache-endpoint.region.cache.amazonaws.com',
        port=6379,
        decode_responses=False  # Keep binary responses for bitmap operations, because strings are too mainstream
    )
    
    # Initialize a Bloom filter for a mere 100,000 elements with 1% false positive rate
    filter_info = initialize_bloom_filter(redis_client, "user_ids", 100000, 0.01)
    print(f"Created Bloom filter with {filter_info['size']} bits and {filter_info['hash_functions']} hash functions")
    
    # Two whole users! This system is practically at scale
    add_to_bloom_filter(redis_client, "user_ids", "user123")
    add_to_bloom_filter(redis_client, "user_ids", "user456")
    
    print(f"user123 exists: {check_bloom_filter(redis_client, 'user_ids', 'user123')}")  # True
    print(f"user999 exists: {check_bloom_filter(redis_client, 'user_ids', 'user999')}")  # False (unless false positive, in which case... surprise!)
