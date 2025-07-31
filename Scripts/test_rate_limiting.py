"""
Test script to verify rate limiting is working properly.
This script will test the API calls with rate limiting to ensure we don't get 429 errors.
"""

import time
import logging
from enhanced_cmj_processor import rate_limited_request, get_FD_results_with_logging_and_retry, shared_token, token_lock
from VALDapiHelpers import get_access_token

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def test_rate_limiting():
    """Test the rate limiting functionality."""
    
    # Get a fresh token
    with token_lock:
        shared_token['token'] = get_access_token()
    
    # Test a few API calls with rate limiting
    test_ids = [
        "58c89a10-72b6-41b6-88e5-e7dde01bb81c",
        "06b4f40a-9f82-4923-b8f3-1b828a7a35da", 
        "a15da16d-d49b-4227-b1e6-27d851d98d35"
    ]
    
    print("Testing rate limiting with 3 API calls...")
    
    for i, test_id in enumerate(test_ids):
        print(f"\nMaking API call {i+1}/3 for test {test_id}")
        start_time = time.time()
        
        try:
            result = get_FD_results_with_logging_and_retry(test_id, shared_token['token'])
            elapsed = time.time() - start_time
            
            if result is not None and not result.empty:
                print(f"✓ Success: Got data in {elapsed:.2f}s")
                print(f"  Data shape: {result.shape}")
            else:
                print(f"✗ No data returned for test {test_id}")
                
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"✗ Error after {elapsed:.2f}s: {e}")
    
    print("\nRate limiting test completed!")

if __name__ == "__main__":
    test_rate_limiting() 