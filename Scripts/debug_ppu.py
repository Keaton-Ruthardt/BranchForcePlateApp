import pandas as pd
import numpy as np
import json

# Import your existing helper functions
from token_generator import get_access_token
from VALDapiHelpers import get_profiles, FD_Tests_by_Profile, FORCEDECKS_URL, TENANT_ID

# Test the process_json_to_pivoted_df function with sample data
def test_concentric_duration_processing():
    """Test function to debug CONCENTRIC_DURATION processing"""
    
    # Sample JSON data structure that might represent CONCENTRIC_DURATION
    sample_json = [
        {
            "results": [
                {
                    "definition": {
                        "result": "CONCENTRIC_DURATION",
                        "unit": "Millisecond"
                    },
                    "value": 150.5,
                    "limb": "Trial"
                },
                {
                    "definition": {
                        "result": "CONCENTRIC_DURATION", 
                        "unit": "Millisecond"
                    },
                    "value": 145.2,
                    "limb": "Trial"
                }
            ]
        }
    ]
    
    print("Testing with sample CONCENTRIC_DURATION data:")
    print(f"Sample JSON: {json.dumps(sample_json, indent=2)}")
    
    # Test the processing function
    from process_ppu import process_json_to_pivoted_df
    result_df = process_json_to_pivoted_df(sample_json)
    
    if result_df is not None:
        print(f"\nProcessed DataFrame:")
        print(result_df)
        
        # Check if CONCENTRIC_DURATION is in the index
        if 'CONCENTRIC_DURATION' in str(result_df['metric_id'].values):
            print(f"\nFound CONCENTRIC_DURATION metrics:")
            concentric_metrics = result_df[result_df['metric_id'].str.contains('CONCENTRIC_DURATION')]
            print(concentric_metrics)
    else:
        print("Processing returned None")

if __name__ == "__main__":
    test_concentric_duration_processing() 