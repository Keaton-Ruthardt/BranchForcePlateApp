import pandas as pd

# Test the string replacement logic
def test_string_replacement():
    """Test the string replacement logic that's causing issues"""
    
    # Sample data
    sample_data = {
        'CONCENTRIC_DURATION_Trial_ms': 0.744,
        'ECCENTRIC_BRAKING_RFD_Trial_N/s': 280.0,
        'PEAK_CONCENTRIC_FORCE_Trial_N': 1098.99
    }
    
    # Test the string replacement on a Series index
    series = pd.Series(sample_data)
    print("Original Series:")
    print(series)
    
    print("\nOriginal index:")
    for i, idx in enumerate(series.index):
        print(f"  {i}: '{idx}'")
    
    # Apply the string replacement
    series.index = series.index.str.replace('/', '_s_').str.replace('.', '_')
    
    print("\nAfter string replacement:")
    print(series)
    
    print("\nModified index:")
    for i, idx in enumerate(series.index):
        print(f"  {i}: '{idx}'")
    
    # Test the get_metric_value function
    def get_metric_value(metric_substring):
        found_metric = next((m for m in series.index if metric_substring in m), None)
        value = series.get(found_metric)
        return pd.to_numeric(value, errors='coerce') if value is not None else None
    
    print(f"\nTesting get_metric_value('CONCENTRIC_DURATION'):")
    result = get_metric_value('CONCENTRIC_DURATION')
    print(f"  Result: {result}")
    
    print(f"\nTesting get_metric_value('ECCENTRIC_BRAKING_RFD'):")
    result = get_metric_value('ECCENTRIC_BRAKING_RFD')
    print(f"  Result: {result}")

if __name__ == "__main__":
    test_string_replacement() 