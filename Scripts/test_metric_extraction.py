import pandas as pd

# Test the get_metric_value function logic
def test_metric_extraction():
    """Test the metric extraction logic"""
    
    # Sample data that mimics the processed DataFrame
    sample_data = {
        'CONCENTRIC_DURATION_Trial_ms': 0.744,
        'ECCENTRIC_BRAKING_RFD_Trial_N_s_': 280.0,
        'MEAN_ECCENTRIC_FORCE_Asym_N': 0.33,
        'MEAN_TAKEOFF_FORCE_Asym_N': 10.12,
        'PEAK_CONCENTRIC_FORCE_Asym_N': 8.94,
        'PEAK_CONCENTRIC_FORCE_Trial_N': 1098.99,
        'PEAK_ECCENTRIC_FORCE_Asym_N': 6.36,
        'RELATIVE_PEAK_CONCENTRIC_FORCE_Trial_N_kg': 12.18
    }
    
    best_trial_series = pd.Series(sample_data)
    print("Available metrics:")
    for metric in best_trial_series.index:
        print(f"  {metric}: {best_trial_series[metric]}")
    
    def get_metric_value(metric_substring):
        found_metric = next((m for m in best_trial_series.index if metric_substring in m), None)
        value = best_trial_series.get(found_metric)
        return pd.to_numeric(value, errors='coerce') if value is not None else None
    
    print(f"\nTesting get_metric_value('CONCENTRIC_DURATION'):")
    result = get_metric_value('CONCENTRIC_DURATION')
    print(f"  Result: {result}")
    
    print(f"\nTesting get_metric_value('ECCENTRIC_BRAKING_RFD'):")
    result = get_metric_value('ECCENTRIC_BRAKING_RFD')
    print(f"  Result: {result}")
    
    print(f"\nTesting get_metric_value('PEAK_CONCENTRIC_FORCE'):")
    result = get_metric_value('PEAK_CONCENTRIC_FORCE')
    print(f"  Result: {result}")

if __name__ == "__main__":
    test_metric_extraction() 