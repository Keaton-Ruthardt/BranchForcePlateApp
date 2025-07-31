import pandas as pd
import numpy as np

# Simulate the UNIT_MAP from the original script
UNIT_MAP = {
    "Newton": "N",
    "Millisecond": "ms",
    "Second": "s",
    "Percent": "%",
    "Kilo": "kg",
    "Pound": "lb",
    "No Unit": "",
    "Newton Per Second": "N/s",
    "Newton Per Kilo": "N/kg",
    "Newton Per Second Per Kilo": "N/s/kg",
    "Centimeter": "cm",
    "Inch": "in"
}

def process_json_to_pivoted_df(test_data_json):
    """Takes the raw JSON from a test result and pivots it into a DataFrame."""
    if not test_data_json or not isinstance(test_data_json, list):
        return None
    
    # DEBUG: Check for CONCENTRIC_DURATION in raw JSON
    concentric_duration_found = False
    for trial in test_data_json:
        results = trial.get("results", [])
        for res in results:
            if "CONCENTRIC_DURATION" in res["definition"].get("result", ""):
                concentric_duration_found = True
                print(f"DEBUG: Found CONCENTRIC_DURATION in raw JSON:")
                print(f"  result: {res['definition'].get('result')}")
                print(f"  unit: {res['definition'].get('unit')}")
                print(f"  value: {res.get('value')}")
                print(f"  limb: {res.get('limb')}")
    
    # DEBUG: Print all available metrics in the raw JSON
    print("DEBUG: All available metrics in raw JSON:")
    all_metrics = set()
    for trial in test_data_json:
        results = trial.get("results", [])
        for res in results:
            result_name = res["definition"].get("result", "")
            unit = res["definition"].get("unit", "")
            limb = res.get("limb", "")
            all_metrics.add(f"{result_name}_{limb}_{unit}")
    
    for metric in sorted(all_metrics):
        print(f"  {metric}")
    print("=== END RAW JSON DEBUG ===")
    
    all_results = []
    for trial in test_data_json:
        results = trial.get("results", [])
        for res in results:
            unit_raw = res["definition"].get("unit", "")
            unit = UNIT_MAP.get(unit_raw, unit_raw)
            limb = res.get("limb")
            result_key = res["definition"].get("result", "")
            # Fix: Avoid duplicate 'Trial' in metric_id
            if limb == "Trial":
                metric_id = f"{result_key}_Trial_{unit}"
            else:
                metric_id = f"{result_key}_{limb}_Trial_{unit}"
            flat_result = {
                "value": res.get("value"),
                "limb": limb,
                "result_key": result_key,
                "unit": unit,
                "metric_id": metric_id
            }
            all_results.append(flat_result)
    if not all_results:
        return None
    df = pd.DataFrame(all_results)
    df['trial'] = df.groupby('metric_id').cumcount() + 1
    pivot = df.pivot_table(index='metric_id', columns='trial', values='value', aggfunc='first')
    pivot.columns = [f'trial {c}' for c in pivot.columns]
    return pivot.reset_index()

def test_concentric_duration_processing():
    """Test the CONCENTRIC_DURATION processing with sample data"""
    
    # Sample JSON data based on the ExampleResultsPPU.csv
    sample_json = [
        {
            "results": [
                {
                    "definition": {
                        "result": "CONCENTRIC_DURATION",
                        "unit": "Millisecond"
                    },
                    "value": 0.7439999999995877,
                    "limb": "Trial"
                },
                {
                    "definition": {
                        "result": "BEAKING_CONCENTRIC_DURATION_RATIO",
                        "unit": "Percent"
                    },
                    "value": 80.0,
                    "limb": "Trial"
                },
                {
                    "definition": {
                        "result": "ECCENTRIC_BRAKING_RFD",
                        "unit": "Newton Per Second"
                    },
                    "value": 280.068728522492,
                    "limb": "Trial"
                },
                {
                    "definition": {
                        "result": "PEAK_CONCENTRIC_FORCE",
                        "unit": "Newton"
                    },
                    "value": 1098.9907663896583,
                    "limb": "Trial"
                }
            ]
        }
    ]
    
    print("Testing CONCENTRIC_DURATION processing...")
    result_df = process_json_to_pivoted_df(sample_json)
    
    if result_df is not None:
        print(f"\nProcessed DataFrame:")
        print(result_df)
        
        # Simulate the processing logic from the main script
        result_df.set_index('metric_id', inplace=True)
        print(f"\nDataFrame with metric_id as index:")
        print(result_df)
        
        # Simulate the string replacement
        result_df.index = result_df.index.str.replace('/', '_s_').str.replace('.', '_')
        print(f"\nDataFrame after string replacement:")
        print(result_df)
        
        # Test the get_metric_value function with the fix
        def get_metric_value(metric_substring):
            # For CONCENTRIC_DURATION, we want the exact metric, not ratios
            if metric_substring == 'CONCENTRIC_DURATION':
                found_metric = next((m for m in result_df.index if m == 'CONCENTRIC_DURATION_Trial_ms'), None)
            else:
                found_metric = next((m for m in result_df.index if metric_substring in m), None)
            value = result_df.loc[found_metric, 'trial 1'] if found_metric else None
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
    else:
        print("Processing returned None")

if __name__ == "__main__":
    test_concentric_duration_processing() 