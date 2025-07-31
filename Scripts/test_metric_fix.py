"""
Test script to verify metric name fixes
"""

import pandas as pd
from VALDapiHelpers import get_access_token, get_profiles, FD_Tests_by_Profile, get_FD_results

def test_metric_fix():
    try:
        print("Testing metric name fixes...")
        
        # Get token
        token = get_access_token()
        
        # Get profiles
        profiles = get_profiles(token)
        if profiles.empty:
            print("No profiles found")
            return
        
        # Get first athlete with CMJ tests
        for index, athlete in profiles.head(3).iterrows():
            profile_id = str(athlete['profileId'])
            athlete_name = str(athlete['fullName'])
            print(f"\nTesting athlete: {athlete_name}")
            
            # Get tests
            start_date = "2021-1-1 00:00:00"
            tests_df = FD_Tests_by_Profile(start_date, profile_id, token)
            
            if tests_df is not None and not tests_df.empty:
                cmj_tests = tests_df[tests_df['testType'] == 'CMJ']
                if not cmj_tests.empty:
                    test_id = cmj_tests.iloc[0]['testId']
                    print(f"Found CMJ test: {test_id}")
                    
                    # Get raw data
                    raw_data = get_FD_results(test_id, token)
                    if raw_data is not None and not raw_data.empty:
                        print(f"Total metrics in raw data: {len(raw_data)}")
                        
                        # Test the fixed metric names
                        fixed_metrics = [
                            'CON_P2_CON_P1_IMPULSE_RATIO_Trial',  # Fixed: removed underscore
                            'CONCENTRIC_RFD_Trial_N/s',           # Fixed: added slash
                        ]
                        
                        print("\nTesting fixed metric names:")
                        for metric in fixed_metrics:
                            if metric in raw_data['metric_id'].values:
                                print(f"✓ Found: {metric}")
                                # Show the actual value
                                metric_data = raw_data[raw_data['metric_id'] == metric]
                                if not metric_data.empty:
                                    trial_cols = [col for col in metric_data.columns if 'trial' in col.lower()]
                                    if trial_cols:
                                        value = metric_data.iloc[0][trial_cols[0]]
                                        print(f"  Value: {value}")
                            else:
                                print(f"✗ Not found: {metric}")
                        
                        break
                    else:
                        print("No raw data found")
                else:
                    print("No CMJ tests found")
            else:
                print("No tests found")
    except Exception as e:
        print(f"Error in test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_metric_fix() 