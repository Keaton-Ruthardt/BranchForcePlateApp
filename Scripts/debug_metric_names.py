"""
Debug script to check exact metric names from VALD API
"""

import pandas as pd
from VALDapiHelpers import get_access_token, get_profiles, FD_Tests_by_Profile, get_FD_results

def debug_metric_names():
    try:
        print("Starting debug script...")
        
        # Get token
        print("Getting access token...")
        token = get_access_token()
        print("Token obtained successfully")
        
        # Get profiles
        print("Getting profiles...")
        profiles = get_profiles(token)
        if profiles.empty:
            print("No profiles found")
            return
        
        print(f"Found {len(profiles)} profiles")
        
        # Get first athlete with CMJ tests
        for index, athlete in profiles.head(5).iterrows():
            profile_id = str(athlete['profileId'])
            athlete_name = str(athlete['fullName'])
            print(f"\nChecking athlete: {athlete_name}")
            
            # Get tests
            start_date = "2021-1-1 00:00:00"
            print(f"Getting tests for profile {profile_id}...")
            tests_df = FD_Tests_by_Profile(start_date, profile_id, token)
            
            if tests_df is not None and not tests_df.empty:
                print(f"Found {len(tests_df)} tests")
                cmj_tests = tests_df[tests_df['testType'] == 'CMJ']
                if not cmj_tests.empty:
                    test_id = cmj_tests.iloc[0]['testId']
                    print(f"Found CMJ test: {test_id}")
                    
                    # Get raw data
                    print("Getting raw data...")
                    raw_data = get_FD_results(test_id, token)
                    if raw_data is not None and not raw_data.empty:
                        print(f"Total metrics in raw data: {len(raw_data)}")
                        
                        # Check for our specific metrics
                        target_metrics = [
                            'CON_P2_CON_P1_IMPULSE_RATIO_Trial_',
                            'CON_P2_CON_P1_IMPULSE_RATIO_Trial',
                            'CONCENTRIC_RFD_Trial_N_s',
                            'CONCENTRIC_RFD_Trial_N/s',
                            'CONCENTRIC_RFD_Trial_N_s_'
                        ]
                        
                        print("\nChecking for target metrics:")
                        for metric in target_metrics:
                            if metric in raw_data['metric_id'].values:
                                print(f"✓ Found: {metric}")
                            else:
                                print(f"✗ Not found: {metric}")
                        
                        # Show all metrics containing our keywords
                        print("\nAll metrics containing 'CON_P2_CON_P1_IMPULSE_RATIO':")
                        impulse_ratio_metrics = raw_data[raw_data['metric_id'].str.contains('CON_P2_CON_P1_IMPULSE_RATIO', na=False)]
                        for _, row in impulse_ratio_metrics.iterrows():
                            print(f"  {row['metric_id']}")
                        
                        print("\nAll metrics containing 'CONCENTRIC_RFD':")
                        rfd_metrics = raw_data[raw_data['metric_id'].str.contains('CONCENTRIC_RFD', na=False)]
                        for _, row in rfd_metrics.iterrows():
                            print(f"  {row['metric_id']}")
                        
                        break
                    else:
                        print("No raw data found")
                else:
                    print("No CMJ tests found")
            else:
                print("No tests found")
    except Exception as e:
        print(f"Error in debug script: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_metric_names() 