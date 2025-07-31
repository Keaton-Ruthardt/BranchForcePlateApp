#!/usr/bin/env python3
"""
Script to add James McArthur (athlete_id: 0000004) to the BigQuery database
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq

# Import VALD API helpers
from VALDapiHelpers import get_access_token, get_profiles, FD_Tests_by_Profile, get_FD_results
import uuid

# Configuration
PROJECT_ID = "vald-ref-data"
DATASET_ID = "athlete_performance_db"
TABLE_ID = "cmj_results"
CREDENTIALS_FILE = 'gcp_credentials.json'
JAMES_ATHLETE_ID = "0000004"

def process_cmj_test(results_df, athlete_id, athlete_name, test_date, test_id, assessment_id):
    """
    Simple CMJ test processor for individual athlete data ingestion
    """
    try:
        if results_df.empty:
            return None
        
        # Define the CMJ metrics we want to extract
        cmj_metrics = [
            'CONCENTRIC_IMPULSE_Trial_Ns',
            'ECCENTRIC_BRAKING_RFD_Trial_N_s', 
            'PEAK_CONCENTRIC_FORCE_Trial_N',
            'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',
            'RSI_MODIFIED_Trial_RSI_mod',
            'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns',
            'BODY_WEIGHT_LBS_Trial_lb',
            'CONCENTRIC_DURATION_Trial_ms',
            'CONCENTRIC_RFD_Trial_N_s',
            'JUMP_HEIGHT_IMP_MOM_Trial_cm',
            'PEAK_TAKEOFF_POWER_Trial_W',
            'CONCENTRIC_IMPULSE_P1_Trial_Ns',
            'CONCENTRIC_IMPULSE_P2_Trial_Ns',
            'RSI_MODIFIED_IMP_MOM_Trial_RSI_mod',
            'CON_P2_CON_P1_IMPULSE_RATIO_Trial',
            'CONCENTRIC_IMPULSE_Asym_Ns',
            'ECCENTRIC_BRAKING_IMPULSE_Asym_Ns',
            'CONCENTRIC_IMPULSE_P1_Asym_Ns',
            'CONCENTRIC_IMPULSE_P2_Asym_Ns'
        ]
        
        # Filter for CMJ metrics
        cmj_data = results_df[results_df['metric_id'].isin(cmj_metrics)]
        
        if cmj_data.empty:
            print(f"No CMJ metrics found in test {test_id}")
            return None
        
        # Get trial columns
        trial_cols = [col for col in cmj_data.columns if 'trial' in col.lower()]
        if not trial_cols:
            print(f"No trial data found in test {test_id}")
            return None
            
        # Create pivot table with metrics as index and trials as columns
        pivot_data = cmj_data.set_index('metric_id')[trial_cols]
        
        # Find the best trial (trial with most non-null values)
        trial_scores = {}
        for trial_col in trial_cols:
            valid_count = pivot_data[trial_col].notna().sum()
            trial_scores[trial_col] = valid_count
        
        best_trial = max(trial_scores, key=trial_scores.get)
        best_trial_data = pivot_data[best_trial]
        
        # Create result dictionary
        result_dict = {}
        for metric in cmj_metrics:
            if metric in best_trial_data.index:
                result_dict[metric] = best_trial_data[metric]
            else:
                result_dict[metric] = np.nan
        
        # Add metadata
        result_dict['result_id'] = str(uuid.uuid4())
        result_dict['assessment_id'] = assessment_id
        result_dict['athlete_id'] = athlete_id
        result_dict['athlete_name'] = athlete_name
        result_dict['test_date'] = pd.to_datetime(test_date).date()
        result_dict['age_at_test'] = None  # Will need to be calculated if DOB is available
        result_dict['cmj_composite_score'] = 75.0  # Placeholder composite score
        
        return pd.DataFrame([result_dict])
        
    except Exception as e:
        print(f"Error processing CMJ test {test_id}: {e}")
        return None

def setup_bigquery():
    """Setup BigQuery client"""
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        print("BigQuery client setup successful")
        return bq_client, credentials
    except Exception as e:
        print(f"Failed to setup BigQuery: {e}")
        return None, None

def find_james_profile(token):
    """Find James McArthur's profile in VALD API"""
    try:
        print("Searching for James McArthur in VALD profiles...")
        profiles_df = get_profiles(token)
        
        if profiles_df.empty:
            print("No profiles found")
            return None
        
        # Debug: Print available columns (can be removed later)
        # print(f"Available columns in profiles: {list(profiles_df.columns)}")
        # print(f"Sample profile data:")
        # print(profiles_df.head(1))
            
        # Search for James McArthur by external_id or name
        james_profiles = profiles_df[
            (profiles_df['externalId'].astype(str).str.contains(JAMES_ATHLETE_ID, na=False)) |
            (profiles_df['givenName'].str.contains('James', case=False, na=False)) |
            (profiles_df['familyName'].str.contains('McArthur', case=False, na=False)) |
            (profiles_df['fullName'].str.contains('James', case=False, na=False)) |
            (profiles_df['fullName'].str.contains('McArthur', case=False, na=False))
        ]
        
        print(f"Found {len(james_profiles)} potential matches:")
        for idx, profile in james_profiles.iterrows():
            print(f"  - {profile['fullName']} (External ID: {profile['externalId']})")
        
        # Look specifically for athlete_id 0000004 in externalId
        exact_match = profiles_df[profiles_df['externalId'].astype(str) == JAMES_ATHLETE_ID]
        if not exact_match.empty:
            profile = exact_match.iloc[0]
            print(f"Found exact match: {profile['fullName']} (External ID: {profile['externalId']})")
            return profile
        
        # If no exact match, show all James profiles for manual selection
        if not james_profiles.empty:
            print("No exact match for external_id 0000004, but found James profiles above")
            return james_profiles.iloc[0]  # Return first match
            
        print("James McArthur not found in profiles")
        return None
        
    except Exception as e:
        print(f"Error searching profiles: {e}")
        return None

def get_james_cmj_data(profile, token):
    """Get CMJ test data for James McArthur"""
    try:
        profile_id = profile['profileId']
        athlete_id = profile['externalId']
        athlete_name = profile['fullName']
        
        print(f"Getting CMJ data for {athlete_name} (Profile: {profile_id})")
        
        # Get tests from the last 2 years to ensure we get all data
        start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        
        tests_df = FD_Tests_by_Profile(start_date, profile_id, token)
        
        if tests_df.empty:
            print("No tests found for this profile")
            return pd.DataFrame()
        
        print(f"Found {len(tests_df)} tests")
        
        # Filter for CMJ tests
        cmj_tests = tests_df[tests_df['testType'] == 'CMJ']
        if cmj_tests.empty:
            print("No CMJ tests found")
            return pd.DataFrame()
            
        print(f"Found {len(cmj_tests)} CMJ tests")
        
        # Debug: Print available columns in test data (can be removed later)
        # print(f"Test data columns: {list(cmj_tests.columns)}")
        # print("Sample test data:")
        # print(cmj_tests.head(1))
        
        all_results = []
        
        for idx, test in cmj_tests.iterrows():
            try:
                test_id = test['testId']
                test_date = test['modifiedDateUtc']
                assessment_id = test.get('assessmentId', '')
                
                print(f"  Processing test {test_id} from {test_date}")
                
                # Get detailed results for this test
                results_df = get_FD_results(test_id, token)
                
                if results_df.empty:
                    print(f"    No results for test {test_id}")
                    continue
                
                # Process the CMJ test data
                processed_data = process_cmj_test(results_df, athlete_id, athlete_name, test_date, test_id, assessment_id)
                
                if processed_data is not None:
                    all_results.append(processed_data)
                    print(f"    Processed test {test_id}")
                else:
                    print(f"    Failed to process test {test_id}")
                    
            except Exception as e:
                print(f"    Error processing test {test_id}: {e}")
                continue
        
        if all_results:
            final_df = pd.concat(all_results, ignore_index=True)
            print(f"Successfully processed {len(final_df)} CMJ results for {athlete_name}")
            return final_df
        else:
            print("No results could be processed")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"Error getting CMJ data: {e}")
        return pd.DataFrame()

def upload_to_bigquery(df, credentials):
    """Upload data to BigQuery"""
    try:
        if df.empty:
            print("No data to upload")
            return False
            
        print(f"Uploading {len(df)} records to BigQuery...")
        
        # Ensure proper data types
        df = df.copy()
        
        # Convert date columns
        if 'test_date' in df.columns:
            df['test_date'] = pd.to_datetime(df['test_date']).dt.date
        
        # Convert numeric columns
        numeric_columns = [col for col in df.columns if col not in ['result_id', 'assessment_id', 'athlete_id', 'athlete_name', 'test_date']]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Upload to BigQuery
        pandas_gbq.to_gbq(
            df,
            destination_table=f'{DATASET_ID}.{TABLE_ID}',
            project_id=PROJECT_ID,
            credentials=credentials,
            if_exists='append',
            table_schema=None
        )
        
        print(f"Successfully uploaded {len(df)} records to BigQuery")
        return True
        
    except Exception as e:
        print(f"Error uploading to BigQuery: {e}")
        return False

def verify_upload(bq_client):
    """Verify James McArthur was added to the database"""
    try:
        query = f"""
        SELECT athlete_id, athlete_name, COUNT(*) as test_count, MIN(test_date) as first_test, MAX(test_date) as last_test
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE athlete_name = 'James McArthur'
        GROUP BY athlete_id, athlete_name
        """
        
        df = bq_client.query(query).to_dataframe()
        
        if df.empty:
            print("James McArthur not found in database after upload")
            return False
        else:
            for idx, row in df.iterrows():
                print(f"Verified: {row['athlete_name']} (ID: {row['athlete_id']}) - {row['test_count']} tests ({row['first_test']} to {row['last_test']})")
            return True
            
    except Exception as e:
        print(f"Error verifying upload: {e}")
        return False

def main():
    print("Starting James McArthur data ingestion...")
    
    # Setup BigQuery
    bq_client, credentials = setup_bigquery()
    if not bq_client:
        return False
    
    # Get VALD API token
    try:
        token = get_access_token()
        print("VALD API token obtained")
    except Exception as e:
        print(f"Failed to get API token: {e}")
        return False
    
    # Find James McArthur's profile
    profile = find_james_profile(token)
    if profile is None:
        return False
    
    # Get his CMJ data
    cmj_data = get_james_cmj_data(profile, token)
    if cmj_data.empty:
        print("No CMJ data found for James McArthur")
        return False
    
    print(f"Retrieved data shape: {cmj_data.shape}")
    print("Columns:", list(cmj_data.columns))
    
    # Upload to BigQuery
    success = upload_to_bigquery(cmj_data, credentials)
    if not success:
        return False
    
    # Verify upload
    return verify_upload(bq_client)

if __name__ == "__main__":
    success = main()
    if success:
        print("James McArthur has been successfully added to the database!")
        sys.exit(0)
    else:
        print("Failed to add James McArthur to the database")
        sys.exit(1)