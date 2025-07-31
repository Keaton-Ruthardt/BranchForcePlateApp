#!/usr/bin/env python3
"""
Script to add James McArthur using the proper pipeline methodology
Follows the same approach as enhanced_cmj_processor.py with global means/stds
"""

import os
import sys
import pandas as pd
import numpy as np
import uuid
from datetime import datetime, timedelta
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging

# Import VALD API helpers and composite scoring
from VALDapiHelpers import get_access_token, get_profiles, FD_Tests_by_Profile, get_FD_results
from newcompositescore import calculate_composite_score_per_trial, get_best_trial, CMJ_weights

# Configuration
PROJECT_ID = "vald-ref-data"
DATASET_ID = "athlete_performance_db"
TABLE_ID = "cmj_results"
CREDENTIALS_FILE = 'gcp_credentials.json'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def get_global_stats_from_all_athletes(token):
    """
    Calculate global means and standard deviations from all athletes in the system
    following the same methodology as enhanced_cmj_processor.py
    """
    print("Calculating global statistics from all athletes...")
    
    # Get all profiles
    profiles = get_profiles(token)
    if profiles.empty:
        raise Exception("No profiles found for global stats calculation")
    
    print(f"Found {len(profiles)} athlete profiles for global stats")
    
    # Gather CMJ trial data for global stats (sample from all athletes)
    all_cmj_trials = []
    skipped_tests = 0
    total_tests_found = 0
    all_test_ids = []
    
    # Use a subset for performance - take every 3rd athlete
    profiles_subset = profiles.iloc[::3]  # Every 3rd athlete
    print(f"Using {len(profiles_subset)} athletes for global statistics calculation")
    
    for index, athlete in profiles_subset.iterrows():
        profile_id = str(athlete['profileId'])
        start_date = "2021-1-1 00:00:00"
        
        try:
            tests_df = FD_Tests_by_Profile(start_date, profile_id, token)
            if tests_df is not None and not tests_df.empty:
                cmj_tests = tests_df[tests_df['testType'] == 'CMJ']
                for _, test_row in cmj_tests.iterrows():
                    total_tests_found += 1
                    all_test_ids.append(test_row['testId'])
                    # Limit to prevent excessive API calls
                    if len(all_test_ids) >= 200:  # Reasonable sample size
                        break
            time.sleep(0.5)  # Rate limiting
            if len(all_test_ids) >= 200:
                break
        except Exception as e:
            logging.warning(f"Error getting tests for profile {profile_id}: {e}")
            continue
    
    print(f"Found {len(all_test_ids)} CMJ tests for global stats calculation")
    
    # Parallel fetch test results for stats
    def fetch_trial_data_for_stats(test_id):
        time.sleep(0.3)  # Rate limiting
        try:
            return get_FD_results(test_id, token)
        except Exception as e:
            logging.warning(f"Error fetching test {test_id}: {e}")
            return None
    
    parallel_cmj_trials = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(fetch_trial_data_for_stats, test_id) for test_id in all_test_ids[:100]]  # Limit to 100 tests
        for future in as_completed(futures):
            raw_data = future.result()
            if raw_data is not None and not raw_data.empty:
                # Filter for CMJ metrics used in composite score
                cmj_metrics = list(CMJ_weights.keys())
                cmj_data = raw_data[raw_data['metric_id'].isin(cmj_metrics)]
                trial_cols = [col for col in cmj_data.columns if 'trial' in col.lower()]
                if trial_cols:
                    pivot_data = cmj_data.set_index('metric_id')[trial_cols]
                    parallel_cmj_trials.append(pivot_data)
                else:
                    skipped_tests += 1
            else:
                skipped_tests += 1
    
    print(f"Successfully processed {len(parallel_cmj_trials)} tests for global stats")
    print(f"Skipped {skipped_tests} tests due to missing data")
    
    # Concatenate all trials into one DataFrame
    if parallel_cmj_trials:
        all_trials_df = pd.concat([df for df in parallel_cmj_trials if not df.empty], axis=1).T
        metrics = list(CMJ_weights.keys())
        
        # Outlier filtering: remove values outside 3 std from mean for each metric
        for metric in metrics:
            if metric in all_trials_df:
                mean = all_trials_df[metric].mean()
                std = all_trials_df[metric].std()
                before_count = len(all_trials_df)
                all_trials_df = all_trials_df[(all_trials_df[metric] >= mean - 3*std) & (all_trials_df[metric] <= mean + 3*std)]
                after_count = len(all_trials_df)
                if before_count != after_count:
                    print(f"Outlier filtering for {metric}: removed {before_count - after_count} outliers")
        
        global_means = all_trials_df[metrics].mean()
        global_stds = all_trials_df[metrics].std()
        
        print("Global statistics calculated:")
        for metric in metrics:
            print(f"  {metric}: mean={global_means[metric]:.3f}, std={global_stds[metric]:.3f}")
        
        return global_means, global_stds
    else:
        raise Exception("No CMJ trial data found for global stats calculation")

def find_james_mcarthur(token):
    """Find James McArthur's profile"""
    print("Searching for James McArthur...")
    profiles_df = get_profiles(token)
    
    if profiles_df.empty:
        raise Exception("No profiles found")
    
    # Search for James McArthur by name
    james_profiles = profiles_df[
        (profiles_df['givenName'].str.contains('James', case=False, na=False)) &
        (profiles_df['familyName'].str.contains('McArthur', case=False, na=False))
    ]
    
    if james_profiles.empty:
        # Try fullName search
        james_profiles = profiles_df[
            profiles_df['fullName'].str.contains('James McArthur', case=False, na=False)
        ]
    
    if james_profiles.empty:
        raise Exception("James McArthur not found in profiles")
    
    profile = james_profiles.iloc[0]  # Take first match
    print(f"Found James McArthur: {profile['fullName']} (Profile: {profile['profileId']})")
    return profile

def process_james_cmj_tests(profile, token, global_means, global_stds):
    """Process James McArthur's CMJ tests using proper methodology"""
    profile_id = profile['profileId']
    athlete_name = profile['fullName']
    athlete_dob = profile.get('dateOfBirth', None)
    
    print(f"Processing CMJ tests for {athlete_name}...")
    
    # Get tests from the last 3 years
    start_date = (datetime.now() - timedelta(days=1095)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    tests_df = FD_Tests_by_Profile(start_date, profile_id, token)
    
    if tests_df is None or tests_df.empty:
        raise Exception("No tests found for James McArthur")
    
    # Filter for CMJ tests
    cmj_tests = tests_df[tests_df['testType'] == 'CMJ']
    if cmj_tests.empty:
        raise Exception("No CMJ tests found for James McArthur")
    
    print(f"Found {len(cmj_tests)} CMJ tests for James McArthur")
    
    processed_results = []
    
    for _, test_row in cmj_tests.iterrows():
        test_id = test_row['testId']
        test_date = pd.to_datetime(test_row['modifiedDateUtc']).date()
        assessment_id = str(uuid.uuid4())
        
        # Calculate age at test
        age_at_test = None
        if athlete_dob is not None and not pd.isnull(athlete_dob):
            try:
                dob = pd.to_datetime(athlete_dob).date()
                age_at_test = test_date.year - dob.year - ((test_date.month, test_date.day) < (dob.month, dob.day))
            except:
                age_at_test = None
        
        print(f"  Processing test {test_id} from {test_date}...")
        
        try:
            # Get raw test data
            raw_data = get_FD_results(test_id, token)
            if raw_data is None or raw_data.empty:
                print(f"    No data found for test {test_id}")
                continue
            
            # Filter for CMJ metrics (all 19 metrics as per your system)
            cmj_metrics = [
                'BODY_WEIGHT_LBS_Trial_lb',
                'CONCENTRIC_DURATION_Trial_ms',
                'CONCENTRIC_IMPULSE_Trial_Ns',
                'CONCENTRIC_RFD_Trial_N_s',
                'ECCENTRIC_BRAKING_RFD_Trial_N_s',
                'JUMP_HEIGHT_IMP_MOM_Trial_cm',
                'PEAK_CONCENTRIC_FORCE_Trial_N',
                'PEAK_TAKEOFF_POWER_Trial_W',
                'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',
                'CONCENTRIC_IMPULSE_P1_Trial_Ns',
                'CONCENTRIC_IMPULSE_P2_Trial_Ns',
                'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns',
                'RSI_MODIFIED_IMP_MOM_Trial_RSI_mod',
                'RSI_MODIFIED_Trial_RSI_mod',
                'CON_P2_CON_P1_IMPULSE_RATIO_Trial',
                'CONCENTRIC_IMPULSE_Asym_Ns',
                'ECCENTRIC_BRAKING_IMPULSE_Asym_Ns',
                'CONCENTRIC_IMPULSE_P1_Asym_Ns',
                'CONCENTRIC_IMPULSE_P2_Asym_Ns'
            ]
            
            cmj_data = raw_data[raw_data['metric_id'].isin(cmj_metrics)]
            if cmj_data.empty:
                print(f"    No CMJ metrics found for test {test_id}")
                continue
            
            # Get trial columns
            trial_cols = [col for col in cmj_data.columns if 'trial' in col.lower()]
            if not trial_cols:
                print(f"    No trial data found for test {test_id}")
                continue
            
            # Create pivot table with metrics as index and trials as columns
            pivot_data = cmj_data.set_index('metric_id')[trial_cols]
            
            # Use the proper composite scoring methodology
            best_trial_col, best_score, composite_scores, best_metrics = get_best_trial(pivot_data, global_means, global_stds)
            
            if best_trial_col is None:
                print(f"    No valid composite scores for test {test_id}")
                continue
            
            # Create result dictionary for all 19 metrics
            result_dict = {}
            for metric in cmj_metrics:
                if metric in pivot_data.index and best_trial_col in pivot_data.columns:
                    result_dict[metric] = pivot_data.loc[metric, best_trial_col]
                else:
                    result_dict[metric] = np.nan
            
            # Add metadata
            result_dict.update({
                'result_id': str(uuid.uuid4()),
                'assessment_id': assessment_id,
                'athlete_id': profile.get('externalId', None),
                'athlete_name': athlete_name,
                'test_date': test_date,
                'age_at_test': age_at_test,
                'cmj_composite_score': best_score
            })
            
            processed_results.append(pd.DataFrame([result_dict]))
            print(f"    Processed test {test_id} with composite score: {best_score:.3f}")
            
        except Exception as e:
            print(f"    Error processing test {test_id}: {e}")
            continue
        
        time.sleep(0.5)  # Rate limiting
    
    if processed_results:
        combined_df = pd.concat(processed_results, ignore_index=True)
        print(f"Successfully processed {len(combined_df)} CMJ tests for {athlete_name}")
        return combined_df
    else:
        raise Exception("No CMJ tests could be processed for James McArthur")

def upload_to_bigquery(df, credentials):
    """Upload data to BigQuery"""
    try:
        print(f"Uploading {len(df)} records to BigQuery...")
        
        # Ensure proper data types
        df = df.copy()
        
        # Convert date columns
        if 'test_date' in df.columns:
            df['test_date'] = pd.to_datetime(df['test_date']).dt.date
        
        # Convert numeric columns
        numeric_columns = [col for col in df.columns if col not in ['result_id', 'assessment_id', 'athlete_id', 'athlete_name', 'test_date']]
        for col in numeric_columns:
            if col in df.columns:
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
    """Verify James McArthur was added correctly"""
    try:
        query = f"""
        SELECT athlete_name, COUNT(*) as test_count, 
               MIN(test_date) as first_test, MAX(test_date) as last_test,
               AVG(cmj_composite_score) as avg_composite_score,
               MIN(cmj_composite_score) as min_composite_score,
               MAX(cmj_composite_score) as max_composite_score
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE athlete_name = 'James McArthur'
        GROUP BY athlete_name
        """
        
        df = bq_client.query(query).to_dataframe()
        
        if df.empty:
            print("James McArthur not found in database after upload")
            return False
        else:
            row = df.iloc[0]
            print(f"Verification successful:")
            print(f"  Athlete: {row['athlete_name']}")
            print(f"  Tests: {row['test_count']}")
            print(f"  Date range: {row['first_test']} to {row['last_test']}")
            print(f"  Composite scores: {row['min_composite_score']:.3f} to {row['max_composite_score']:.3f} (avg: {row['avg_composite_score']:.3f})")
            return True
            
    except Exception as e:
        print(f"Error verifying upload: {e}")
        return False

def main():
    print("Starting proper James McArthur data ingestion...")
    
    try:
        # Setup BigQuery
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        print("BigQuery client setup successful")
        
        # Get VALD API token
        token = get_access_token()
        print("VALD API token obtained")
        
        # Calculate global statistics from all athletes
        global_means, global_stds = get_global_stats_from_all_athletes(token)
        
        # Find James McArthur
        profile = find_james_mcarthur(token)
        
        # Process his CMJ tests using proper methodology
        cmj_data = process_james_cmj_tests(profile, token, global_means, global_stds)
        
        print(f"Final data shape: {cmj_data.shape}")
        print("Sample composite scores (before scaling):", cmj_data['cmj_composite_score'].describe())
        
        # Normalize composite scores to 50-100 scale (same as enhanced_cmj_processor.py)
        min_score = cmj_data['cmj_composite_score'].min()
        max_score = cmj_data['cmj_composite_score'].max()
        if max_score != min_score:
            cmj_data['cmj_composite_score'] = 50 + (cmj_data['cmj_composite_score'] - min_score) / (max_score - min_score) * 50
        else:
            cmj_data['cmj_composite_score'] = 100
            
        print("Sample composite scores (after scaling):", cmj_data['cmj_composite_score'].describe())
        
        # Upload to BigQuery
        success = upload_to_bigquery(cmj_data, credentials)
        if not success:
            return False
        
        # Verify upload
        return verify_upload(bq_client)
        
    except Exception as e:
        print(f"Error in main process: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("James McArthur has been successfully added to the database using proper methodology!")
        sys.exit(0)
    else:
        print("Failed to add James McArthur to the database")
        sys.exit(1)