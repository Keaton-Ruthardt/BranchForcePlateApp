"""
Enhanced CMJ Processor with Composite Scoring Integration
This script integrates composite scoring into the existing GCP pipeline for CMJ data.
"""

import pandas as pd
import numpy as np
import uuid
from datetime import datetime, timedelta
from newcompositescore import calculate_composite_score_per_trial, get_best_trial, CMJ_weights
from VALDapiHelpers import get_access_token, get_profiles, FD_Tests_by_Profile, get_FD_results
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
import os
# Add import for deepcopy
from copy import deepcopy
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import threading
import random
import requests
import json

# Configuration
CREDENTIALS_FILE = 'gcp_credentials.json'
PROJECT_ID = 'vald-ref-data'  # Replace with your actual project ID
DATASET_ID = 'athlete_performance_db'
TABLE_ID = 'cmj_results'
ATHLETES_TABLE_ID = 'athletes'

# Rate limiting configuration - Optimized for better throughput
MIN_REQUEST_INTERVAL = 0.5  # Reduced from 1.5s to 0.5s (still conservative)
MAX_CONCURRENT_REQUESTS = 2  # Increased from 1 to 2 concurrent requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

# Shared token and lock for thread-safe refresh
shared_token = {'token': None}
token_lock = threading.Lock()

# Athlete ID cache to reduce BigQuery lookups
athlete_id_cache = {}
cache_lock = threading.Lock()

# Progress tracking and checkpointing
processed_athletes_file = 'processed_athletes.txt'
failed_athletes_file = 'failed_athletes.txt'
last_token_refresh = 0
TOKEN_REFRESH_INTERVAL = 1800  # Refresh token every 30 minutes (1800 seconds)

# Rate limiting
rate_limit_lock = threading.Lock()
last_request_time = 0
api_semaphore = threading.Semaphore(MAX_CONCURRENT_REQUESTS)  # Only allow 1 concurrent API call

def rate_limited_request():
    """Ensure minimum time between API requests to avoid 429 errors."""
    global last_request_time
    with api_semaphore:
        with rate_limit_lock:
            current_time = time.time()
            time_since_last = current_time - last_request_time
            if time_since_last < MIN_REQUEST_INTERVAL:
                sleep_time = MIN_REQUEST_INTERVAL - time_since_last
                time.sleep(sleep_time)
            last_request_time = time.time()

def force_refresh_token():
    """Force refresh token regardless of cache"""
    global last_token_refresh
    logging.info("Force refreshing access token...")
    
    payload = {
        "grant_type": "client_credentials",
        "client_id": os.getenv('CLIENT_ID'),
        "client_secret": os.getenv('CLIENT_SECRET')
    }

    response = requests.post(os.getenv('AUTH_URL'), data=payload)
    if response.status_code == 200:
        token = response.json()['access_token']
        expires_in = response.json().get('expires_in', 7200)
        expires_at = (datetime.now() + timedelta(seconds=expires_in - 60)).isoformat()

        # Update cache file
        with open('.token_cache.json', "w") as f:
            json.dump({"access_token": token, "expires_at": expires_at}, f)

        last_token_refresh = time.time()
        logging.info("Access token force refreshed successfully.")
        return token
    else:
        logging.error(f"Force token refresh failed: {response.status_code} - {response.text}")
        raise Exception(f"Auth failed: {response.status_code} - {response.text}")

def periodic_token_refresh():
    """Check if token needs periodic refresh"""
    global last_token_refresh, shared_token
    current_time = time.time()
    
    if current_time - last_token_refresh > TOKEN_REFRESH_INTERVAL:
        try:
            new_token = force_refresh_token()
            with token_lock:
                shared_token['token'] = new_token
            logging.info("Periodic token refresh completed")
        except Exception as e:
            logging.error(f"Periodic token refresh failed: {e}")

def load_processed_athletes():
    """Load list of already processed athletes"""
    processed = set()
    if os.path.exists(processed_athletes_file):
        with open(processed_athletes_file, 'r') as f:
            processed = set(line.strip() for line in f)
    return processed

def save_processed_athlete(athlete_name):
    """Save processed athlete to checkpoint file"""
    with open(processed_athletes_file, 'a') as f:
        f.write(f"{athlete_name}\n")

def save_failed_athlete(athlete_name, error_message):
    """Save failed athlete with error details"""
    with open(failed_athletes_file, 'a') as f:
        f.write(f"{athlete_name}: {error_message}\n")

def clear_checkpoints():
    """Clear checkpoint files to start fresh"""
    for file in [processed_athletes_file, failed_athletes_file]:
        if os.path.exists(file):
            os.remove(file)
            print(f"Cleared checkpoint file: {file}")
    print("Checkpoints cleared - will process all athletes from the beginning")

def get_athlete_id_from_profile(profile_id):
    """
    Fetch athlete_ID from athletes table using profileId with caching.
    
    Args:
        profile_id: VALD profile ID
        
    Returns:
        athlete_ID string or None if not found
    """
    # Check cache first
    with cache_lock:
        if profile_id in athlete_id_cache:
            return athlete_id_cache[profile_id]
    
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        
        query = f"""
        SELECT athlete_ID 
        FROM `{PROJECT_ID}.{DATASET_ID}.{ATHLETES_TABLE_ID}` 
        WHERE profileId = '{profile_id}'
        LIMIT 1
        """
        
        result = client.query(query).to_dataframe()
        if not result.empty:
            athlete_id = result.iloc[0]['athlete_ID']
            # Cache the result
            with cache_lock:
                athlete_id_cache[profile_id] = athlete_id
            return athlete_id
        else:
            logging.warning(f"No athlete_ID found for profileId: {profile_id}")
            # Cache None result to avoid repeated lookups
            with cache_lock:
                athlete_id_cache[profile_id] = None
            return None
    except Exception as e:
        logging.error(f"Error fetching athlete_ID for profileId {profile_id}: {e}")
        return None

def upload_to_bigquery(df, table_name, table_schema=None):
    """Upload DataFrame to BigQuery with proper error handling."""
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        pandas_gbq.to_gbq(
            df,
            destination_table=f"{DATASET_ID}.{table_name}",
            project_id=PROJECT_ID,
            credentials=credentials,
            if_exists='append',
            table_schema=table_schema
        )
        print(f"Successfully uploaded {len(df)} rows to {table_name}")
        return True
    except Exception as e:
        print(f"Error uploading to BigQuery: {e}")
        return False

def get_FD_results_with_logging_and_retry(test_id, token, max_retries=5):
    for attempt in range(max_retries):
        start_time = time.time()
        try:
            # Apply rate limiting
            rate_limited_request()
            result = get_FD_results(test_id, token)
            elapsed = time.time() - start_time
            logging.info(f"API call: get_FD_results({test_id}) took {elapsed:.2f}s")
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            # Check for 429 Too Many Requests
            if hasattr(e, 'response') and hasattr(e.response, 'status_code') and e.response.status_code == 429:
                # Exponential backoff with jitter
                base_wait = 2 ** attempt
                jitter = random.uniform(0, 0.1 * base_wait)
                wait_time = base_wait + jitter
                logging.warning(f"429 Too Many Requests for test {test_id}. Retrying in {wait_time:.2f}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                logging.error(f"API call failed: get_FD_results({test_id}) after {elapsed:.2f}s: {e}")
                if attempt == max_retries - 1:
                    break
                # For other errors, wait a bit before retrying
                time.sleep(1)
    return None

def get_FD_results_with_auto_refresh(test_id, max_retries=2, timeout=20):
    global shared_token
    for attempt in range(max_retries):
        with token_lock:
            token = shared_token['token']
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(get_FD_results_with_logging_and_retry, test_id, token)
                return future.result(timeout=timeout)
        except Exception as e:
            # Detect 401 Unauthorized
            if hasattr(e, 'response') and hasattr(e.response, 'status_code') and e.response.status_code == 401:
                if attempt == max_retries - 1:
                    logging.critical(f"401 Unauthorized for test {test_id} even after token refresh. Stopping script.")
                    raise SystemExit("Critical: Unable to authenticate with VALD API. Check credentials.")
                logging.warning(f"401 Unauthorized for test {test_id}, force refreshing token and retrying...")
                with token_lock:
                    shared_token['token'] = get_access_token()
                continue  # Retry with new token
            elif isinstance(e, TimeoutError):
                logging.warning(f"Timeout fetching test {test_id}, skipping.")
                return None
            else:
                logging.error(f"API call failed: get_FD_results({test_id}): {e}")
                return None
    return None

def FD_Tests_by_Profile_with_auto_refresh(start_date, profile_id, max_retries=3):
    global shared_token
    for attempt in range(max_retries):
        with token_lock:
            token = shared_token['token']
        try:
            # Apply rate limiting
            rate_limited_request()
            return FD_Tests_by_Profile(start_date, profile_id, token)
        except Exception as e:
            if hasattr(e, 'response') and hasattr(e.response, 'status_code') and e.response.status_code == 401:
                if attempt == max_retries - 1:
                    logging.error(f"401 Unauthorized for profile {profile_id} even after token refresh. Skipping athlete.")
                    return None  # Return None instead of stopping script
                logging.warning(f"401 Unauthorized for profile {profile_id}, force refreshing token and retrying...")
                try:
                    new_token = force_refresh_token()
                    with token_lock:
                        shared_token['token'] = new_token
                except Exception as refresh_error:
                    logging.error(f"Token refresh failed: {refresh_error}. Skipping athlete.")
                    return None
                continue
            elif hasattr(e, 'response') and hasattr(e.response, 'status_code') and e.response.status_code == 204:
                # 204 No Content is normal - profile has no tests
                logging.info(f"204 No Content for profile {profile_id} - no tests found")
                return pd.DataFrame()
            elif hasattr(e, 'response') and hasattr(e.response, 'status_code') and e.response.status_code == 503:
                # 503 Service Unavailable - API temporarily down
                base_wait = 2 ** attempt
                jitter = random.uniform(0, 0.1 * base_wait)
                wait_time = base_wait + jitter
                logging.warning(f"503 Service Unavailable for profile {profile_id}. Retrying in {wait_time:.2f}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            elif hasattr(e, 'response') and hasattr(e.response, 'status_code') and e.response.status_code == 429:
                # Exponential backoff with jitter for 429 errors
                base_wait = 2 ** attempt
                jitter = random.uniform(0, 0.1 * base_wait)
                wait_time = base_wait + jitter
                logging.warning(f"429 Too Many Requests for profile {profile_id}. Retrying in {wait_time:.2f}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                logging.error(f"API call failed: FD_Tests_by_Profile({profile_id}): {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(1)
    return None

def process_cmj_test_with_composite(test_id, token, assessment_id, global_means, global_stds):
    """
    Process a single CMJ test and calculate composite scores.
    
    Args:
        test_id: VALD test ID
        token: Access token
        assessment_id: Assessment ID for GCP
    
    Returns:
        DataFrame ready for GCP upload with composite scores
    """
    
    # Fetch raw CMJ data
    print(f"Fetching CMJ data for test {test_id}...")
    raw_data = get_FD_results_with_auto_refresh(test_id)

    if raw_data is None or raw_data.empty:
        print(f"No data found for test {test_id}")
        return None, None
    
    # Filter for CMJ-specific metrics (all 19 metrics of interest)
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
        # Additional metrics (7-19) for comprehensive analysis
        'CONCENTRIC_IMPULSE_Asym_Ns',
        'ECCENTRIC_BRAKING_IMPULSE_Asym_Ns',
        'CONCENTRIC_IMPULSE_P1_Asym_Ns',
        'CONCENTRIC_IMPULSE_P2_Asym_Ns'
    ]
    
    # Filter data for CMJ metrics
    cmj_data = raw_data[raw_data['metric_id'].isin(cmj_metrics)]
    
    if cmj_data.empty:
        print(f"No CMJ metrics found for test {test_id}")
        return None, None
    
    # Pivot data to get trials as columns
    trial_cols = [col for col in cmj_data.columns if 'trial' in col.lower()]
    if not trial_cols:
        print(f"No trial data found for test {test_id}")
        return None, None
    
    # Create pivot table with metrics as index and trials as columns
    pivot_data = cmj_data.set_index('metric_id')[trial_cols]
    if not isinstance(pivot_data, pd.DataFrame):
        pivot_data = pd.DataFrame(pivot_data)
    # DEBUG: Print available metric names
    print(f"[DEBUG] Available metrics in pivot_data for test {test_id}: {list(pivot_data.index)}")
    # Calculate composite scores per trial using global means/stds
    best_trial_col, best_score, composite_scores, best_metrics = get_best_trial(pivot_data, global_means, global_stds)
    if best_trial_col is None:
        print(f"No valid composite scores for test {test_id}")
        return None, None
    # DEBUG: Print best_metrics dict
    print(f"[DEBUG] best_metrics for test {test_id}: {best_metrics}")
    # Prepare DataFrame for upload using best_metrics dict
    # Upload all 19 metrics of interest, not just composite score metrics
    all_metrics_of_interest = [
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
    # Map API metric names to BigQuery-safe column names if needed
    metric_map = {
        'CONCENTRIC_IMPULSE_Trial_N/s': 'CONCENTRIC_IMPULSE_Trial_Ns',
        'ECCENTRIC_BRAKING_RFD_Trial_N/s': 'ECCENTRIC_BRAKING_RFD_Trial_N_s',
        'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W/kg': 'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',
        'CONCENTRIC_RFD_Trial_N_s': 'CONCENTRIC_RFD_Trial_N_s',
    }
    upload_dict = {}
    for metric in all_metrics_of_interest:
        bq_col = metric_map.get(metric, metric)
        # Get the value from the best trial data
        if metric in pivot_data.index and best_trial_col in pivot_data.columns:
            upload_dict[bq_col] = pivot_data.loc[metric, best_trial_col]
        else:
            upload_dict[bq_col] = float('nan')
    gcp_data = pd.DataFrame([upload_dict])
    gcp_data['result_id'] = str(uuid.uuid4())
    gcp_data['assessment_id'] = assessment_id
    gcp_data['cmj_composite_score'] = best_score
    gcp_data.reset_index(drop=True, inplace=True)
    # Use the required schema for all 19 metrics
    gcp_schema = [
        {'name': 'result_id', 'type': 'STRING'},
        {'name': 'assessment_id', 'type': 'STRING'},
        {'name': 'athlete_id', 'type': 'STRING'},
    ] + [
        {'name': metric_map.get(metric, metric), 'type': 'FLOAT64'} for metric in all_metrics_of_interest
    ] + [
        {'name': 'athlete_name', 'type': 'STRING'},
        {'name': 'test_date', 'type': 'DATE'},
        {'name': 'age_at_test', 'type': 'INT64'},
        {'name': 'cmj_composite_score', 'type': 'FLOAT64'},
    ]
    return gcp_data, gcp_schema

def process_cmj_test_with_composite_parallel(test_id, token, assessment_id, global_means, global_stds):
    # Fetch raw CMJ data with logging and retry
    logging.info(f"Fetching CMJ data for test {test_id}...")
    raw_data = get_FD_results_with_auto_refresh(test_id, timeout=20)
    if raw_data is None or raw_data.empty:
        logging.warning(f"No data found for test {test_id}")
        return None, None
    
    # Filter for CMJ-specific metrics
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
        'CON_P2_CON_P1_IMPULSE_RATIO_Trial'
    ]
    
    # Filter data for CMJ metrics
    cmj_data = raw_data[raw_data['metric_id'].isin(cmj_metrics)]
    
    if cmj_data.empty:
        logging.warning(f"No CMJ metrics found for test {test_id}")
        return None, None
    
    # Pivot data to get trials as columns
    trial_cols = [col for col in cmj_data.columns if 'trial' in col.lower()]
    if not trial_cols:
        logging.warning(f"No trial data found for test {test_id}")
        return None, None
    
    # Create pivot table with metrics as index and trials as columns
    pivot_data = cmj_data.set_index('metric_id')[trial_cols]
    if not isinstance(pivot_data, pd.DataFrame):
        pivot_data = pd.DataFrame(pivot_data)
    logging.debug(f"[DEBUG] Available metrics in pivot_data for test {test_id}: {list(pivot_data.index)}")
    # Calculate composite scores per trial using global means/stds
    best_trial_col, best_score, composite_scores, best_metrics = get_best_trial(pivot_data, global_means, global_stds)
    if best_trial_col is None:
        logging.warning(f"No valid composite scores for test {test_id}")
        return None, None
    logging.debug(f"[DEBUG] best_metrics for test {test_id}: {best_metrics}")
    # Prepare DataFrame for upload using best_metrics dict
    # Upload all 19 metrics of interest, not just composite score metrics
    all_metrics_of_interest = [
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
    # Map API metric names to BigQuery-safe column names if needed
    metric_map = {
        'CONCENTRIC_IMPULSE_Trial_N/s': 'CONCENTRIC_IMPULSE_Trial_Ns',
        'ECCENTRIC_BRAKING_RFD_Trial_N/s': 'ECCENTRIC_BRAKING_RFD_Trial_N_s',
        'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W/kg': 'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',
        'CONCENTRIC_RFD_Trial_N_s': 'CONCENTRIC_RFD_Trial_N_s',
    }
    upload_dict = {}
    for metric in all_metrics_of_interest:
        bq_col = metric_map.get(metric, metric)
        # Get the value from the best trial data
        if metric in pivot_data.index and best_trial_col in pivot_data.columns:
            upload_dict[bq_col] = pivot_data.loc[metric, best_trial_col]
        else:
            upload_dict[bq_col] = float('nan')
    gcp_data = pd.DataFrame([upload_dict])
    gcp_data['result_id'] = str(uuid.uuid4())
    gcp_data['assessment_id'] = assessment_id
    gcp_data['cmj_composite_score'] = best_score
    gcp_data.reset_index(drop=True, inplace=True)
    # Use the required schema for all 19 metrics
    gcp_schema = [
        {'name': 'result_id', 'type': 'STRING'},
        {'name': 'assessment_id', 'type': 'STRING'},
        {'name': 'athlete_id', 'type': 'STRING'},
    ] + [
        {'name': metric_map.get(metric, metric), 'type': 'FLOAT64'} for metric in all_metrics_of_interest
    ] + [
        {'name': 'athlete_name', 'type': 'STRING'},
        {'name': 'test_date', 'type': 'DATE'},
        {'name': 'age_at_test', 'type': 'INT64'},
        {'name': 'cmj_composite_score', 'type': 'FLOAT64'},
    ]
    return gcp_data, gcp_schema

def process_cmj_test_with_composite_parallel_with_timeout(test_id, assessment_id, global_means, global_stds):
    logging.info(f"Fetching CMJ data for test {test_id}...")
    raw_data = get_FD_results_with_auto_refresh(test_id, timeout=20)
    if raw_data is None or raw_data.empty:
        logging.warning(f"No data found for test {test_id}")
        return None, None
    
    # Filter for CMJ-specific metrics (all 19 metrics of interest)
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
        # Additional metrics (7-19) for comprehensive analysis
        'CONCENTRIC_IMPULSE_Asym_Ns',
        'ECCENTRIC_BRAKING_IMPULSE_Asym_Ns',
        'CONCENTRIC_IMPULSE_P1_Asym_Ns',
        'CONCENTRIC_IMPULSE_P2_Asym_Ns'
    ]
    cmj_data = raw_data[raw_data['metric_id'].isin(cmj_metrics)]
    if cmj_data.empty:
        logging.warning(f"No CMJ metrics found for test {test_id}")
        return None, None
    trial_cols = [col for col in cmj_data.columns if 'trial' in col.lower()]
    if not trial_cols:
        logging.warning(f"No trial data found for test {test_id}")
        return None, None
    pivot_data = cmj_data.set_index('metric_id')[trial_cols]
    if not isinstance(pivot_data, pd.DataFrame):
        pivot_data = pd.DataFrame(pivot_data)
    logging.debug(f"[DEBUG] Available metrics in pivot_data for test {test_id}: {list(pivot_data.index)}")
    from newcompositescore import get_best_trial, CMJ_weights
    best_trial_col, best_score, composite_scores, best_metrics = get_best_trial(pivot_data, global_means, global_stds)
    if best_trial_col is None:
        logging.warning(f"No valid composite scores for test {test_id}")
        return None, None
    logging.debug(f"[DEBUG] best_metrics for test {test_id}: {best_metrics}")
    # Upload all 19 metrics of interest, not just composite score metrics
    all_metrics_of_interest = [
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
    # Outlier check: skip upload if any metric is >3 std from mean (only for composite score metrics)
    required_metrics = list(CMJ_weights.keys())
    for metric in required_metrics:
        value = best_metrics.get(metric, None)
        mean = global_means.get(metric, None)
        std = global_stds.get(metric, None)
        if value is not None and mean is not None and std is not None and std > 0:
            if abs(value - mean) > 3 * std:
                logging.warning(f"Skipping test {test_id} due to outlier in {metric}: value={value}, mean={mean}, std={std}")
                return None, None
    metric_map = {
        'CONCENTRIC_IMPULSE_Trial_N/s': 'CONCENTRIC_IMPULSE_Trial_Ns',
        'ECCENTRIC_BRAKING_RFD_Trial_N/s': 'ECCENTRIC_BRAKING_RFD_Trial_N_s',
        'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W/kg': 'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',
        'CONCENTRIC_RFD_Trial_N_s': 'CONCENTRIC_RFD_Trial_N_s',
    }
    upload_dict = {}
    for metric in all_metrics_of_interest:
        bq_col = metric_map.get(metric, metric)
        # Get the value from the best trial data
        if metric in pivot_data.index and best_trial_col in pivot_data.columns:
            upload_dict[bq_col] = pivot_data.loc[metric, best_trial_col]
        else:
            upload_dict[bq_col] = float('nan')
    gcp_data = pd.DataFrame([upload_dict])
    gcp_data['result_id'] = str(uuid.uuid4())
    gcp_data['assessment_id'] = assessment_id
    gcp_data['cmj_composite_score'] = best_score
    gcp_data.reset_index(drop=True, inplace=True)
    gcp_schema = [
        {'name': 'result_id', 'type': 'STRING'},
        {'name': 'assessment_id', 'type': 'STRING'},
    ] + [
        {'name': metric_map.get(metric, metric), 'type': 'FLOAT64'} for metric in all_metrics_of_interest
    ] + [
        {'name': 'test_date', 'type': 'DATE'},
        {'name': 'age_at_test', 'type': 'INT64'},
        {'name': 'cmj_composite_score', 'type': 'FLOAT64'},
    ]
    return gcp_data, gcp_schema

def process_all_cmj_tests_for_athlete(profile_id: str, token: str, assessment_id: str, athlete_name: str, athlete_dob=None, global_means=None, global_stds=None) -> list[pd.DataFrame]:
    """
    Process all CMJ tests for a given athlete and upload to GCP.
    
    Args:
        profile_id: VALD profile ID
        token: Access token
        assessment_id: Assessment ID for GCP
        athlete_name: Athlete's full name
        athlete_dob: Athlete's date of birth (datetime.date or None)
    
    Returns:
        List of processed test results
    """
    
    # Fetch all tests for the athlete
    start_date = "2021-1-1 00:00:00"
    tests_df = FD_Tests_by_Profile_with_auto_refresh(start_date, profile_id)
    
    if tests_df is None or tests_df.empty:
        print(f"No tests found for profile {profile_id}")
        return []
    
    # Filter for CMJ tests
    cmj_tests = tests_df[tests_df['testType']== 'CMJ']
    
    if cmj_tests.empty:
        print(f"No CMJ tests found for profile {profile_id}")
        return []
    
    print(f"Found {len(cmj_tests)} CMJ tests for profile {profile_id}")
    
    processed_results = []
    
    for _, test_row in cmj_tests.iterrows():
        test_id = test_row['testId']
        test_date = pd.to_datetime(test_row['modifiedDateUtc']).date()
        # Calculate age_at_test using athlete_dob from profile
        age_at_test = None
        if athlete_dob is not None and not pd.isnull(athlete_dob):
            dob = pd.to_datetime(athlete_dob).date() if not isinstance(athlete_dob, (datetime, pd.Timestamp)) else athlete_dob
            age_at_test = test_date.year - dob.year - ((test_date.month, test_date.day) < (dob.month, dob.day))

        print(f"Processing CMJ test {test_id} from {test_date}...")

        # Process the test, pass global_means and global_stds
        gcp_data, gcp_schema = process_cmj_test_with_composite_parallel(test_id, token, assessment_id, global_means, global_stds)

        # Only append if gcp_data is a valid, non-empty DataFrame
        if isinstance(gcp_data, pd.DataFrame) and not gcp_data.empty:
            # Get athlete_ID from athletes table
            athlete_id = get_athlete_id_from_profile(profile_id)
            
            gcp_data['athlete_id'] = athlete_id
            gcp_data['athlete_name'] = athlete_name
            gcp_data['test_date'] = test_date
            gcp_data['age_at_test'] = age_at_test
            processed_results.append(gcp_data)
            print(f"Successfully processed test {test_id}")
        else:
            print(f"Failed to process test {test_id}")
    
    return processed_results

def fetch_and_process_test(test_row, assessment_id, global_means, global_stds, athlete_name, athlete_dob, profile_id):
    test_id = test_row['testId']
    test_date = pd.to_datetime(test_row['modifiedDateUtc']).date()
    age_at_test = None
    if athlete_dob is not None and not pd.isnull(athlete_dob):
        dob = pd.to_datetime(athlete_dob).date() if not isinstance(athlete_dob, (datetime, pd.Timestamp)) else athlete_dob
        age_at_test = test_date.year - dob.year - ((test_date.month, test_date.day) < (dob.month, dob.day))
    
    # Add small delay to avoid overwhelming the API
    time.sleep(0.5)
    
    gcp_data, gcp_schema = process_cmj_test_with_composite_parallel_with_timeout(test_id, assessment_id, global_means, global_stds)
    if isinstance(gcp_data, pd.DataFrame) and not gcp_data.empty:
        # Get athlete_ID from athletes table
        athlete_id = get_athlete_id_from_profile(profile_id)
        
        gcp_data['athlete_id'] = athlete_id
        gcp_data['athlete_name'] = athlete_name
        gcp_data['test_date'] = test_date
        gcp_data['age_at_test'] = age_at_test
        return gcp_data
    return None

def process_all_cmj_tests_for_athlete_parallel(profile_id, token, assessment_id, athlete_name, athlete_dob, global_means, global_stds):
    start_date = "2021-1-1 00:00:00"
    tests_df = FD_Tests_by_Profile_with_auto_refresh(start_date, profile_id)
    if tests_df is None or tests_df.empty:
        logging.warning(f"No tests found for profile {profile_id}")
        return []
    cmj_tests = tests_df[tests_df['testType'] == 'CMJ']
    if cmj_tests.empty:
        logging.warning(f"No CMJ tests found for profile {profile_id}")
        return []
    logging.info(f"Found {len(cmj_tests)} CMJ tests for profile {profile_id}")
    processed_results = []
    with ThreadPoolExecutor(max_workers=2) as executor:  # Optimized from 1 to 2 workers
        futures = [
            executor.submit(
                fetch_and_process_test, test_row, assessment_id, global_means, global_stds, athlete_name, athlete_dob, profile_id
            )
            for _, test_row in cmj_tests.iterrows()
        ]
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                processed_results.append(result)
    return processed_results

def main_pipeline():
    """
    Main pipeline to process CMJ data with composite scoring for all athletes.
    """
    
    # Authentication
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        print("Successfully loaded GCP credentials.")
    except Exception as e:
        print(f"ERROR: Could not load credentials. {e}")
        return

    # Always fetch a fresh token at the start
    with token_lock:
        shared_token['token'] = get_access_token()
        logging.info("Access token refreshed at script start.")
    
    # Fetch all athlete profiles
    print("Fetching athlete profiles...")
    profiles = get_profiles(shared_token['token'])
    if profiles.empty:
        print("No profiles found. Exiting.")
        return
    
    print(f"Found {len(profiles)} athlete profiles")

    # Gather CMJ trial data for global stats (all athletes)
    print("Gathering CMJ trial data for global mean/std calculation (all athletes)...")
    all_cmj_trials = []
    skipped_tests = 0
    total_tests_found = 0
    all_test_ids = []
    profiles_subset_for_stats = profiles  # Use all athletes for stats calculation
    
    for index, athlete in profiles_subset_for_stats.iterrows():
        profile_id = str(athlete['profileId'])
        start_date = "2021-1-1 00:00:00"
        tests_df = FD_Tests_by_Profile_with_auto_refresh(start_date, profile_id)
        if tests_df is not None and not tests_df.empty:
            cmj_tests = tests_df[tests_df['testType'] == 'CMJ']
            for _, test_row in cmj_tests.iterrows():
                total_tests_found += 1
                all_test_ids.append(test_row['testId'])
        else:
            print(f"[DEBUG] No tests found for profile {profile_id}")
    print(f"[DEBUG] Total CMJ tests found: {total_tests_found}")

    # Parallel fetch all test results
    def fetch_trial_data_for_stats(test_id):
        # Add small delay to avoid overwhelming the API
        time.sleep(0.5)
        return get_FD_results_with_auto_refresh(test_id, timeout=20)
    parallel_cmj_trials = []
    skipped_tests = 0
    with ThreadPoolExecutor(max_workers=3) as executor:  # Optimized to 3 workers
        futures = [executor.submit(fetch_trial_data_for_stats, test_id) for test_id in all_test_ids]
        for future in as_completed(futures):
            raw_data = future.result()
            if raw_data is not None and not raw_data.empty:
                cmj_metrics = list(CMJ_weights.keys())
                cmj_data = raw_data[raw_data['metric_id'].isin(cmj_metrics)]
                trial_cols = [col for col in cmj_data.columns if 'trial' in col.lower()]
                if trial_cols:
                    pivot_data = cmj_data.set_index('metric_id')[trial_cols]
                    parallel_cmj_trials.append(pivot_data)
                else:
                    skipped_tests += 1
                    logging.debug(f"[DEBUG] Skipping test in global stats: No trial columns found.")
            else:
                skipped_tests += 1
                logging.debug(f"[DEBUG] Skipping test in global stats: No raw data found.")
    print(f"[DEBUG] Total CMJ tests skipped (missing data/trials): {skipped_tests}")
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
                    print(f"[DEBUG] Outlier filtering for {metric}: removed {before_count - after_count} rows")
        global_means = all_trials_df[metrics].mean()
        global_stds = all_trials_df[metrics].std()
    else:
        print("No CMJ trial data found for global stats. Exiting.")
        return

    # Process all athletes
    all_results = []
    processed_tests = 0
    skipped_tests_processing = 0
    profiles_subset = profiles  # Process all athletes
    print(f"Processing all {len(profiles_subset)} athletes")
    
    # Don't clear checkpoints - allow resuming from where we left off
    # clear_checkpoints()  # Commented out to allow resuming
    
    # Load already processed athletes to resume from where we left off
    processed_athletes = load_processed_athletes()
    logging.info(f"Loaded {len(processed_athletes)} already processed athletes from checkpoint")
    
    # Initialize token refresh timer
    global last_token_refresh
    last_token_refresh = time.time()
    
    for index, athlete in profiles_subset.iterrows():
        profile_id = str(athlete['profileId'])
        athlete_name = str(athlete['fullName'])
        athlete_dob = athlete['dateOfBirth'] if 'dateOfBirth' in athlete else None
        
        # Skip if already processed
        if athlete_name in processed_athletes:
            logging.info(f"Skipping already processed athlete: {athlete_name}")
            continue
            
        print(f"\nProcessing athlete {index + 1}/{len(profiles_subset)}: {athlete_name}")
        
        # Periodic token refresh (every 30 minutes)
        periodic_token_refresh()
        
        assessment_id = str(uuid.uuid4())
        
        try:
            # Use parallelized version with bulletproof error handling
            athlete_results = process_all_cmj_tests_for_athlete_parallel(profile_id, shared_token['token'], assessment_id, athlete_name, athlete_dob, global_means, global_stds)
            
            if athlete_results:
                all_results.extend(athlete_results)
                processed_tests += len(athlete_results)
                print(f"Processed {len(athlete_results)} CMJ tests for {athlete_name}")
                save_processed_athlete(athlete_name)  # Save checkpoint
            else:
                skipped_tests_processing += 1
                print(f"No CMJ tests processed for {athlete_name}")
                save_processed_athlete(athlete_name)  # Still save to avoid re-processing
                
        except Exception as e:
            # Catch any remaining errors and continue processing
            error_msg = f"Unexpected error processing {athlete_name}: {str(e)}"
            logging.error(error_msg)
            save_failed_athlete(athlete_name, str(e))
            skipped_tests_processing += 1
            print(f"Error processing {athlete_name}, skipping and continuing...")
            
        if index < len(profiles_subset) - 1:
            print("Waiting 1 second before next athlete...")
            time.sleep(1)  # Reduced from 3 seconds to 1
    print(f"[DEBUG] Total processed tests: {processed_tests}")
    print(f"[DEBUG] Total athletes with no processed tests: {skipped_tests_processing}")
    
    # After collecting all_results and before upload
    if all_results:
        print(f"\nUploading {len(all_results)} total CMJ results to BigQuery...")
        combined_df = pd.concat(all_results, ignore_index=True)
        # Normalize composite scores to 50-100 scale
        min_score = combined_df['cmj_composite_score'].min()
        max_score = combined_df['cmj_composite_score'].max()
        if max_score != min_score:
            combined_df['cmj_composite_score'] = 50 + (combined_df['cmj_composite_score'] - min_score) / (max_score - min_score) * 50
        else:
            combined_df['cmj_composite_score'] = 100
        # Rename columns to BigQuery-safe names
        rename_map = {
            'ECCENTRIC_BRAKING_RFD_Trial_N/s': 'ECCENTRIC_BRAKING_RFD_Trial_N_s',
            'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W/kg': 'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',
        'CONCENTRIC_RFD_Trial_N_s': 'CONCENTRIC_RFD_Trial_N_s',
            'CONCENTRIC_DURATION_Trial/ms': 'CONCENTRIC_DURATION_Trial_ms',
        }
        combined_df.rename(columns=rename_map, inplace=True)
        # Print debug info before upload
        print("[DEBUG] Columns in combined_df before upload:", combined_df.columns.tolist())
        print("[DEBUG] First 5 rows of combined_df:")
        print(combined_df.head())
        # Upload all columns (including metrics and composite score)
        upload_to_bigquery(combined_df, TABLE_ID)
        
        # Print summary statistics
        print("\nSummary Statistics:")
        print(f"Average Composite Score: {combined_df['cmj_composite_score'].mean():.3f}")
        print(f"Best Composite Score: {combined_df['cmj_composite_score'].max():.3f}")
        # print(f"Number of athletes with data: {combined_df['profile_id'].nunique()}")  # Removed, not in schema
        print(f"Total tests processed: {len(combined_df)}")
    else:
        print("No CMJ results to upload")

if __name__ == "__main__":
    # Check if credentials file exists
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"ERROR: {CREDENTIALS_FILE} not found. Please ensure your GCP credentials are in place.")
    else:
        main_pipeline() 