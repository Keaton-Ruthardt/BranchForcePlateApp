"""
Enhanced VALD Assessment Project with Composite Scoring Integration
This script processes VALD data and uploads to GCP with composite scores for CMJ tests.
"""

import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import numpy as np
import uuid
from datetime import datetime
from VALDapiHelpers import get_access_token, get_profiles, FD_Tests_by_Profile, get_FD_results
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
from CompositeScore import calculate_composite_score

# Configuration
CREDENTIALS_FILE = 'gcp_credentials.json'
PROJECT_ID = 'your-project-id'  # Replace with your actual project ID
DATASET_ID = 'vald_data'

# Metrics of interest for each test type
METRICS_OF_INTEREST = {
    'CMJ': [
        'BODY_WEIGHT_LBS_Trial_lb',
        'CONCENTRIC_DURATION_Trial_ms',
        'CONCENTRIC_IMPULSE_Trial_Ns',
        'CONCENTRIC_RFD_Trial_N/s',
        'ECCENTRIC_BRAKING_RFD_Trial_N/s',
        'JUMP_HEIGHT_IMP_MOM_Trial_cm',
        'PEAK_CONCENTRIC_FORCE_Trial_N',
        'PEAK_TAKEOFF_POWER_Trial_W',
        'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W/kg',
        'CONCENTRIC_IMPULSE_P1_Trial_Ns',
        'CONCENTRIC_IMPULSE_P2_Trial_Ns',
        'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns',
        'RSI_MODIFIED_IMP_MOM_Trial_RSI_mod',
        'RSI_MODIFIED_Trial_RSI_mod',
        'CON_P2_CON_P1_IMPULSE_RATIO_Trial_'
    ],
    'HJ': ['HOP_RSI_Trial_'],
    'PPU': [
        'PEAK_CONCENTRIC_FORCE_Trial_N',
        'RELATIVE_PEAK_CONCENTRIC_FORCE_Trial_N/kg',
        'ECCENTRIC_BRAKING_RFD_Trial_N/s',
        'CONCENTRIC_DURATION_Trial_ms'
    ]
}

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

def process_and_upload_data():
    """Process and upload all available test data with composite scoring."""
    
    # Create assessment record
    assessment_id = str(uuid.uuid4())
    assessment_df = pd.DataFrame([{
        'assessment_id': assessment_id,
        'athlete_id': selected_profile_id,
        'test_date': test_date,
        'assessment_type': 'Standard Assessment with Composite Scoring'
    }])
    
    assessments_schema = [
        {'name': 'assessment_id', 'type': 'STRING'},
        {'name': 'athlete_id', 'type': 'STRING'},
        {'name': 'test_date', 'type': 'DATE'},
        {'name': 'assessment_type', 'type': 'STRING'}
    ]
    
    upload_to_bigquery(assessment_df, 'Assessments', table_schema=assessments_schema)

    # --- Process and upload CMJ data with composite scoring ---
    if 'df_cmj' in last_test_dfs:
        print("Processing CMJ data with composite scoring...")
        
        # Enhanced schema with composite scoring fields
        cmj_schema = [
            {'name': 'result_id', 'type': 'STRING'},
            {'name': 'assessment_id', 'type': 'STRING'},
            {'name': 'cmj_composite_score', 'type': 'FLOAT64'},
            {'name': 'best_trial_number', 'type': 'INT64'},
            {'name': 'test_date', 'type': 'DATE'},
            {'name': 'age_at_test', 'type': 'INT64'},
            {'name': 'BODY_WEIGHT_LBS_Trial_lb', 'type': 'FLOAT64'},
            {'name': 'CONCENTRIC_DURATION_Trial_ms', 'type': 'INT64'},
            {'name': 'CONCENTRIC_IMPULSE_Trial_Ns', 'type': 'FLOAT64'},
            {'name': 'CONCENTRIC_IMPULSE_Asym_Ns', 'type': 'FLOAT64'},
            {'name': 'CONCENTRIC_RFD_Trial_N_s_', 'type': 'FLOAT64'},
            {'name': 'ECCENTRIC_BRAKING_IMPULSE_Asym_Ns', 'type': 'FLOAT64'},
            {'name': 'ECCENTRIC_BRAKING_RFD_Trial_N_s_', 'type': 'FLOAT64'},
            {'name': 'JUMP_HEIGHT_IMP_MOM_Trial_cm', 'type': 'FLOAT64'},
            {'name': 'PEAK_CONCENTRIC_FORCE_Trial_N', 'type': 'FLOAT64'},
            {'name': 'RSI_MODIFIED_IMP_MOM_Trial_RSI_mod', 'type': 'FLOAT64'},
            {'name': 'RSI_MODIFIED_Trial_RSI_mod', 'type': 'FLOAT64'},
            {'name': 'PEAK_TAKEOFF_POWER_Trial_W', 'type': 'FLOAT64'},
            {'name': 'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg', 'type': 'FLOAT64'},
            {'name': 'CON_P2_CON_P1_IMPULSE_RATIO_Trial_', 'type': 'FLOAT64'},
            {'name': 'CONCENTRIC_IMPULSE_P1_Trial_Ns', 'type': 'FLOAT64'},
            {'name': 'CONCENTRIC_IMPULSE_P2_Trial_Ns', 'type': 'FLOAT64'},
            {'name': 'CONCENTRIC_IMPULSE_P1_Asym_Ns', 'type': 'FLOAT64'},
            {'name': 'CONCENTRIC_IMPULSE_P2_Asym_Ns', 'type': 'FLOAT64'},
            {'name': 'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns', 'type': 'FLOAT64'}
        ]
        
        df = last_test_dfs['df_cmj'].copy()
        trial_cols = [c for c in df.columns if 'trial' in c.lower()]
        
        if trial_cols:
            # Create pivot table with metrics as index and trials as columns
            pivot_data = df.set_index('metric_id')[trial_cols]
            
            # Calculate composite scores
            print("Calculating composite scores for CMJ trials...")
            processed_data = calculate_composite_score(pivot_data, 'CMJ')
            
            # Extract best trial information
            best_trial_num = 1  # Default
            best_composite_score = 0.0  # Default
            
            if 'composite_score' in processed_data.index:
                best_trial_num = processed_data.loc['composite_score', 'best_trial']
                best_composite_score = processed_data.loc['composite_score', 'best_composite_score']
                print(f"Best trial: {best_trial_num} with composite score: {best_composite_score:.3f}")
            
            # Get best trial data
            best_trial_col = f'trial {best_trial_num}'
            
            # Create GCP-ready dataframe
            gcp_data = {}
            gcp_data['result_id'] = str(uuid.uuid4())
            gcp_data['assessment_id'] = assessment_id
            gcp_data['cmj_composite_score'] = best_composite_score
            gcp_data['best_trial_number'] = best_trial_num
            
            # Add test_date and age_at_test to gcp_data
            gcp_data['test_date'] = test_date
            age_at_test = None
            if 'dateOfBirth' in profile_row and pd.notna(profile_row['dateOfBirth']):
                dob = pd.to_datetime(profile_row['dateOfBirth']).date()
                age_at_test = test_date.year - dob.year - ((test_date.month, test_date.day) < (dob.month, dob.day))
            gcp_data['age_at_test'] = age_at_test
            
            # Map metrics to GCP schema
            metric_mapping = {
                'BODY_WEIGHT_LBS_Trial_lb': 'BODY_WEIGHT_LBS_Trial_lb',
                'CONCENTRIC_DURATION_Trial_ms': 'CONCENTRIC_DURATION_Trial_ms',
                'CONCENTRIC_IMPULSE_Trial_Ns': 'CONCENTRIC_IMPULSE_Trial_Ns',
                'CONCENTRIC_RFD_Trial_N/s': 'CONCENTRIC_RFD_Trial_N_s_',
                'ECCENTRIC_BRAKING_RFD_Trial_N/s': 'ECCENTRIC_BRAKING_RFD_Trial_N_s_',
                'JUMP_HEIGHT_IMP_MOM_Trial_cm': 'JUMP_HEIGHT_IMP_MOM_Trial_cm',
                'PEAK_CONCENTRIC_FORCE_Trial_N': 'PEAK_CONCENTRIC_FORCE_Trial_N',
                'PEAK_TAKEOFF_POWER_Trial_W': 'PEAK_TAKEOFF_POWER_Trial_W',
                'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W/kg': 'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',
                'CONCENTRIC_IMPULSE_P1_Trial_Ns': 'CONCENTRIC_IMPULSE_P1_Trial_Ns',
                'CONCENTRIC_IMPULSE_P2_Trial_Ns': 'CONCENTRIC_IMPULSE_P2_Trial_Ns',
                'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns': 'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns'
            }
            
            for vald_metric, gcp_metric in metric_mapping.items():
                if vald_metric in processed_data.index and best_trial_col in processed_data.columns:
                    value = processed_data.loc[vald_metric, best_trial_col]
                    if pd.notna(value):
                        gcp_data[gcp_metric] = value
                    else:
                        gcp_data[gcp_metric] = None
                else:
                    gcp_data[gcp_metric] = None
            
            # Add asymmetry metrics if available
            for metric in ['CONCENTRIC_IMPULSE_Asym_Ns', 'ECCENTRIC_BRAKING_IMPULSE_Asym_Ns', 
                           'CONCENTRIC_IMPULSE_P1_Asym_Ns', 'CONCENTRIC_IMPULSE_P2_Asym_Ns']:
                asym_metric = metric.replace('_Asym_', '_Asym_')
                if asym_metric in processed_data.index and best_trial_col in processed_data.columns:
                    value = processed_data.loc[asym_metric, best_trial_col]
                    if pd.notna(value):
                        gcp_data[metric] = value
                    else:
                        gcp_data[metric] = None
                else:
                    gcp_data[metric] = None
            
            # Add RSI metrics if available
            for rsi_metric in ['RSI_MODIFIED_IMP_MOM_Trial_RSI_mod', 'RSI_MODIFIED_Trial_RSI_mod']:
                if rsi_metric in processed_data.index and best_trial_col in processed_data.columns:
                    value = processed_data.loc[rsi_metric, best_trial_col]
                    if pd.notna(value):
                        gcp_data[rsi_metric] = value
                    else:
                        gcp_data[rsi_metric] = None
                else:
                    gcp_data[rsi_metric] = None
            
            # Add impulse ratio if available
            if 'CON_P2_CON_P1_IMPULSE_RATIO_Trial_' in processed_data.index and best_trial_col in processed_data.columns:
                value = processed_data.loc['CON_P2_CON_P1_IMPULSE_RATIO_Trial_', best_trial_col]
                if pd.notna(value):
                    gcp_data['CON_P2_CON_P1_IMPULSE_RATIO_Trial_'] = value
                else:
                    gcp_data['CON_P2_CON_P1_IMPULSE_RATIO_Trial_'] = None
            else:
                gcp_data['CON_P2_CON_P1_IMPULSE_RATIO_Trial_'] = None
            
            # Convert to DataFrame and upload
            df_to_upload = pd.DataFrame([gcp_data])
            
            # Ensure proper data types
            for col_schema in cmj_schema:
                col_name = col_schema['name']
                col_type = col_schema['type']
                if col_type in ['INT64', 'FLOAT64'] and col_name in df_to_upload.columns:
                    df_to_upload[col_name] = pd.to_numeric(df_to_upload[col_name], errors='coerce')
            
            upload_to_bigquery(df_to_upload, 'cmj_results_with_composite', table_schema=cmj_schema)
            print(f"CMJ data with composite score {best_composite_score:.3f} uploaded successfully!")
        else:
            print("No trial data found for CMJ")

    # --- Process and upload HJ data ---
    if 'df_hj' in last_test_dfs:
        hj_schema = [
            {'name': 'result_id', 'type': 'STRING'},
            {'name': 'assessment_id', 'type': 'STRING'},
            {'name': 'HOP_RSI_Trial_', 'type': 'FLOAT64'}
        ]
        
        df = last_test_dfs['df_hj'].copy()
        trial_cols = [c for c in df.columns if 'trial' in c.lower()]
        df['summary'] = df[trial_cols].apply(lambda row: row.nlargest(5).mean(), axis=1)
        df_wide = df.pivot_table(index=None, columns='metric_id', values='summary')

        df_wide.columns = df_wide.columns.str.replace('/', '_s_').str.replace('.', '_')

        schema_col_names = [col['name'] for col in hj_schema]
        df_to_upload = df_wide.reindex(columns=schema_col_names)

        df_to_upload['result_id'] = str(uuid.uuid4())
        df_to_upload['assessment_id'] = assessment_id
        
        upload_to_bigquery(df_to_upload, 'hj_results', table_schema=hj_schema)

    # --- Process and upload PPU data ---
    if 'df_ppu' in last_test_dfs:
        ppu_schema = [
            {'name': 'result_id', 'type': 'STRING'},
            {'name': 'assessment_id', 'type': 'STRING'},
            {'name': 'ECCENTRIC_BRAKING_RFD_Trial_N_s_', 'type': 'FLOAT64'},
            {'name': 'MEAN_ECCENTRIC_FORCE_Asym_N', 'type': 'FLOAT64'},
            {'name': 'MEAN_TAKEOFF_FORCE_Asym_N', 'type': 'FLOAT64'},
            {'name': 'PEAK_CONCENTRIC_FORCE_Trial_N', 'type': 'FLOAT64'},
            {'name': 'RELATIVE_PEAK_CONCENTRIC_FORCE_Trial_N_kg', 'type': 'FLOAT64'},
            {'name': 'CONCENTRIC_DURATION_Trial_ms', 'type': 'INT64'},
            {'name': 'PEAK_CONCENTRIC_FORCE_Asym_N', 'type': 'FLOAT64'},
            {'name': 'PEAK_ECCENTRIC_FORCE_Asym_N', 'type': 'FLOAT64'}
        ]

        df = last_test_dfs['df_ppu'].copy()
        trial_cols = [c for c in df.columns if 'trial' in c.lower()]
        max_metrics = ['PEAK_CONCENTRIC_FORCE_Trial_N', 'RELATIVE_PEAK_CONCENTRIC_FORCE_Trial_N/kg', 'ECCENTRIC_BRAKING_RFD_Trial_N/s']
        df['summary'] = np.where(df['metric_id'].isin(max_metrics), df[trial_cols].max(axis=1), df[trial_cols].mean(axis=1))
        df_wide = df.pivot_table(index=None, columns='metric_id', values='summary')

        df_wide.columns = df_wide.columns.str.replace('/', '_s_').str.replace('.', '_')
        
        schema_col_names = [col['name'] for col in ppu_schema]
        df_to_upload = df_wide.reindex(columns=schema_col_names)

        df_to_upload['result_id'] = str(uuid.uuid4())
        df_to_upload['assessment_id'] = assessment_id
        
        for col_schema in ppu_schema:
            col_name = col_schema['name']
            col_type = col_schema['type']
            if col_type in ['INT64', 'FLOAT64']:
                df_to_upload[col_name] = pd.to_numeric(df_to_upload[col_name], errors='coerce')

        upload_to_bigquery(df_to_upload, 'ppu_results', table_schema=ppu_schema)
    
    messagebox.showinfo("Upload Complete", "Finished processing and uploading all available test data with composite scoring!")

# =================================================================================
# GUI AND API FETCHING LOGIC
# =================================================================================

# Initialize token and profiles
token = get_access_token()
profiles = get_profiles(token)
if profiles.empty:
    print("No profiles found. Exiting.")
    exit()

name_to_id = dict(zip(profiles['fullName'], profiles['profileId']))
name_list = sorted(name_to_id.keys())

selected_profile_id = None
last_test_dfs = {}
available_tests_df = pd.DataFrame()
test_date = None

# Create GUI
root = tk.Tk()
root.title("VALD Assessment Project - Enhanced with Composite Scoring")

def fetch_all_tests_for_profile(profile_id, token):
    start_date = "2021-01-01T00:00:00Z"
    df = FD_Tests_by_Profile(start_date, profile_id, token)
    if df is None or df.empty:
        return pd.DataFrame()
    df['modifiedDateUtc'] = pd.to_datetime(df['modifiedDateUtc'])
    if 'recordedDateUtc' in df.columns:
        df['recordedDateUtc'] = pd.to_datetime(df['recordedDateUtc'])
    return df

def update_listbox(*args):
    typed = name_var.get().lower()
    listbox.delete(0, tk.END)
    if typed:
        for name in name_list:
            if typed in name.lower():
                listbox.insert(tk.END, name)

def fill_entry(event):
    if not listbox.curselection():
        return
    selected_name = listbox.get(listbox.curselection())
    name_var.set(selected_name)
    listbox.delete(0, tk.END)
    select_profile()

def select_profile():
    global selected_profile_id, available_tests_df
    selected_name = name_var.get().strip()
    selected_profile_id = name_to_id.get(selected_name)

    if selected_profile_id:
        result_label.config(text=f"Profile ID: {selected_profile_id}")
        fetch_button.config(state="normal")
        available_tests_df = fetch_all_tests_for_profile(selected_profile_id, token)
        if not available_tests_df.empty:
            unique_dates = sorted(available_tests_df['modifiedDateUtc'].dt.date.unique())
            test_selector['values'] = [str(d) for d in unique_dates]
            if unique_dates:
                test_selector.current(0)
        else:
            test_selector['values'] = []
            test_selector.set('')
    else:
        result_label.config(text="Name not found.")
        fetch_button.config(state="disabled")
        test_selector['values'] = []
        test_selector.set('')

def fetch_test_data():
    global last_test_dfs, test_date
    if not selected_profile_id:
        messagebox.showwarning("No Profile", "Please select a profile first.")
        return

    selected_index = test_selector.current()
    if selected_index == -1 or available_tests_df.empty:
        messagebox.showinfo("No Test", "Please select a test date.")
        return

    selected_date_str = test_selector.get()
    selected_date = pd.to_datetime(selected_date_str).date()
    test_date = selected_date

    tests_on_date = available_tests_df[available_tests_df['modifiedDateUtc'].dt.date == selected_date]

    if tests_on_date.empty:
        messagebox.showinfo("No Data", "No tests found for this date.")
        last_test_dfs = {}
        return

    all_dfs = {}
    for _, test_row in tests_on_date.iterrows():
        tid = test_row['testId']
        ttype = test_row['testType']
        results = get_FD_results(tid, token)
        if results is None or results.empty:
            continue
        
        if 'metric_id' in results.columns:
            filtered = results[results['metric_id'].isin(METRICS_OF_INTEREST.get(ttype, []))].copy()
            key = f"df_{ttype.lower()}"
            all_dfs[key] = filtered
        else:
            print(f"Warning: 'metric_id' not found in results for test type {ttype}")

    last_test_dfs = all_dfs
    messagebox.showinfo("Data Fetched", f"Found {len(all_dfs)} test types for the selected date.")

# GUI Layout
frame = ttk.Frame(root, padding="10")
frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

# Profile selection
ttk.Label(frame, text="Select Athlete:").grid(row=0, column=0, sticky=tk.W, pady=5)
name_var = tk.StringVar()
name_var.trace('w', update_listbox)
name_entry = ttk.Entry(frame, textvariable=name_var, width=40)
name_entry.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=5)

listbox = tk.Listbox(frame, height=5)
listbox.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=5)
listbox.bind('<Double-Button-1>', fill_entry)

result_label = ttk.Label(frame, text="")
result_label.grid(row=3, column=0, sticky=tk.W, pady=5)

# Test date selection
ttk.Label(frame, text="Select Test Date:").grid(row=4, column=0, sticky=tk.W, pady=5)
test_selector = ttk.Combobox(frame, state="readonly", width=40)
test_selector.grid(row=5, column=0, sticky=(tk.W, tk.E), pady=5)

# Buttons
fetch_button = ttk.Button(frame, text="Fetch Test Data", command=fetch_test_data, state="disabled")
fetch_button.grid(row=6, column=0, sticky=(tk.W, tk.E), pady=10)

upload_button = ttk.Button(frame, text="Process & Upload with Composite Scoring", command=process_and_upload_data)
upload_button.grid(row=7, column=0, sticky=(tk.W, tk.E), pady=10)

# Status
status_label = ttk.Label(frame, text="Ready to process VALD data with composite scoring")
status_label.grid(row=8, column=0, sticky=tk.W, pady=10)

root.mainloop() 