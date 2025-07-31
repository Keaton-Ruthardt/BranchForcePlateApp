import pandas as pd
import numpy as np
import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
from token_generator import get_access_token
from VALDapiHelpers import get_profiles, FD_Tests_by_Profile, get_FD_results
from datetime import datetime
import os
import uuid
import json

import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery

# =================================================================================
# CONFIGURATION - Make sure to set your new Project ID here
# =================================================================================
PROJECT_ID = "vald-ref-data"
DATASET_ID = "athlete_performance_db"
CREDENTIALS_FILE = 'gcp_credentials.json'

# =================================================================================
# LOAD CREDENTIALS
# =================================================================================
try:
    with open(CREDENTIALS_FILE, 'r') as f:
        creds_json = json.load(f)
        print(f"Attempting to use credentials for: {creds_json.get('client_email')}")

    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
    bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    print("Successfully loaded GCP credentials and BigQuery client.")
except Exception as e:
    print(f"ERROR: Could not load credentials. {e}")
    messagebox.showerror("Auth Error", f"Could not load GCP credentials:\n{e}")
    bq_client = None

# =================================================================================
# METRICS OF INTEREST
# =================================================================================
METRICS_OF_INTEREST = {
    'CMJ': ['BODY_WEIGHT_LBS_Trial_lb', 'CONCENTRIC_DURATION_Trial_ms', 'CONCENTRIC_IMPULSE_Trial_Ns', 'CONCENTRIC_IMPULSE_Asym_Ns', 'CONCENTRIC_RFD_Trial_N/s', 'ECCENTRIC_BRAKING_IMPULSE_Asym_Ns', 'ECCENTRIC_BRAKING_RFD_Trial_N/s', 'JUMP_HEIGHT_IMP_MOM_Trial_cm', 'PEAK_CONCENTRIC_FORCE_Trial_N', 'RSI_MODIFIED_IMP_MOM_Trial_RSI_mod', 'RSI_MODIFIED_Trial_RSI_mod', 'PEAK_TAKEOFF_POWER_Trial_W', 'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W/kg', 'CON_P2_CON_P1_IMPULSE_RATIO_Trial_', 'CONCENTRIC_IMPULSE_P1_Trial_Ns', 'CONCENTRIC_IMPULSE_P2_Trial_Ns', 'CONCENTRIC_IMPULSE_P1_Asym_Ns', 'CONCENTRIC_IMPULSE_P2_Asym_Ns', 'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns'],
    'IMTP': ['ISO_BM_REL_FORCE_PEAK_Trial_N/kg', 'PEAK_VERTICAL_FORCE_Trial_N'],
    'PPU': ['ECCENTRIC_BRAKING_RFD_Trial_N/s', 'MEAN_ECCENTRIC_FORCE_Asym_N', 'MEAN_TAKEOFF_FORCE_Asym_N', 'PEAK_CONCENTRIC_FORCE_Trial_N', 'RELATIVE_PEAK_CONCENTRIC_FORCE_Trial_N/kg', 'CONCENTRIC_DURATION_Trial_ms', 'PEAK_CONCENTRIC_FORCE_Asym_N', 'PEAK_ECCENTRIC_FORCE_Asym_N'],
    'HJ': ['HOP_RSI_Trial_']
}

# =================================================================================
# BigQuery Upload Helper
# =================================================================================
def upload_to_bigquery(df, table_name, table_schema=None):
    if df.empty:
        print(f"DataFrame for {table_name} is empty. Skipping upload.")
        return
    if bq_client is None:
        messagebox.showerror("Auth Error", "BigQuery client not available. Cannot upload.")
        return False

    table_id = f"{DATASET_ID}.{table_name}"
    print(f"Uploading data to BigQuery table: {table_id}...")

    try:
        # For debugging: print dtypes right before upload
        print(f"--- Schema for {table_name} ---")
        print(df.info())
        print("--------------------")
        
        pandas_gbq.to_gbq(
            df,
            destination_table=table_id,
            project_id=PROJECT_ID,
            credentials=credentials,
            if_exists='append',
            table_schema=table_schema
        )
        print(f"Successfully uploaded {len(df)} rows to {table_name}.")
        return True
    except Exception as e:
        print(f"An error occurred during BigQuery upload to {table_name}: {e}")
        messagebox.showerror("Upload Failed", f"Failed to upload to {table_name}:\n\n{e}")
        return False

# =================================================================================
# Main Data Processing Logic
# =================================================================================
def process_and_upload_data():
    if not last_test_dfs or not selected_profile_id: return
    if bq_client is None: return

    profile_row = profiles.loc[profiles['profileId'] == selected_profile_id].iloc[0]
    
    print(f"Upserting athlete {profile_row['fullName']} into Athletes table...")
    merge_sql = f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.Athletes` T
        USING (SELECT '{selected_profile_id}' as athlete_id, '{profile_row['fullName']}' as full_name, DATE('{profile_row['dateOfBirth'].date()}') as dob) S
        ON T.athlete_id = S.athlete_id
        WHEN MATCHED THEN UPDATE SET full_name = S.full_name, last_updated = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (athlete_id, full_name, date_of_birth, last_updated)
            VALUES(S.athlete_id, S.full_name, S.dob, CURRENT_TIMESTAMP())
    """
    bq_client.query(merge_sql).result()

    assessment_id = str(uuid.uuid4())
    assessment_df = pd.DataFrame([{'assessment_id': assessment_id, 'athlete_id': selected_profile_id, 'test_date': test_date, 'assessment_type': 'Standard Assessment'}])
    assessments_schema = [{'name': 'assessment_id', 'type': 'STRING'}, {'name': 'athlete_id', 'type': 'STRING'}, {'name': 'test_date', 'type': 'DATE'}, {'name': 'assessment_type', 'type': 'STRING'}]
    upload_to_bigquery(assessment_df, 'Assessments', table_schema=assessments_schema)

    # --- Process and upload CMJ data ---
    if 'df_cmj' in last_test_dfs:
        cmj_schema = [
            {'name': 'result_id', 'type': 'STRING'}, {'name': 'assessment_id', 'type': 'STRING'}, {'name': 'cmj_composite_score', 'type': 'FLOAT64'},
            {'name': 'test_date', 'type': 'DATE'}, {'name': 'age_at_test', 'type': 'INT64'},
            {'name': 'BODY_WEIGHT_LBS_Trial_lb', 'type': 'FLOAT64'}, {'name': 'CONCENTRIC_DURATION_Trial_ms', 'type': 'INT64'},
            {'name': 'CONCENTRIC_IMPULSE_Trial_Ns', 'type': 'FLOAT64'}, {'name': 'CONCENTRIC_IMPULSE_Asym_Ns', 'type': 'FLOAT64'},
            {'name': 'CONCENTRIC_RFD_Trial_N_s_', 'type': 'FLOAT64'}, {'name': 'ECCENTRIC_BRAKING_IMPULSE_Asym_Ns', 'type': 'FLOAT64'},
            {'name': 'ECCENTRIC_BRAKING_RFD_Trial_N_s_', 'type': 'FLOAT64'}, {'name': 'JUMP_HEIGHT_IMP_MOM_Trial_cm', 'type': 'FLOAT64'},
            {'name': 'PEAK_CONCENTRIC_FORCE_Trial_N', 'type': 'FLOAT64'}, {'name': 'RSI_MODIFIED_IMP_MOM_Trial_RSI_mod', 'type': 'FLOAT64'},
            {'name': 'RSI_MODIFIED_Trial_RSI_mod', 'type': 'FLOAT64'}, {'name': 'PEAK_TAKEOFF_POWER_Trial_W', 'type': 'FLOAT64'},
            {'name': 'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg', 'type': 'FLOAT64'}, {'name': 'CON_P2_CON_P1_IMPULSE_RATIO_Trial_', 'type': 'FLOAT64'},
            {'name': 'CONCENTRIC_IMPULSE_P1_Trial_Ns', 'type': 'FLOAT64'}, {'name': 'CONCENTRIC_IMPULSE_P2_Trial_Ns', 'type': 'FLOAT64'},
            {'name': 'CONCENTRIC_IMPULSE_P1_Asym_Ns', 'type': 'FLOAT64'}, {'name': 'CONCENTRIC_IMPULSE_P2_Asym_Ns', 'type': 'FLOAT64'},
            {'name': 'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns', 'type': 'FLOAT64'}
        ]
        
        df = last_test_dfs['df_cmj'].copy()
        trial_cols = [c for c in df.columns if 'trial' in c.lower()]
        max_metrics = ['BODY_WEIGHT_LBS_Trial_lb', 'CONCENTRIC_IMPULSE_Trial_Ns', 'CONCENTRIC_RFD_Trial_N/s', 'ECCENTRIC_BRAKING_RFD_Trial_N/s', 'JUMP_HEIGHT_IMP_MOM_Trial_cm', 'PEAK_CONCENTRIC_FORCE_Trial_N', 'PEAK_TAKEOFF_POWER_Trial_W', 'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W/kg', 'CONCENTRIC_IMPULSE_P1_Trial_Ns', 'CONCENTRIC_IMPULSE_P2_Trial_Ns', 'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns']
        df['summary'] = np.where(df['metric_id'].isin(max_metrics), df[trial_cols].max(axis=1), df[trial_cols].mean(axis=1))
        df_wide = df.pivot_table(index=None, columns='metric_id', values='summary')
        
        df_wide.columns = df_wide.columns.str.replace('/', '_s_').str.replace('.', '_')
        
        # ## FIX: Use reindex to conform the DataFrame to the schema perfectly
        schema_col_names = [col['name'] for col in cmj_schema]
        df_to_upload = df_wide.reindex(columns=schema_col_names)

        df_to_upload['result_id'] = str(uuid.uuid4())
        df_to_upload['assessment_id'] = assessment_id
        df_to_upload['cmj_composite_score'] = 0.0

        # Add test_date and age_at_test to df_to_upload
        df_to_upload['test_date'] = test_date
        age_at_test = None
        if 'dateOfBirth' in profile_row and pd.notna(profile_row['dateOfBirth']):
            dob = pd.to_datetime(profile_row['dateOfBirth']).date()
            age_at_test = test_date.year - dob.year - ((test_date.month, test_date.day) < (dob.month, dob.day))
        df_to_upload['age_at_test'] = age_at_test

        for col_schema in cmj_schema:
            col_name = col_schema['name']
            col_type = col_schema['type']
            if col_type in ['INT64', 'FLOAT64']:
                df_to_upload[col_name] = pd.to_numeric(df_to_upload[col_name], errors='coerce')
        
        upload_to_bigquery(df_to_upload, 'cmj_results', table_schema=cmj_schema)

    # --- Process and upload HJ data ---
    if 'df_hj' in last_test_dfs:
        hj_schema = [{'name': 'result_id', 'type': 'STRING'}, {'name': 'assessment_id', 'type': 'STRING'}, {'name': 'HOP_RSI_Trial_', 'type': 'FLOAT64'}]
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
            {'name': 'result_id', 'type': 'STRING'}, {'name': 'assessment_id', 'type': 'STRING'},
            {'name': 'ECCENTRIC_BRAKING_RFD_Trial_N_s_', 'type': 'FLOAT64'}, {'name': 'MEAN_ECCENTRIC_FORCE_Asym_N', 'type': 'FLOAT64'},
            {'name': 'MEAN_TAKEOFF_FORCE_Asym_N', 'type': 'FLOAT64'}, {'name': 'PEAK_CONCENTRIC_FORCE_Trial_N', 'type': 'FLOAT64'},
            {'name': 'RELATIVE_PEAK_CONCENTRIC_FORCE_Trial_N_kg', 'type': 'FLOAT64'}, {'name': 'CONCENTRIC_DURATION_Trial_ms', 'type': 'INT64'},
            {'name': 'PEAK_CONCENTRIC_FORCE_Asym_N', 'type': 'FLOAT64'}, {'name': 'PEAK_ECCENTRIC_FORCE_Asym_N', 'type': 'FLOAT64'}
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
    
    messagebox.showinfo("Upload Complete", "Finished processing and uploading all available test data.")

# =================================================================================
# GUI AND API FETCHING LOGIC (Largely Unchanged)
# =================================================================================
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

root = tk.Tk()
root.title("VALD Assessment Project")

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
    print("Dataframes created:", list(last_test_dfs.keys()))
    
    process_and_upload_data()

# --- GUI Layout ---
tk.Label(root, text="Start typing a name:").pack(padx=10, pady=(10, 0))
name_var = tk.StringVar()
name_var.trace_add("write", update_listbox)
entry = tk.Entry(root, textvariable=name_var, width=40)
entry.pack(padx=10, pady=5)
listbox = tk.Listbox(root, height=6, width=40)
listbox.pack(padx=10, pady=(0, 10))
listbox.bind("<<ListboxSelect>>", fill_entry)
result_label = tk.Label(root, text="")
result_label.pack(padx=10, pady=(0, 10))
tk.Label(root, text="Select a test date:").pack(padx=10, pady=(5, 0))
test_selector = ttk.Combobox(root, state="readonly")
test_selector.pack(padx=10, pady=5)
fetch_button = tk.Button(root, text="Fetch Test Data", command=fetch_test_data, state="disabled")
fetch_button.pack(padx=10, pady=(5, 10))
root.mainloop()
