import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
from token_generator import get_access_token
from VALDapiHelpers import get_profiles, FD_Tests_by_Profile, get_FD_results
from datetime import datetime
import os

# Environment/config
TENANT_ID = os.getenv("TENANT_ID")
FORCEDECKS_URL = os.getenv("FORCEDECKS_URL")

METRICS_OF_INTEREST = {
    'CMJ': ['BODY_WEIGHT_LBS_Trial_lb',
            'CONCENTRIC_DURATION_Trial_ms',
            'CONCENTRIC_IMPULSE_Trial_Ns',
            'CONCENTRIC_IMPULSE_Asym_Ns',
            'CONCENTRIC_RFD_Trial_N/s',
            'ECCENTRIC_BRAKING_IMPULSE_Asym_Ns',
            'ECCENTRIC_BRAKING_RFD_Trial_N/s',
            'JUMP_HEIGHT_IMP_MOM_Trial_cm',
            'PEAK_CONCENTRIC_FORCE_Trial_N',
            'RSI_MODIFIED_IMP_MOM_Trial_RSI_mod',
            'RSI_MODIFIED_Trial_RSI_mod',
            'PEAK_TAKEOFF_POWER_Trial_W',
            'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W/kg',
            'CON_P2_CON_P1_IMPULSE_RATIO_Trial_',
            'CONCENTRIC_IMPULSE_P1_Trial_Ns',
            'CONCENTRIC_IMPULSE_P2_Trial_Ns',
            'CONCENTRIC_IMPULSE_P1_Asym_Ns',
            'CONCENTRIC_IMPULSE_P2_Asym_Ns',
            'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns',],
    'IMTP': ['ISO_BM_REL_FORCE_PEAK_Trial_N/kg',
             'PEAK_VERTICAL_FORCE_Trial_N'],
    'PPU': ['ECCENTRIC_BRAKING_RFD_Trial_N/s',
            'MEAN_ECCENTRIC_FORCE_Asym_N',
            'MEAN_TAKEOFF_FORCE_Asym_N',
            'PEAK_CONCENTRIC_FORCE_Trial_N',
            'RELATIVE_PEAK_CONCENTRIC_FORCE_Trial_N/kg',
            'CONCENTRIC_DURATION_Trial_ms',
            'PEAK_CONCENTRIC_FORCE_Asym_N',
            'PEAK_ECCENTRIC_FORCE_Asym_N'], 
    'HJ': ['HOP_RSI_Trial_']
}

# Step 1: Get token
token = get_access_token()

# Step 2: Get profiles from API
profiles = get_profiles(token)
if profiles.empty:
    print("No profiles found. Exiting.")
    exit()

# Map names to IDs
name_to_id = dict(zip(profiles['fullName'], profiles['profileId']))
name_list = sorted(name_to_id.keys())

# State variables
selected_profile_id = None
last_test_df = pd.DataFrame()

# Main window
root = tk.Tk()
root.title("VALD Test by Profile Finder")

# -----------------------------------------------------------------------------
# UI Helper: create tabs for each test type and display DataFrame
# -----------------------------------------------------------------------------
def display_all_tests(notebook, tests_df, token):
    # Clear existing tabs
    for tab_id in notebook.tabs():
        notebook.forget(tab_id)

    for test_type in tests_df['testType'].unique():
        # Fetch first testId for this type
        test_id = (
            tests_df.loc[tests_df['testType'] == test_type, 'testId']
            .iloc[0]
        )
        # Get the detailed results
        result_df = get_FD_results(test_id, token)
        filtered = result_df[result_df['metric_id'].isin(METRICS_OF_INTEREST[test_type])]
        #result_df.to_csv(f'{test_type}.csv')

        # Create a frame for this tab
        frame = ttk.Frame(notebook)
        notebook.add(frame, text=test_type)

        # Display DataFrame in a Text widget
        text_widget = tk.Text(frame, wrap='none')
        text_widget.insert('1.0', filtered.to_string(index=False))
        text_widget.pack(fill='both', expand=True)

        # (Optional) Add scrollbars within each tab
        h_scroll = ttk.Scrollbar(frame, orient='horizontal', command=text_widget.xview)
        v_scroll = ttk.Scrollbar(frame, orient='vertical', command=text_widget.yview)
        text_widget.configure(xscrollcommand=h_scroll.set, yscrollcommand=v_scroll.set)
        h_scroll.pack(fill='x', side='bottom')
        v_scroll.pack(fill='y', side='right')

# -----------------------------------------------------------------------------
# Callbacks
# -----------------------------------------------------------------------------
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
    global selected_profile_id
    selected_name = name_var.get().strip()
    selected_profile_id = name_to_id.get(selected_name)

    if selected_profile_id:
        result_label.config(text=f"Profile ID: {selected_profile_id}")
        fetch_button.config(state="normal")
    else:
        result_label.config(text="Name not found.")
        fetch_button.config(state="disabled")

def fetch_test_data():
    global last_test_df

    if not selected_profile_id:
        messagebox.showwarning("No Profile", "Please select a profile first.")
        return

    # Get the chosen date
    selected_date = date_picker.get_date()
    date_iso = selected_date.strftime("%Y-%m-%dT00:00:00Z")

    # Fetch list of tests
    df = FD_Tests_by_Profile(date_iso, selected_profile_id, token)

    if df is None or df.empty:
        messagebox.showinfo("No Tests", "No tests found for that profile and date.")
        display_all_tests(notebook, pd.DataFrame(), token)
        return

    # Filter to the exact day
    df['modifiedDateUtc'] = pd.to_datetime(df['modifiedDateUtc'])
    df = df[df['modifiedDateUtc'].dt.date == selected_date]

    if df.empty:
        messagebox.showinfo("No Tests", "No tests found on that date.")
        display_all_tests(notebook, pd.DataFrame(), token)
        return

    # Save and display
    last_test_df = df
    display_all_tests(notebook, last_test_df, token)

# -----------------------------------------------------------------------------
# GUI Layout
# -----------------------------------------------------------------------------
# Name autocomplete
tk.Label(root, text="Start typing a name:").pack(padx=10, pady=(10, 0))
name_var = tk.StringVar()
name_var.trace_add("write", update_listbox)
entry = tk.Entry(root, textvariable=name_var, width=40)
entry.pack(padx=10, pady=5)
listbox = tk.Listbox(root, height=6, width=40)
listbox.pack(padx=10, pady=(0, 10))
listbox.bind("<<ListboxSelect>>", fill_entry)

# Profile result
result_label = tk.Label(root, text="")
result_label.pack(padx=10, pady=(0, 10))

# Date picker
tk.Label(root, text="Select test date:").pack(padx=10, pady=(5, 0))
date_picker = DateEntry(root, width=12, background='darkblue',
                        foreground='white', borderwidth=2,
                        date_pattern='yyyy-mm-dd')
date_picker.pack(padx=10, pady=5)

# Fetch button
fetch_button = tk.Button(root, text="Fetch Test Data", command=fetch_test_data, state="disabled")
fetch_button.pack(padx=10, pady=(5, 10))

# Notebook for per-test-type pages
notebook = ttk.Notebook(root)
notebook.pack(fill='both', expand=True, padx=10, pady=10)

root.mainloop()