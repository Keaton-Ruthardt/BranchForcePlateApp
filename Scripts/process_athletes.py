import pandas as pd
import os
from VALDapiHelpers import get_access_token, get_profiles
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuration
CREDENTIALS_FILE = 'gcp_credentials.json'
PROJECT_ID = 'vald-ref-data'
DATASET_ID = 'athlete_performance_db'
TABLE_ID = 'athletes'

# Helper to zero-pad athlete_ID
def format_athlete_id(n):
    return f"{n:07d}"

def main():
    # Authenticate with GCP
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        print("Successfully loaded GCP credentials.")
    except Exception as e:
        print(f"ERROR: Could not load credentials. {e}")
        return

    # Read current athletes table
    try:
        query = f"SELECT athlete_ID, profileId FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        existing_athletes = bq_client.query(query).to_dataframe()
        print(f"Loaded {len(existing_athletes)} existing athletes from BigQuery.")
    except Exception as e:
        print(f"WARNING: Could not read athletes table. Assuming empty. {e}")
        existing_athletes = pd.DataFrame(columns=["athlete_ID", "profileId"])

    # Build mapping: profileId -> athlete_ID
    profile_to_id = dict(zip(existing_athletes['profileId'], existing_athletes['athlete_ID']))
    used_ids = set(existing_athletes['athlete_ID'])

    # Get next available athlete_ID
    def get_next_id():
        if not used_ids:
            return format_athlete_id(1)
        max_id = max(int(aid) for aid in used_ids)
        return format_athlete_id(max_id + 1)

    # Fetch all profiles from VALD API
    token = get_access_token()
    profiles = get_profiles(token)
    print(f"Fetched {len(profiles)} profiles from VALD API.")


    # Prepare new athletes to add
    new_athletes = []
    for _, row in profiles.iterrows():
        profileId = str(row['profileId'])
        if profileId in profile_to_id:
            continue  # Already in table
        athlete_ID = get_next_id()
        used_ids.add(athlete_ID)
        profile_to_id[profileId] = athlete_ID
        new_athletes.append({
            'athlete_ID': athlete_ID,
            'profileId': profileId,
            'fullName': row.get('fullName', None),
            'dateOfBirth': row.get('dateOfBirth', None)
        })

    # Insert new athletes into BigQuery
    if new_athletes:
        df_new = pd.DataFrame(new_athletes)
        job = bq_client.load_table_from_dataframe(df_new, f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        job.result()  # Wait for job to complete
        print(f"Inserted {len(df_new)} new athletes into BigQuery.")
    else:
        print("No new athletes to add.")

if __name__ == "__main__":
    main() 