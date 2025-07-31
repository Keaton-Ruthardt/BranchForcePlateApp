"""
Quick test to check if athlete_id was properly uploaded to CMJ results
"""
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# Configuration
CREDENTIALS_FILE = 'gcp_credentials.json'
PROJECT_ID = 'vald-ref-data'
DATASET_ID = 'athlete_performance_db'

def check_cmj_results():
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        
        # Check recent CMJ results with athlete_id
        query = f"""
        SELECT 
            athlete_id,
            athlete_name,
            test_date,
            cmj_composite_score,
            result_id
        FROM `{PROJECT_ID}.{DATASET_ID}.cmj_results`
        WHERE athlete_id IS NOT NULL
        ORDER BY test_date DESC
        LIMIT 10
        """
        
        result = client.query(query).to_dataframe()
        print(f"Found {len(result)} CMJ results with athlete_id:")
        print(result[['athlete_id', 'athlete_name', 'test_date', 'cmj_composite_score']])
        
        # Check if athlete_id matches athletes table
        if not result.empty:
            print("\nVerifying athlete_id matches athletes table:")
            athlete_id = result.iloc[0]['athlete_id']
            
            verify_query = f"""
            SELECT a.athlete_ID, a.fullName, a.profileId
            FROM `{PROJECT_ID}.{DATASET_ID}.athletes` a
            WHERE a.athlete_ID = '{athlete_id}'
            """
            
            verify_result = client.query(verify_query).to_dataframe()
            print(verify_result)
            
    except Exception as e:
        print(f"Error checking CMJ results: {e}")

if __name__ == "__main__":
    check_cmj_results()