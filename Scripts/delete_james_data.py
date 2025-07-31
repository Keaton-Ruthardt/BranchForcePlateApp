#!/usr/bin/env python3
"""
Script to delete incorrect James McArthur data from BigQuery
"""

from google.oauth2 import service_account
from google.cloud import bigquery

# Configuration
PROJECT_ID = "vald-ref-data"
DATASET_ID = "athlete_performance_db"
TABLE_ID = "cmj_results"
CREDENTIALS_FILE = 'gcp_credentials.json'

def delete_james_data():
    """Delete James McArthur data from BigQuery"""
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        
        # Delete query
        delete_query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE athlete_name = 'James McArthur'
        """
        
        print("Deleting James McArthur data...")
        query_job = client.query(delete_query)
        query_job.result()  # Wait for completion
        
        print(f"Successfully deleted James McArthur data from {TABLE_ID}")
        
        # Verify deletion
        verify_query = f"""
        SELECT COUNT(*) as count
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE athlete_name = 'James McArthur'
        """
        
        result = client.query(verify_query).to_dataframe()
        remaining_count = result.iloc[0]['count']
        
        if remaining_count == 0:
            print("Verification: James McArthur data successfully deleted")
        else:
            print(f"Warning: {remaining_count} records still remain")
            
        return True
        
    except Exception as e:
        print(f"Error deleting James McArthur data: {e}")
        return False

if __name__ == "__main__":
    success = delete_james_data()
    if success:
        print("Deletion completed successfully")
    else:
        print("Deletion failed")