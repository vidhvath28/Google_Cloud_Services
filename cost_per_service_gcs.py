import os
from google.cloud import bigquery

# Set the path to your service account JSON file
SERVICE_ACCOUNT_FILE = "C:/Users/Vidhvath28/Downloads/cost_mangament_backup/ym-region-jakarta-fcce3479d4f1.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE

# Define your project and dataset details
PROJECT_ID = "ym-region-jakarta"  # Your GCP project ID
DATASET_ID = "billing_dataset"  # Update with actual dataset
TABLE_ID = "gcp_billing_export_v1_"  # Update with actual table name

def fetch_billing_data():
    try:
        client = bigquery.Client(project=PROJECT_ID)

        # Construct the full table path
        table_path = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        # Query to fetch recent billing data
        query = f"""
        SELECT service.description, usage_start_time, cost
        FROM `{table_path}`
        ORDER BY usage_start_time DESC
        LIMIT 10
        """

        query_job = client.query(query)  # Execute query
        results = query_job.result()  # Wait for results

        print("✅ Billing Data:")
        for row in results:
            print(f"Service: {row.description}, Date: {row.usage_start_time}, Cost: ${row.cost}")

    except Exception as e:
        print(f"❌ Error fetching billing data:\n{e}")

if __name__ == "__main__":
    fetch_billing_data()
