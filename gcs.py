import os
import pandas as pd
from google.cloud import bigquery, billing
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Google Cloud credentials and project details
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BILLING_ACCOUNT_ID = os.getenv("BILLING_ACCOUNT_ID")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BILLING_DATASET_ID = os.getenv("BILLING_DATASET_ID")

# Ensure service account authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

# Initialize BigQuery client
bq_client = bigquery.Client()

# Query to fetch cost details
def fetch_cost_details():
    query = f"""
        SELECT 
            invoice.month AS InvoiceMonth, 
            service.description AS Service, 
            sku.description AS SKU, 
            usage_start_time AS UsageStartTime, 
            project.id AS ProjectID, 
            labels, 
            cost, 
            currency
        FROM `{GCP_PROJECT_ID}.{BILLING_DATASET_ID}.gcp_billing_export_v1`
        WHERE invoice.month = FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
        ORDER BY usage_start_time DESC
    """
    query_job = bq_client.query(query)
    return query_job.to_dataframe()

# Save data to a separate CSV file for each service
def save_to_csv(df):
    grouped = df.groupby("Service")
    for service, data in grouped:
        filename = f"cost_reports/{service.replace(' ', '_')}.csv"
        os.makedirs("cost_reports", exist_ok=True)
        data.to_csv(filename, index=False)
        print(f"Saved: {filename}")

# Main function
def main():
    df = fetch_cost_details()
    if not df.empty:
        save_to_csv(df)
    else:
        print("No billing data found!")

if __name__ == "__main__":
    main()
