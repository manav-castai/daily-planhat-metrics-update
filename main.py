import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage
from io import StringIO
import calendar
from google.api_core.exceptions import NotFound, Forbidden, GoogleAPIError
import unittest
import pandas as pd
from datetime import datetime
import time

# Setup logging
logging.basicConfig(level=logging.INFO)

def download_and_process_csv_for_date(bucket_name, service_account_info, target_date):
    """
    Downloads and processes the CSV file for a particular date from a GCP bucket directly in memory.
    """
    try:
        # Create the storage client using the service account info (as dictionary)
        storage_client = storage.Client.from_service_account_info(service_account_info)
        bucket = storage_client.get_bucket(bucket_name)
        logging.info(f"Accessing bucket '{bucket_name}' for files with date {target_date}.")

        blobs = bucket.list_blobs()
        for blob in blobs:
            if target_date in blob.name and blob.name.endswith('.csv'):
                logging.info(f"Found file: {blob.name}")
                # Download the CSV content as a string and process it in memory
                csv_content = blob.download_as_string().decode('utf-8')
                df = pd.read_csv(StringIO(csv_content))
                logging.info("File processed successfully in memory.")
                return df  # Return the DataFrame

        logging.warning(f"No file found for the date: {target_date}")
        return None
    except NotFound as e:
        logging.error(f"Bucket '{bucket_name}' does not exist. Details: {e}")
    except Forbidden as e:
        logging.error(f"Access to bucket '{bucket_name}' is forbidden. Details: {e}")
    except GoogleAPIError as e:
        logging.error(f"A Google API error occurred. Details: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    return None

def fetch_planhat_companies(api_token, limit=500):
    """
    Fetches a single batch of companies from Planhat with a limit on the number of companies.
    
    Parameters:
    api_token (str): Planhat API token for authentication.
    limit (int): Maximum number of companies to fetch (default 500).
    
    Returns:
    pd.DataFrame: A DataFrame containing the fetched companies' information.
    """
    url = 'https://api.planhat.com/companies'
    headers = {'Authorization': f'Bearer {api_token}'}
    
    try:
        logging.info("Fetching a single batch of companies from Planhat...")
        
        params = {'offset': 0, 'limit': limit}
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        companies = response.json()

        # Process the batch of companies
        batch_data = [{
            'Planhat ID': c.get('_id'),
            'Org ID': c.get('custom', {}).get('Org ID'),
            'Company Name': c.get('name')
        } for c in companies]

        df = pd.DataFrame(batch_data)
        logging.info(f"Successfully fetched {len(df)} companies.\n")
        return df
    
    except Exception as e:
        logging.error(f"Error fetching companies from Planhat: {e}")
        return None

def calculate_metrics(df_current, org_ids, data_date):
    """
    Calculates cumulative and forecasted billable CPUs.
    """
    # Ensure Org IDs are strings and cleaned
    org_ids = [str(org_id).strip().lower() for org_id in org_ids]

    # Clean and convert 'OrganizationID' in DataFrame
    df_current['OrganizationID'] = df_current['OrganizationID'].astype(str).str.strip().str.lower()
    df_current['Total'] = pd.to_numeric(df_current['Total'], errors='coerce').fillna(0)

    # Filter data for the organization IDs
    df_current_org = df_current[df_current['OrganizationID'].isin(org_ids)]
    cumulative_total = df_current_org['Total'].sum()

    # Forecasted billable CPUs
    days_passed = data_date.day
    days_in_month = calendar.monthrange(data_date.year, data_date.month)[1]
    if days_passed > 0:
        average_daily_cpus = cumulative_total / days_passed
    else:
        average_daily_cpus = 0
    forecasted_cpus = average_daily_cpus * days_in_month

    return round(cumulative_total, 2), round(forecasted_cpus, 2)

def update_planhat(api_token, company_id, org_id, date_str, cumulative_cpus, forecasted_cpus, company_name):
    """
    Updates Planhat with the cumulative and forecasted metrics.
    """
    planhat_tenant_id = '8fbda5b0-f5fd-4d6f-86e2-1d9eecf0322a'
    # planhat_tenant_id = os.getenv('PLANHAT_TENANT_TOKEN')
    
    url = f'https://analytics.planhat.com/dimensiondata/{planhat_tenant_id}'
    headers = {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json'
    }

    upload_date = datetime.strptime(date_str, '%Y-%m-%d')

    data = [
        {
            "dimensionId": "Cumulative Billable CPUs",
            "value": cumulative_cpus,
            "externalId": org_id,
            "model": "Asset",
            "date": upload_date.strftime('%Y-%m-%d')
        },
        {
            "dimensionId": "Forecasted Billable CPUs",
            "value": forecasted_cpus,
            "externalId": org_id,
            "model": "Asset",
            "date": upload_date.strftime('%Y-%m-%d')
        }
    ]

    try:
        logging.info(f"Updating Planhat for Company Name: {company_name}")
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        logging.info(f"Planhat updated successfully for Company Name: {company_name}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error updating Planhat for Company Name: {company_name}, {e}")
    logging.info("\n")

def pull_and_update(request):
    # Configuration

    service_account_json_str = os.getenv('GCP_SERVICE_ACCOUNT_JSON')
    service_account_info = json.loads(service_account_json_str)
    
    api_token = os.getenv('PLANHAT_API_TOKEN')
    planhat_tenant_id = os.getenv('PLANHAT_TENANT_TOKEN')
    bucket_name = os.getenv('BILLING_BUCKET_NAME')

    # Ensure configuration variables are set
    if not bucket_name or not service_account_info or not api_token:
        logging.error("Environment variables GCP_BUCKET_NAME, GCP_SERVICE_ACCOUNT_JSON, or PLANHAT_API_TOKEN are not set.")
        return "Env var configuration error", 500

    # Execution date (current date)
    execution_date = datetime.utcnow()  # Use current UTC date and time
    data_date = execution_date - timedelta(days=1)  # Data corresponds to previous day
    date_str = data_date.strftime('%Y-%m-%d')

    # Download current day's data
    df_current = download_and_process_csv_for_date(bucket_name, service_account_info, execution_date.strftime('%Y-%m-%d'))
    if df_current is None:
        logging.error("Current day's data not available. Exiting.")
        return "CSV data not available", 500

    # Fetch companies from Planhat (as in original)
    df_companies = fetch_planhat_companies(api_token)
    if df_companies is None:
        logging.error("Failed to fetch companies from Planhat. Exiting.")
        return "Failed to fetch companies", 500

    # Define org_id_sets for companies with multiple Org IDs
    org_id_sets = [
        {'7ba2041d-b88f-4b67-a63a-64e78962b014', 'a29883b2-997e-4b44-8bf5-a0a95bbdf639'}, # Provenir
        {'551cf481-0042-4076-a5a1-a78e23193c84', 'c116cabe-9d57-46c3-b37b-a93e8f52967e'}, # OneTrust
    ]

    # Process each company
    for _, company in df_companies.iterrows():
        company_org_id = company['Org ID']
        company_name = company['Company Name']
        planhat_id = company['Planhat ID']
        if not company_org_id:
            logging.warning(f"Company '{company_name}' does not have an Org ID. Skipping.")
            continue

        # Determine if the company_org_id is in any of the org_id_sets
        matching_set = None
        for org_set in org_id_sets:
            if company_org_id in org_set:
                matching_set = org_set
                break

        if matching_set:
            org_ids = matching_set
            logging.info(f"Company '{company_name}' uses multiple Org IDs: {org_ids}")
        else:
            org_ids = [company_org_id]
            logging.info(f"Company '{company_name}' uses single Org ID: {company_org_id}")

        # Calculate metrics
        cumulative_cpus, forecasted_cpus = calculate_metrics(df_current, org_ids, data_date)

        # Print the results
        logging.info(f"Company: {company_name}")
        logging.info(f"Date: {date_str}")
        logging.info(f"Cumulative CPUs: {cumulative_cpus}")
        logging.info(f"Forecasted CPUs: {forecasted_cpus}")

        # Update Planhat with cumulative and forecasted CPUs
        update_planhat(api_token, planhat_id, company_org_id, date_str, cumulative_cpus, forecasted_cpus, company_name)
        time.sleep(1)

    logging.info("Script completed successfully.")
    return "Success", 200