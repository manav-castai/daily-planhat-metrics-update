# Daily Planhat Metrics Update

This script provides a daily update mechanism that pulls billing data from a GCP bucket, processes it to calculate cumulative and forecasted billable CPUs for various companies, and uploads the results to Planhat using their API.

## Features
- **Google Cloud Storage Integration**: Fetches CSV files from a specified GCP bucket.
- **Metrics Calculation**: Computes cumulative and forecasted billable CPUs based on daily data.
- **Planhat Integration**: Updates the cumulative and forecasted billable CPUs for companies using the Planhat API.

## Setup

### Prerequisites
- Python 3.8 or higher
- A GCP Service Account with access to the required storage bucket
- Planhat API token

### Installation
1. Clone the repository:
    ```bash
    git clone https://github.com/your-username/daily-planhat-metrics-update.git
    ```
2. Navigate into the project directory:
    ```bash
    cd daily-planhat-metrics-update
    ```
3. Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

### Environment Variables

Create a `.env` file in the root of the project and set the following environment variables:

- **GCP_SERVICE_ACCOUNT_JSON**: The JSON content of your GCP service account credentials (in string format).
- **PLANHAT_API_TOKEN**: The API token for authenticating with Planhat.
- **PLANHAT_TENANT_TOKEN**: The tenant ID used for Planhat API calls.
- **BILLING_BUCKET_NAME**: The name of the GCP storage bucket containing the billing data.

### Usage
To simulate or test the script, you can comment out the `update_planhat` function call to avoid sending updates to Planhat during testing.

Note: The 'pull_and_update(request)' is the entry point for the entire script.

- Run it locally:
    ```bash
    python main.py
    ```
- Deploy it as a function if using Google Cloud Functions or other cloud services.

### Logging
The script provides extensive logging using Python's built-in `logging` module. Logs include:
- Processed companies
- Cumulative and forecasted CPU metrics
- Any errors encountered during data retrieval or API updates