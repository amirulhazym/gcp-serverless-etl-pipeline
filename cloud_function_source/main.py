# Import lib
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import io # For reading string data as if it were a file
import functions_framework # Required framework for Python Cloud Functions (Gen2)
from datetime import datetime, timezone # To get the current datetime for processing_datetime
import traceback

# --- Configuration (MUST EDIT THESE!) ---
TARGET_BIGQUERY_PROJECT_ID = "annular-net-386720" 
TARGET_BIGQUERY_DATASET_ID = "mp2_pipeline_output" 
TARGET_BIGQUERY_TABLE_ID = "transformed_user_events" 

# Initialize client outside function for potential reuse across invocations
storage_client = storage.Client()
# Explicitly pass project ID to BigQuery client if it might differ from ADC's default
bigquery_client = bigquery.Client(project=TARGET_BIGQUERY_PROJECT_ID)

@functions_framework.cloud_event # The @functions_framework.cloud_event decorator tells GCP
# that this function is designed to be triggered by CloudEvents, such as
# an object finalization event from Google Cloud Storage.
def process_gcs_csv_to_bq(cloud_event):
    """
    Triggered by a CloudEvent (e.g., a new file in a GCS bucket).
    Reads a CSV file from GCS, performs transformations using pandas,
    and loads the transformed data into a BigQuery table.

    Args:
        cloud_event (functions_framework.CloudEvent): The event payload.
        For GCS, data is in cloud_event.data
    """
    # Extract GCS event details from the CloudEvent data payload
    event_data = cloud_event.data
    bucket_name = event_data["bucket"]
    file_name = event_data["name"]

    print(f"Function triggered for file: gs://{bucket_name}/{file_name}")
    print(f"Event ID: {cloud_event['id']}, Event Type: {cloud_event['type']}")

    # For this simple project, we'll process any CSV uploaded.
    if not file_name.lower().endswith('.csv'):
        print(f"Skipping file {file_name} as it is not a CSV.")
        return # Exit if not a CSV file
    
    # --- 1. Download CSV content from GCS ---
    print(f"Attempting to access bucket: '{bucket_name}' and file: '{file_name}'") # Added for clarity
    retrieved_bucket = None # Initialize to None
    try:
        # Ensure bucket_name is valid before proceeding
        if not bucket_name or not file_name:
            # This case should ideally be caught by how GCS sends events, but good to be defensive
            print(f"Error: Invalid bucket_name ('{bucket_name}') or file_name ('{file_name}') received.")
            raise ValueError("Bucket name or file name is empty.")
    
        retrieved_bucket = storage_client.bucket(bucket_name)
        blob = retrieved_bucket.blob(file_name)
        
        if not blob.exists(storage_client): # Check if the blob actually exists
            print(f"Error: File '{file_name}' does not exist in bucket '{bucket_name}'.")
            # Decide how to handle: raise error, or return gracefully
            raise FileNotFoundError(f"File gs://{bucket_name}/{file_name} not found.")
    
        csv_data_string = blob.download_as_text()
        print(f"Successfully downloaded content of {file_name} from GCS.")
    
    except ValueError as ve: # Catch our specific ValueError
        print(f"ValueError during GCS operation: {ve}")
        raise
    except FileNotFoundError as fnfe: # Catch our specific FileNotFoundError
        print(f"FileNotFoundError during GCS operation: {fnfe}")
        raise
    except Exception as e:
        # This is a more general catch-all for other GCS or download issues
        # The original error 'e' might contain the UnboundLocalError if 'retrieved_bucket' was never set
        # or if the 'storage_client.bucket(bucket_name)' call itself had an issue.
        error_message = f"Error downloading '{file_name}' from GCS bucket '{bucket_name}': {str(e)}"
        print(error_message)
        # To get more details from the original exception e:
        import traceback
        print("Full traceback for GCS download error:")
        traceback.print_exc() 
        raise # Re-raise the original exception to mark function as failed

    # --- 2. Transform data using pandas ---
    try:
        df = pd.read_csv(io.StringIO(csv_data_string))
        print(f"Original DataFrame (first 3 rows):\n{df.head(3)}")
        # Print original dtypes for better debugging
        print(f"Original DataFrame dtypes:\n{df.dtypes}")


        # --- Data Cleaning & Transformation Examples ---
        # a) Handle event_timestamp:
        #    Convert to pandas datetime objects first, making them UTC aware.
        #    'errors="coerce"' will turn unparseable dates into NaT (Not a Time).
        df.loc[:, 'event_timestamp'] = pd.to_datetime(df['event_timestamp'], errors='coerce', utc=True)

        # Fill any NaT (resulting from parsing errors or original NaNs)
        # with the current UTC time.
        current_utc_for_fill = datetime.now(timezone.utc)
        # Fill NaNs/NaTs
        df['event_timestamp'].fillna(value=current_utc_for_fill, inplace=True) # Using inplace on the Series

        # CRUCIAL: After all operations, ensure the entire column is of datetime64[ns, UTC] dtype.
        # This re-application of to_datetime can help if fillna changed the Series dtype to 'object'.
        df.loc[:, 'event_timestamp'] = pd.to_datetime(df['event_timestamp'], utc=True)
        # We will NOT convert this to string here. We pass datetime objects to BigQuery loader.


        # b) Standardize country names (convert to uppercase) and map to a 2-letter country code
        country_mapping = {
            "MALAYSIA": "MY",
            "SINGAPORE": "SG",
            "THAILAND": "TH",
            "INDONESIA": "ID"
        }
        df.loc[:, 'country_code'] = df['country'].astype(str).str.upper().map(country_mapping).fillna('OT')


        # c) Create the 'is_high_value' boolean column based on the 'value' column
        df.loc[:, 'value'] = pd.to_numeric(df['value'], errors='coerce').fillna(0.0)
        df.loc[:, 'is_high_value'] = df['value'] > 100.0


        # d) Add a 'processing_datetime' column with the current UTC datetime
        #    For BigQuery DATETIME (naive), pass Python naive datetime objects.
        df.loc[:, 'processing_datetime'] = datetime.utcnow() # Python naive datetime object


        # e) Select columns to exactly match the BigQuery target table schema
        #    Order might matter if not relying on schema name matching perfectly in BQ load.
        #    Our schema: user_id, event_timestamp, country_code, value, is_high_value, processing_datetime
        final_df = df[['user_id', 'event_timestamp', 'country_code', 'value', 'is_high_value', 'processing_datetime']]

        print(f"Transformed DataFrame (first 3 rows):\n{final_df.head(3)}")
        # --- Crucial: Print dtypes JUST BEFORE sending to BigQuery ---
        print(f"Data types of final_df columns to be loaded:\n{final_df.dtypes}")


    except Exception as e:
        print(f"Error transforming data with pandas: {e}")
        # import traceback # Already imported
        print("Full traceback for pandas transformation error:")
        traceback.print_exc()
        raise

    except Exception as e:
        print(f"Error transforming data with pandas: {e}")
        import traceback # Ensure traceback is imported
        print("Full traceback for pandas transformation error:")
        traceback.print_exc()
        raise

    # --- 3. Load transformed DataFrame into BigQuery ---
    try:
        # Construct the full table reference for BigQuery
        table_id_full = f"{TARGET_BIGQUERY_PROJECT_ID}.{TARGET_BIGQUERY_DATASET_ID}.{TARGET_BIGQUERY_TABLE_ID}"

        # Configure the load job.
        # JIT Learning: WRITE_APPEND adds new rows to the table.
        # WRITE_TRUNCATE would delete all existing rows before loading new data.
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            # schema=SCHEMA, # Not needed if df columns match table schema and I load from DataFrame
            # autodetect=False # We have a defined schema
        )

        # The BigQuery client library can load data directly from a pandas DataFrame.
        # It handles schema mapping if DataFrame column names match BigQuery column names.
        print(f"Loading {len(final_df)} rows into BigQuery table: {table_id_full}")
        job = bigquery_client.load_table_from_dataframe(
            final_df, table_id_full, job_config=job_config
        )
        job.result()  # Wait for the load job to complete.

        # Fetch the updated table to get the new row count (optional, for logging)
        table = bigquery_client.get_table(table_id_full)
        print(
            f"Load job {job.job_id} completed successfully. "
            f"Output rows: {job.output_rows}. "
            f"Total rows in table '{table.table_id}': {table.num_rows}"
        )

    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")
        # For BigQuery load errors, 'e.errors' often contains more specific details
        if hasattr(e, 'errors') and e.errors:
            for error_detail in e.errors:
                print(f"  BigQuery Error Detail: {error_detail['message']}")
        raise # Re-raise to mark function execution as failed

# --- Main execution block for local testing simulation (not used by Cloud Function directly) ---
if __name__ == '__main__':
    # This section is for simulating a GCS event locally for testing purposes.
    # It requires creating a mock 'cloud_event' object.
    # For actual deployment, GCP provides the 'cloud_event' when triggered.
    print("This script contains the Cloud Function logic. "
          "To test locally in a simulated environment, you would typically "
          "use the Functions Framework CLI or mock the CloudEvent object.")
    # Example mock event for local testing (requires more setup if need/want to run this part):
    # mock_event_data = {"bucket": "test-bucket", "name": "test-file.csv"}
    # class MockCloudEvent:
    #     def __init__(self, data):
    #         self.data = data
    #         self["id"] = "test-id"
    #         self["type"] = "test-type"
    #     def __getitem__(self, key): # To allow event['id']
    #         return getattr(self, key)
    # mock_event = MockCloudEvent(data=mock_event_data)
    # process_gcs_csv_to_bq(mock_event) # This would require GCS file & BQ setup