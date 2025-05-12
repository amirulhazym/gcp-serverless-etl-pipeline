# GCP Serverless Data Pipeline: GCS to BigQuery via Cloud Function (Gen 2) & Cloud Run

An automated data pipeline built on Google Cloud Platform (GCP) that processes CSV files uploaded to Cloud Storage (GCS). The data is transformed by a Python Cloud Function (Gen 2, running on Cloud Run) with pandas, and then loaded into BigQuery for analysis. This project demonstrates a practical, event-driven, serverless ETL (Extract, Transform, Load) workflow.

<!-- Example: ![Data Pipeline Architecture](docs/images/mp2_architecture.png) -->
<!-- Diagram could show: [CSV Upload] -> [GCS Bucket] --(Eventarc Trigger)--> [Cloud Function (executing on Cloud Run)] --> [Python/Pandas Transform] --> [BigQuery Table] -->

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [How It Works (Pipeline Stages)](#how-it-works-pipeline-stages)
- [Project Structure](#project-structure)
- [Setup and Usage](#setup-and-usage)
    - [GCP Prerequisites](#gcp-prerequisites)
    - [Local Project Setup](#local-project-setup)
    - [Cloud Function (Gen 2) Deployment](#cloud-function-gen-2-deployment)
    - [Testing the Pipeline](#testing-the-pipeline)
- [Sample Input & Output](#sample-input--output)
    - [Sample Input CSV (`user_events_input.csv`)](#sample-input-csv-user_events_inputcsv)
    - [Sample Transformed Output (in BigQuery)](#sample-transformed-output-in-bigquery)
- [Learnings & Key Takeaways](#learnings--key-takeaways)
- [Future Considerations & Next Steps](#future-considerations--next-steps)

## Overview
This project implements an automated, serverless data pipeline on GCP. When a new CSV file containing user event data is uploaded to a specified Google Cloud Storage (GCS) bucket, it triggers a Gen 2 Python Cloud Function. This function, **running as a service on Google Cloud Run**, performs the following:
1.  Downloads the CSV content from GCS.
2.  Performs data cleaning and transformations using the pandas library (e.g., standardizing country codes, handling missing timestamps, deriving new features).
3.  Loads the processed, structured data into a predefined table in Google BigQuery.

The primary goal was to gain hands-on experience with modern serverless data engineering on GCP, event-driven architectures, and the integration of key services like GCS, Cloud Functions (Gen 2)/Cloud Run, and BigQuery.

## Features
-   **Automated Trigger:** Event-driven execution upon new file uploads to a GCS bucket (managed by Eventarc).
-   **Serverless Processing:** Utilizes GCP Cloud Functions (Gen 2), which are **executed as containerized applications on Google Cloud Run**, for scalable, cost-effective data transformation without managing servers.
-   **Data Transformation:** Leverages the pandas library in Python for flexible data manipulation.
-   **Data Warehousing:** Loads structured, transformed data into Google BigQuery for analytics and querying.
-   **Reproducible Setup:** Includes detailed setup instructions and dependency management (`requirements.txt`).

## Technologies Used
-   **Cloud Provider:** Google Cloud Platform (GCP)
-   **Core GCP Services:**
    -   Google Cloud Storage (GCS): For input file storage and triggering events.
    -   **GCP Cloud Functions (Gen 2):** Provides the developer experience for event-driven functions.
    -   **Google Cloud Run:** The underlying serverless platform that executes the Gen 2 Cloud Function's container.
    -   Google BigQuery: For data warehousing and analytics.
    -   Eventarc: For managing event-driven triggers for Cloud Functions (Gen 2) / Cloud Run services.
    -   Cloud Build: Used by Cloud Functions for building deployment artifacts (container images).
    -   Artifact Registry: Used by Cloud Build and Cloud Run to store container images.
    -   IAM (Identity and Access Management): For managing service account permissions.
-   **Language:** Python 3.11
-   **Key Python Libraries:**
    -   `pandas` (for data manipulation)
    -   `google-cloud-storage` (GCP GCS client)
    *   `google-cloud-bigquery` (GCP BigQuery client)
    *   `functions-framework` (for GCP Cloud Functions in Python)
    *   `pyarrow` (dependency for efficient DataFrame loading to BigQuery)
-   **Development Environment:**
    -   Python Virtual Environment (`gcp2env` or `venv`)
    -   `pip` for package management
-   **Version Control:** Git & GitHub
-   **Local Terminal:** PowerShell (for `gcloud` CLI and script execution)

## How It Works (Pipeline Stages)
1.  **File Upload & Trigger:** A user or process uploads a CSV file to the designated input GCS bucket.
2.  **Event Notification & Routing:** GCS detects the new object (`object.finalized` event). **Eventarc** captures this event and routes it to the appropriate target.
3.  **Cloud Function (on Cloud Run) Invocation:** Eventarc triggers the deployed Python Cloud Function, which is hosted and executed as a **Google Cloud Run service**.
4.  **Data Download (in Cloud Function):** The function's Python script (`main.py`):
    -   Receives event metadata.
    -   Uses the `google-cloud-storage` client to download the CSV file's content from GCS.
5.  **Data Transformation (in Cloud Function):**
    -   The CSV content is loaded into a pandas DataFrame.
    -   Data cleaning and transformations are applied (timestamp standardization, country code mapping, derived features, etc.).
    -   The DataFrame columns are prepared to match the target BigQuery table schema.
6.  **Data Loading (in Cloud Function):**
    -   The transformed pandas DataFrame is loaded into the target table in BigQuery using the `google-cloud-bigquery` client library.
7.  **Logging & Monitoring:** The Cloud Function (running on Cloud Run) logs its progress and any errors to Cloud Logging, viewable under both the Cloud Functions and Cloud Run interfaces for the service.

## Project Structure
gcp-data-pipeline-project/
├── .git/
├── gcp2env/ # Python virtual environment (in .gitignore)
├── cloud_function_source/ # Source code for the Cloud Function
│ ├── main.py # The Python function logic
│ └── requirements.txt # Python dependencies for the function
├── sample_data/ # Sample input data
│ └── user_events_input.csv
├── .gitignore
├── README.md # This file
└── (root requirements.txt for local dev, optional)

## Setup and Usage

### GCP Prerequisites
1.  **GCP Project:** Active project with billing enabled.
2.  **Enabled APIs:** Cloud Functions API, Cloud Storage API, BigQuery API, Cloud Build API, **Eventarc API**, **Cloud Run API**, Artifact Registry API, IAM API.
3.  **GCS Bucket:** A globally unique GCS bucket in a specific region.
4.  **BigQuery Dataset & Table:** A BigQuery Dataset in the same region as the GCS bucket, and a target Table with the [specified schema](#sample-input--output).
5.  **IAM Permissions:** Correct IAM roles assigned to:
    *   Eventarc Service Agent (e.g., `Storage Admin` on the GCS bucket).
    *   Cloud Storage Service Agent (e.g., `Pub/Sub Publisher` on the project).
    *   Cloud Function's runtime service account (e.g., Compute Engine default SA with `BigQuery Data Editor`).

### Local Project Setup
1.  **`gcloud` CLI:** Installed, configured, and authenticated (`gcloud auth login`, `gcloud auth application-default login`, `gcloud config set project YOUR_PROJECT_ID`).
2.  **Clone Repository (if applicable):** `git clone <YOUR_REPO_URL>` & `cd <repo-name>`
3.  **Virtual Environment:** `python -m venv gcp2env` & activate (`.\gcp2env\Scripts\activate` on PowerShell).
4.  **Install Dependencies (for function):** `pip install -r cloud_function_source/requirements.txt` (if testing locally with Functions Framework, otherwise deployment handles it).

### Cloud Function (Gen 2) Deployment
1.  **Navigate to Project Root.**
2.  **Modify `cloud_function_source/main.py`:** Update `TARGET_BIGQUERY_PROJECT_ID`.
3.  **Deploy:**
    ```powershell
    gcloud functions deploy your-function-name `
      --gen2 `
      --runtime python311 `
      --region your-gcp-region `
      --source ./cloud_function_source/ `
      --entry-point process_gcs_csv_to_bq `
      --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" `
      --trigger-event-filters="bucket=your-gcs-input-bucket-name" `
      --memory=512MiB `
      --timeout=300s
    ```
    *(Replace placeholders. Proxy for `gcloud` may be needed depending on network.)*

### Testing the Pipeline
1.  **Upload CSV to the GCS input bucket.**
2.  **Monitor Logs:** Check Cloud Logging for your function (often accessible via Cloud Run service logs or Cloud Functions UI links) for execution details and errors.
3.  **Verify Data in BigQuery:** Check the target table for transformed data.

## Sample Input & Output

### Sample Input CSV (`user_events_input.csv`)
```csv
user_id,event_timestamp,country,value
user123,2025-05-12T11:00:00Z,Malaysia,50.50
user456,2025-05-12T11:05:00Z,Singapore,120.00
user789,2025-05-12T11:10:00Z,malaysia,15.75
userxyz,,Other,10.00
```

### Sample Transformed Output (in BigQuery transformed_user_events table)
```
user_id	event_timestamp	country_code	value	is_high_value	processing_datetime
user123	2025-05-13 11:00:00 UTC	MY	50.5	false	2025-05-13TXX:XX:XX.XXXXXX
user456	2025-05-13 11:05:00 UTC	SG	120.0	true	2025-05-13TXX:XX:XX.XXXXXX
user789	2025-05-13 11:10:00 UTC	MY	15.75	false	2025-05-13TXX:XX:XX.XXXXXX
userxyz	2025-05-13 XX:XX:XX UTC	OT	10.0	false	2025-05-13TXX:XX:XX.XXXXXX
```
(Note: XX:XX:XX.XXXXXX for processing_datetime and userxyz's event_timestamp will be the actual processing time.)					

## Learnings & Key Takeaways
**Serverless Architecture with GCP:** Practical experience with event-driven serverless pipelines.

**Cloud Functions Gen 2 & Cloud Run:** Understood that Gen 2 Cloud Functions are executed on Cloud Run, leveraging its container-based infrastructure.

**GCP Service Integration:** Successfully integrated GCS, Cloud Functions/Cloud Run, Eventarc, and BigQuery.

**Data Transformation with Pandas in Cloud:** Applied pandas for data manipulation in a serverless function.

**IAM & Service Account Permissions:** Gained critical experience in configuring IAM roles for secure inter-service communication.

**Troubleshooting Cloud Deployments:** Diagnosed and resolved issues related to dependencies (pyarrow), data type conversions for BigQuery, Cloud Function memory limits, and trigger configurations.

**Importance of Detailed Logging:** Recognized the value of comprehensive logging within the function for debugging.

## Future Considerations & Next Steps
**Enhanced Error Handling:** Implement a dead-letter queue (DLQ) or move failed files to an error bucket.

**Data Validation:** Add explicit data validation steps (e.g., using Pandera).

**Monitoring & Alerting:** Set up Cloud Monitoring for pipeline health.

**Idempotency:** Ensure the pipeline can handle re-processing of the same file without duplication.

**Security Hardening:** Further refine IAM permissions to adhere strictly to the principle of least privilege.
