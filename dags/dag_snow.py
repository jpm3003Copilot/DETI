import os
import time

import pandas as pd
import numpy as np
import requests

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

np.set_printoptions(linewidth=100000)

# Load environment variables
load_dotenv()

# Adzuna API credentials
API_ID = os.environ.get("API_ID")
API_KEY = os.environ.get("API_KEY")

# Snowflake constants
TABLE_NAME = "JOBS_STAGING"
SNOWFLAKE_CONN_ID = "my_snowflake_conn"  # Defined in Airflow connections

# Constants
API_URL = "http://api.adzuna.com/v1/api/jobs/fr/search"
params = {
    "app_id": API_ID,
    "app_key": API_KEY,
    "results_per_page": 20,
    "what": "data engineer",
    "content-type": "application/json"
}

# Default args for Airflow
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Airflow DAG definition
dag = DAG(
    "adzuna_daily_job_ingestion_snowflake_operator",
    default_args=default_args,
    description="Fetch job postings from Adzuna and insert into Snowflake using SnowflakeOperator",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

def fetch_job_data_page(page):
    response = requests.get(f"{API_URL}/{page}", params=params)
    if response.status_code == 200:
        data = response.json()
        return data['results']
    else:
        print("Failed to fetch data:", response.status_code, "from page:", page)
        time.sleep(1)
        response = requests.get(f"{API_URL}/{page}", params=params)
        if response.status_code == 200:
            data = response.json()
            return data['results']
        else:
            print("Failed again to fetch data:", response.status_code, "from page:", page)
            return []
        
def fetch_and_save_to_csv():
    """Fetch job postings from Adzuna API and save to a local CSV.""" 
    results = []
    for page in range(1, 101):  # Fetch up to 100 pages
        results += fetch_job_data_page(page)

    job_list = [
        {
            "job_id": job.get("id"),
            "title": job.get("title"),
            "company": job.get("company", {}).get("display_name"),
            "location": job.get("location", {}).get("display_name"),
            "latitude": job.get("latitude"),
            "longitude": job.get("longitude"),
            "salary_min": job.get("salary_min"),
            "salary_max": job.get("salary_max"),
            "contract_type": job.get("contract_type"),
            "description": job.get("description"),
            "category": job.get("category", {}).get("label"),
            "created": job.get("created"),
            "redirect_url": job.get("redirect_url"),
        }
        for job in results
    ]

    # Save to CSV
    df = pd.DataFrame(job_list)
    csv_file = "/opt/airflow/data/job_data.csv"
    df.to_csv(csv_file, index=False)
    print(f"Data saved to {csv_file}")

# SQL to create schema and tables
create_schema_and_tables_sql = """
CREATE SCHEMA IF NOT EXISTS job_postings;

CREATE STAGE IF NOT EXISTS my_stage
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

CREATE TABLE IF NOT EXISTS job_postings.jobs (
    job_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    company VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    salary_min FLOAT,
    salary_max FLOAT,
    contract_type VARCHAR,
    description TEXT,
    category VARCHAR,
    created TIMESTAMP,
    redirect_url VARCHAR,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE TABLE job_postings.jobs_staging LIKE job_postings.jobs;

CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
NULL_IF = ('', 'NULL');
"""

# SQL to copy and merge data into Snowflake
copy_and_merge_into_sql = f"""
COPY INTO job_postings.jobs_staging (job_id, title, company, location, latitude, longitude, salary_min, salary_max, contract_type, description, category, created, redirect_url)
FROM @my_stage/job_data
FILE_FORMAT = my_csv_format;

MERGE INTO job_postings.jobs AS target
USING job_postings.jobs_staging AS source
ON target.job_id = source.job_id
WHEN MATCHED THEN
    UPDATE SET
        title = source.title,
        company = source.company,
        location = source.location,
        latitude = source.latitude,
        longitude = source.longitude,
        salary_min = source.salary_min,
        salary_max = source.salary_max,
        contract_type = source.contract_type,
        description = source.description,
        category = source.category,
        created = source.created,
        redirect_url = source.redirect_url,
        updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        job_id, title, company, location, latitude, longitude, salary_min, 
        salary_max, contract_type, description, category, created, redirect_url, 
        inserted_at
    )
    VALUES (
        source.job_id, source.title, source.company, source.location, 
        source.latitude, source.longitude, source.salary_min, source.salary_max, 
        source.contract_type, source.description, source.category, source.created, 
        source.redirect_url, CURRENT_TIMESTAMP
    );

TRUNCATE TABLE job_postings.jobs_staging;

"""

# Upload the CSV to Snowflake stage
def upload_csv_to_snowflake():
    import snowflake.connector

    # Snowflake connection details
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    # Specify the stage and file
    stage_name = "@MY_STAGE"
    file_path = "/opt/airflow/data/job_data.csv"  # Update with the correct path

    # Execute the PUT command
    with conn.cursor() as cur:
        cur.execute(f"PUT 'file://{file_path}' {stage_name};")
        print("File uploaded to stage successfully.")

# Task 1: Fetch data from API and save to CSV
fetch_data_task = PythonOperator(
    task_id="fetch_data_from_api",
    python_callable=fetch_and_save_to_csv,
    dag=dag,
)

# Task 2: Create the Snowflake table
create_schema_and_tables_task = SnowflakeOperator(
    task_id="create_snowflake_schema_and_tables",
    sql=create_schema_and_tables_sql,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag,
)

# Task 3: Upload the CSV to Snowflake stage
upload_csv_task = PythonOperator(
    task_id="upload_csv_to_snowflake_stage",
    python_callable=upload_csv_to_snowflake,
    dag=dag,
)

# Task 4: Copy data into Snowflake table
copy_and_merge_into_task = SnowflakeOperator(
    task_id="copy_and_merge_data_to_snowflake",
    sql=copy_and_merge_into_sql,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag,
)

# Set task dependencies
fetch_data_task >> create_schema_and_tables_task >> upload_csv_task >> copy_and_merge_into_task
