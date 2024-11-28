import os
import time

import requests
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

# Adzuna API credentials
API_ID = os.environ.get("API_ID")
API_KEY = os.environ.get("API_KEY")

# API endpoint and parameters
API_URL = "http://api.adzuna.com/v1/api/jobs/fr/search"
params = {
    "app_id": API_ID,
    "app_key": API_KEY,
    "results_per_page": 20,
    "what": "data engineer",
    "content-type": "application/json"
}

# Function to get data from Adzuna API at a specific page
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

# Function to get all the data from Adzuna API
def fetch_job_data_all_pages():
    results = []
    for i in range(1, 101):
        results += fetch_job_data_page(i)
    return results

# Parsing and structuring the job data
def parse_job_data(results):
    job_list = []
    for job in results:
        job_data = {
            "job_id": job.get("id"),
            "title": job.get("title"),
            "company": job.get("company").get("display_name"),
            "location": job.get("location").get("display_name"),
            "latitude": job.get("latitude"),
            "longitude": job.get("longitude"),
            "salary_min": job.get("salary_min"),
            "salary_max": job.get("salary_max"),
            "contract_type": job.get("contract_type"),
            "description": job.get("description"),
            "category": job.get("category").get("label"),
            "created": job.get("created"),
            "redirect_url": job.get("redirect_url")
        }
        job_list.append(job_data)
    return pd.DataFrame(job_list)

# Save the data as CSV for loading to Snowflake or direct insertion
def save_data_to_csv(df, filename="job_data.csv"):
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

# Main function to execute the process
def main():
    results = fetch_job_data_all_pages()
    if results:
        df = parse_job_data(results)
        save_data_to_csv(df)

# Run the main function
if __name__ == "__main__":
    main()
