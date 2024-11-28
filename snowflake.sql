CREATE SCHEMA job_postings;

CREATE OR REPLACE TABLE job_postings.jobs (
    job_id VARCHAR PRIMARY KEY,                      -- Unique identifier for the job
    title VARCHAR,                                   -- Job title
    company VARCHAR,                                 -- Name of the hiring company
    location VARCHAR,                                -- Display name of the location
    latitude FLOAT,                                  -- Latitude of job location
    longitude FLOAT,                                 -- Longitude of job location
    salary_min FLOAT,                                -- Minimum salary
    salary_max FLOAT,                                -- Maximum salary
    contract_type VARCHAR,                           -- Contract type (e.g., permanent)
    description TEXT,                                -- Full job description
    category VARCHAR,                                -- Job category
    created TIMESTAMP,                               -- Date job was posted
    redirect_url VARCHAR,                            -- URL to the job posting
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Ingestion timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- Last update timestamp
);

CREATE OR REPLACE TABLE job_postings.jobs_staging LIKE job_postings.jobs;

CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
NULL_IF = ('', 'NULL');

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