WITH base AS (
    SELECT
        location,
        salary_min,
        salary_max,
        company
    FROM {{ source('job_postings', 'jobs') }}
)
SELECT
    location,
    COUNT(*) AS total_jobs,
    AVG(salary_min) AS avg_salary_min,
    AVG(salary_max) AS avg_salary_max,
    COUNT(DISTINCT company) AS unique_companies
FROM base
GROUP BY location
ORDER BY total_jobs DESC
