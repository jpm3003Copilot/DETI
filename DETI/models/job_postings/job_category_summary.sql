WITH base AS (
    SELECT
        category,
        salary_min,
        salary_max
    FROM {{ source('job_postings', 'jobs') }}
)
SELECT
    category,
    COUNT(*) AS total_jobs,
    AVG(salary_min) AS avg_salary_min,
    AVG(salary_max) AS avg_salary_max
FROM base
GROUP BY category
ORDER BY total_jobs DESC
