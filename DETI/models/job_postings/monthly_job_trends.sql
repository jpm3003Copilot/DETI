WITH base AS (
    SELECT
        DATE_TRUNC('month', created) AS job_month,
        COUNT(*) AS total_jobs
    FROM {{ source('job_postings', 'jobs') }}
    WHERE created >= DATEADD('year', -1, CURRENT_DATE)
    GROUP BY job_month
)
SELECT
    job_month,
    total_jobs
FROM base
ORDER BY job_month ASC
