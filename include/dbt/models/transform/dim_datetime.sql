-- dim_datetime.sql

-- Create datetime dimension table

WITH datetime_cte AS (
    SELECT DISTINCT InvoiceDate AS datetime_id,
    CASE
        WHEN LENGTH(InvoiceDate) = 16 THEN
            -- Format datetime as: "MM/DD/YYYY HH:MM"
            PARSE_DATETIME('%m/%d/%Y %H:%M', InvoiceDate)
        WHEN LENGTH(InvoiceDate) <= 14 THEN
            -- Format datetime as: "MM/DD/YY HH:MM"
            PARSE_DATETIME('%m/%d/%y %H:%M', InvoiceDate)
        ELSE
            NULL
        END AS processed_datetime
    FROM {{ source('retail', 'raw_invoices') }}
    WHERE InvoiceDate IS NOT NULL
)
SELECT datetime_id,
processed_datetime AS datetime,
EXTRACT(YEAR FROM processed_datetime) AS year,
EXTRACT(MONTH FROM processed_datetime) AS month,
EXTRACT(DAY FROM processed_datetime) AS day,
EXTRACT(HOUR FROM processed_datetime) AS hour,
EXTRACT(MINUTE FROM processed_datetime) AS min,
EXTRACT(DAYOFWEEK FROM processed_datetime) AS weekday
FROM datetime_cte