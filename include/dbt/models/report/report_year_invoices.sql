SELECT dt.year, dt.month,
COUNT(DISTINCT fi.invoice_id) AS num_invoices,
SUM(fi.total) AS total_revenues
FROM {{ ref('dim_datetime') }} dt
JOIN {{ ref('fct_invoices') }} fi ON fi.datetime_id = dt.datetime_id
GROUP BY dt.year, dt.month
ORDER BY dt.year, dt.month