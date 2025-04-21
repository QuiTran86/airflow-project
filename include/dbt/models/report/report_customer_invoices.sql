SELECT dc.country, dc.iso,
COUNT(fi.invoice_id) AS total_invoices,
SUM(fi.total) AS total_revenues
FROM {{ ref('fct_invoices') }} fi
JOIN {{ ref('dim_customer') }} dc
ON fi.customer_id = dc.customer_id
GROUP BY dc.country, dc.iso
ORDER BY total_revenues DESC
LIMIT 10