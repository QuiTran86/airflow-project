SELECT dp.product_id, dp.stock_code, dp.description,
SUM(fi.quantity) AS total_quantity_sold
FROM {{ ref('fct_invoices') }} fi
JOIN {{ ref('dim_product') }} dp ON fi.product_id = dp.product_id
GROUP BY dp.product_id, dp.stock_code, dp.description
ORDER BY total_quantity_sold DESC
LIMIT 10