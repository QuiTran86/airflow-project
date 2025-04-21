SELECT dp.product_id, dp.stock_code, dp.description,
SUM(fi.quantity) AS total_quantity_sold
FROM `dbt-project-431506`.`retail`.`fct_invoices` fi
JOIN `dbt-project-431506`.`retail`.`dim_product` dp ON fi.product_id = dp.product_id
GROUP BY dp.product_id, dp.stock_code, dp.description
ORDER BY total_quantity_sold DESC
LIMIT 10