
  
    

    create or replace table `dbt-project-431506`.`retail`.`report_customer_invoices`
      
    
    

    OPTIONS()
    as (
      SELECT dc.country, dc.iso,
COUNT(fi.invoice_id) AS total_invoices,
SUM(fi.total) AS total_revenues
FROM `dbt-project-431506`.`retail`.`fct_invoices` fi
JOIN `dbt-project-431506`.`retail`.`dim_customer` dc
ON fi.customer_id = dc.customer_id
GROUP BY dc.country, dc.iso
ORDER BY total_revenues DESC
LIMIT 10
    );
  