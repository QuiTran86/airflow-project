
  
    

    create or replace table `dbt-project-431506`.`retail`.`report_year_invoices`
      
    
    

    OPTIONS()
    as (
      SELECT dt.year, dt.month,
COUNT(DISTINCT fi.invoice_id) AS num_invoices,
SUM(fi.total) AS total_revenues
FROM `dbt-project-431506`.`retail`.`dim_datetime` dt
JOIN `dbt-project-431506`.`retail`.`fct_invoices` fi ON fi.datetime_id = dt.datetime_id
GROUP BY dt.year, dt.month
ORDER BY dt.year, dt.month
    );
  