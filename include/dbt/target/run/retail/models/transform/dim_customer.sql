
  
    

    create or replace table `dbt-project-431506`.`retail`.`dim_customer`
      
    
    

    OPTIONS()
    as (
      -- dim_customer.sql

-- create customer dimension table

WITH customer_cte AS (
    SELECT DISTINCT
        to_hex(md5(cast(coalesce(cast(CustomerID as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as string), '_dbt_utils_surrogate_key_null_') as string))) AS customer_id,
        Country as country
    FROM `dbt-project-431506`.`retail`.`raw_invoices`
    WHERE CustomerID IS NOT NULL
)
SELECT t.*, cm.iso
FROM customer_cte t
LEFT JOIN `dbt-project-431506`.`retail`.`country` cm
ON t.country = cm.nicename
    );
  