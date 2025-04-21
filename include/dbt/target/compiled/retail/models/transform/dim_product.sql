-- dim_product.sql

-- Create product table dimension with UnitPrice > 0

-- StockCode is not unique, that mean a product with a same id can have different price.

SELECT DISTINCT
    to_hex(md5(cast(coalesce(cast(StockCode as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Description as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(UnitPrice as string), '_dbt_utils_surrogate_key_null_') as string))) AS product_id,
    StockCode AS stock_code,
    Description AS description,
    UnitPrice AS unit_price
FROM `dbt-project-431506`.`retail`.`raw_invoices`
WHERE StockCode IS NOT NULL
AND UnitPrice > 0