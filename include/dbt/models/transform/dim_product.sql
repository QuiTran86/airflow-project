-- dim_product.sql

-- Create product table dimension with UnitPrice > 0

-- StockCode is not unique, that mean a product with a same id can have different price.

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['StockCode', 'Description', 'UnitPrice']) }} AS product_id,
    StockCode AS stock_code,
    Description AS description,
    UnitPrice AS unit_price
FROM {{ source('retail', 'raw_invoices') }}
WHERE StockCode IS NOT NULL
AND UnitPrice > 0