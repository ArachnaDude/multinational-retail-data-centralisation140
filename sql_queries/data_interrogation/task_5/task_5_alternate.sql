SELECT dim_store_details.store_type,
ROUND(SUM(CAST(orders_table.product_quantity AS NUMERIC) * CAST(dim_products.product_price AS NUMERIC)) , 2) as total_sales,
ROUND(CAST(COUNT(*) AS NUMERIC) / CAST((SELECT COUNT(*) FROM orders_table) AS NUMERIC) * 100, 2) as "percentage_total(%)"
-- COUNT(*) as count,
-- (SELECT COUNT(*) FROM orders_table),
-- COUNT(*) / (SELECT COUNT(*) FROM orders_table)
FROM orders_table
LEFT JOIN dim_products ON orders_table.product_code = dim_products.product_code
LEFT JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY store_type
ORDER BY total_sales DESC;