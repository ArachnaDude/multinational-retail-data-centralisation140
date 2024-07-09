SELECT 
	ROUND(CAST(SUM(p.product_price * o.product_quantity) AS NUMERIC), 2) AS total_sales, 
	dt.year, 
	dt.month
FROM dim_date_times AS dt
LEFT JOIN orders_table AS o ON o.date_uuid = dt.date_uuid
LEFT JOIN dim_products AS p ON p.product_code = o.product_code
GROUP BY dt.year, dt.month
ORDER BY total_sales DESC
LIMIT 10;