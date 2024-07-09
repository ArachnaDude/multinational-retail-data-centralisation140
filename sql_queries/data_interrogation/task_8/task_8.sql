SELECT 
	ROUND(CAST(SUM(p.product_price * o.product_quantity) AS NUMERIC), 2) AS total_sales,
	sd.store_type, 
	sd.country_code
FROM dim_store_details AS sd
LEFT JOIN orders_table AS o ON o.store_code = sd.store_code
LEFT JOIN dim_products AS p ON p.product_code = o.product_code
WHERE sd.country_code = 'DE'
GROUP BY sd.country_code, sd.store_type
ORDER BY total_sales ASC;