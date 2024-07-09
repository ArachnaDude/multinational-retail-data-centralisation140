SELECT 
	sd.store_type, 
	ROUND(CAST(SUM(p.product_price * o.product_quantity) AS NUMERIC), 2) AS total_sales,

	ROUND((
	CAST(SUM(p.product_price * o.product_quantity) AS NUMERIC) / 	
	CAST(SUM(SUM(p.product_price * o.product_quantity)) OVER () AS NUMERIC)
	) * 100, 2)
	AS "percentage_total(%)"
	
FROM dim_store_details AS sd
LEFT JOIN orders_table AS o ON sd.store_code = o.store_code
LEFT JOIN dim_products AS p ON p.product_code = o.product_code
GROUP BY sd.store_type
ORDER BY total_sales DESC;