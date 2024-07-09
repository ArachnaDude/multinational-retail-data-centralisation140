SELECT
	COUNT(o.*) AS number_of_sales,
	SUM(o.product_quantity) AS product_quantity_count,
	CASE
		WHEN sd.store_type = 'Web Portal' THEN 'Web' ELSE 'Offline' END AS location
FROM dim_store_details AS sd
LEFT JOIN orders_table AS o ON sd.store_code = o.store_code
LEFT JOIN dim_products AS p ON p.product_code = o.product_code

GROUP BY location
	ORDER BY location DESC;