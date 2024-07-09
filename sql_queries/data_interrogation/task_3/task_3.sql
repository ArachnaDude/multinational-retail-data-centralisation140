SELECT ROUND(CAST(SUM(pr.product_price * o.product_quantity) AS NUMERIC), 2) AS total_sales, dt.month
FROM dim_date_times AS dt
LEFT JOIN orders_table AS o ON dt.date_uuid = o.date_uuid
LEFT JOIN dim_products AS pr ON pr.product_code = o.product_code
GROUP BY dt.month
ORDER BY total_sales DESC
LIMIT 6;