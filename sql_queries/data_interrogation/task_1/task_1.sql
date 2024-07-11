SELECT country_code AS country,
	COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country
ORDER BY COUNT(*) DESC
LIMIT 3;