WITH cte AS

(
SELECT
	year,
TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS' ) AS purchase_time
FROM dim_date_times
ORDER BY purchase_time DESC
),

	cte2 AS (
SELECT
	year,
	purchase_time,
LEAD(purchase_time, 1) OVER(
ORDER BY purchase_time DESC
) AS next_purchase_time
FROM cte
	),

	cte3 AS (
SELECT 
	year, 
	AVG(purchase_time - next_purchase_time) AS difference FROM cte2
	GROUP BY year
	ORDER BY difference DESC
	)
	
SELECT
	year,
	CONCAT(
		'"hours": ', EXTRACT(HOUR FROM difference), ', ' ,
		' "minutes": ', EXTRACT(MINUTE FROM difference), ', ',
		' "seconds": ', FLOOR(EXTRACT(SECOND FROM difference)), ', ',
		' "milliseconds": ', ROUND((EXTRACT(SECOND FROM difference) - FLOOR(EXTRACT(SECOND FROM difference))) * 1000)
		) AS total_time
	
	FROM cte3

LIMIT 5;