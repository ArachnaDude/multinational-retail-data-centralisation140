UPDATE dim_store_details
SET latitude = NULL
WHERE store_type = 'Web Portal';

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE DOUBLE PRECISION
USING latitude::double precision;

UPDATE dim_store_details
SET longitude = NULL
WHERE store_type = 'Web Portal';

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE DOUBLE PRECISION
USING longitude::double precision;

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT;

ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE;

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255);

UPDATE dim_store_details
SET address = NULL
WHERE store_type = 'Web Portal';

UPDATE dim_store_details
SET locality = NULL
WHERE store_type = 'Web Portal';

UPDATE dim_store_details
SET country_code = NULL
WHERE store_type = 'Web Portal';

UPDATE dim_store_details
SET continent = NULL
WHERE store_type = 'Web Portal';