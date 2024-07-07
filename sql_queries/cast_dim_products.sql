ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(14);

UPDATE dim_products
SET weight_class = CASE
WHEN weight_kg < 2 THEN 'Light'
WHEN weight_kg >= 2 AND weight_kg < 40 THEN 'Mid-Sized'
WHEN weight_kg >= 40 AND weight_kg < 140 THEN 'Heavy'
WHEN weight_kg >= 140 THEN 'Truck_Required'
END;

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE DOUBLE PRECISION
USING product_price::double precision;

ALTER TABLE dim_products
ALTER COLUMN weight_kg TYPE DOUBLE PRECISION
USING weight_kg::double precision;

ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(17);

ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE
USING date_added::date;

ALTER TABLE dim_products
ALTER COLUMN "uuid" TYPE UUID
USING "uuid"::uuid;

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOLEAN
USING (CASE
WHEN still_available = 'Still_avaliable' THEN TRUE
	ELSE FALSE
END);