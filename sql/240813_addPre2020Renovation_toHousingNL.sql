-- Step 1: Create table housing_nl if it doesn't exist
DROP TABLE IF EXISTS housing_nl; 
CREATE TABLE IF NOT EXISTS housing_nl (
	function TEXT,
	sqm BIGINT,
	id_pand VARCHAR,
	geometry VARCHAR,
	build_year VARCHAR,
	status VARCHAR,
	document_date VARCHAR,
	document_number VARCHAR,
	registration_start VARCHAR,
	registration_end VARCHAR,
	geom GEOMETRY,
	geom_28992 GEOMETRY,
	neighborhood_code VARCHAR,
	neighborhood VARCHAR,
	municipality VARCHAR,
	province VARCHAR
);

-- Delete rows where municipality = 'Delft' AND status = 'renovation - pre2020'
DELETE FROM housing_nl
WHERE 
	municipality = 'Delft' 
	AND status = 'renovation - pre2020';

-- Insert rows from the query into housing_nl
WITH bag_pand_municipality AS (
	SELECT * FROM bag_pand
	WHERE 
		municipality = 'Delft' 
		AND LEFT(registration_start, 4)::INTEGER < 2020
), 
ranked_pand AS (
    SELECT *,
           LAG(status) OVER (PARTITION BY id_pand ORDER BY registration_start) AS previous_status
    FROM bag_pand_municipality
), 
pand_renovations AS (
	SELECT 
		id_pand, geometry::TEXT,  -- Casting geometry to text to match table column
		build_year::TEXT,         -- Casting build_year to text to match table column
		'renovation - pre2020' AS status, 
		document_date::TEXT, 
		document_number, 
		registration_start::TEXT, 
		registration_end::TEXT, 
		geom, geom_28992, 
		neighborhood_code, neighborhood, municipality, province
	FROM ranked_pand
	WHERE 
		status = 'Pand in gebruik (niet ingemeten)' 
		AND previous_status IS NOT NULL 
		AND previous_status = 'Pand in gebruik'
), 
housing_units_inuse AS (
	SELECT * 
	FROM bag_vbo 
	WHERE 
		municipality = 'Delft' 
		AND status = 'Verblijfsobject in gebruik'
		AND function = 'woonfunctie'
		AND sqm::INTEGER < 9999
), 
housing_units_unique AS (
	SELECT DISTINCT ON (id_vbo) * 
	FROM housing_units_inuse
), 
housing_sqm AS (
	SELECT id_pand, SUM(sqm::INTEGER) AS sqm
	FROM housing_units_unique
	GROUP BY id_pand
), 
housing_sqm_function AS (
	SELECT 'woonfunctie' AS function, *
	FROM housing_sqm
)
INSERT INTO housing_nl (
	function, sqm, id_pand, geometry, build_year, status, 
	document_date, document_number, registration_start, registration_end, 
	geom, geom_28992, neighborhood_code, neighborhood, municipality, province
)
SELECT 
	h.function, h.sqm, 
	r.id_pand, r.geometry, r.build_year, r.status, 
	r.document_date, r.document_number, r.registration_start, r.registration_end, 
	r.geom, r.geom_28992, r.neighborhood_code, r.neighborhood, r.municipality, r.province
FROM pand_renovations r
LEFT JOIN housing_sqm_function h
ON r.id_pand = h.id_pand