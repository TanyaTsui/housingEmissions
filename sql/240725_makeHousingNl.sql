/* 
MAKE HOUSING_NL TABLE 
housing_nl is a table showing all the buildings in NL that have a housing function (woonfunctie). 
it contains information on sqm, function, geometry, registration, and status of the building. 

steps: 
- get relevant units and buildings from bag_pand and bag_vbo 
- extract buildings with 'woonfunctie' as function and get their sqm 
- for buildings with no function (NULL), guess function by looking at units with 50m radius 
*/ 

-- -- create indexes to optimize query - indexes already created, no need to run again
-- CREATE INDEX IF NOT EXISTS idx_vbo_status_sqm ON bag_vbo (status, CAST(sqm AS INTEGER));
-- CREATE INDEX IF NOT EXISTS idx_pand_status ON bag_pand (status);
-- CREATE INDEX IF NOT EXISTS idx_vbo_id_pand_function ON bag_vbo (id_pand, function);
-- CREATE INDEX IF NOT EXISTS idx_vbo_municipality ON bag_vbo (municipality); 
-- CREATE INDEX IF NOT EXISTS idx_pand_municipality ON bag_pand (municipality); 
-- CREATE INDEX IF NOT EXISTS idx_vbo_neighborhood_code ON bag_vbo (neighborhood_code); 
-- CREATE INDEX IF NOT EXISTS idx_pand_neighborhood_code ON bag_pand (neighborhood_code); 


-- create table housing_nl
DROP TABLE IF EXISTS housing_delft; 
CREATE TABLE housing_delft (
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
    geom GEOMETRY,             -- Assuming GEOMETRY is the intended type
    geom_28992 GEOMETRY,       -- Assuming GEOMETRY is the intended type
    neighborhood_code VARCHAR, -- Added column
    neighborhood VARCHAR,      -- Added column
    municipality VARCHAR,
    province VARCHAR
);

INSERT INTO housing_delft (
    function, sqm, id_pand, geometry, build_year, status, 
    document_date, document_number, registration_start, registration_end, 
    geom, geom_28992, neighborhood_code, neighborhood, municipality, province
)
	
-- select relevant units and buildings from BAG
WITH bag_vbo_sample AS (
	SELECT *
	FROM bag_vbo
	WHERE municipality = 'Delft'
), 
units AS (
	SELECT DISTINCT ON (id_vbo) * 
	FROM bag_vbo_sample
	WHERE 
		status = 'Verblijfsobject in gebruik' AND sqm::INTEGER < 9999
	ORDER BY id_vbo, status, registration_start, document_number
), 
bag_pand_sample_without_reno AS (
	SELECT * 
	FROM bag_pand_delft
	WHERE municipality = 'Delft'
), 
bag_pand_sample AS (
	SELECT 
		id_pand, geometry, build_year, 
		CASE 
			WHEN renovation = TRUE THEN 'Verbouwing pand'
			ELSE status
		END AS status,
		document_date, document_number, 
		registration_start, registration_end, geom, geom_28992, 
		neighborhood_code, neighborhood, municipality, province
	FROM bag_pand_sample_without_reno
), 
buildings_construction_demolition AS (
	SELECT DISTINCT ON (id_pand, status) * 
	FROM bag_pand_sample
	WHERE status IN ('Bouw gestart', 'Pand gesloopt')
	ORDER BY id_pand, status, registration_start, document_number
), 
buildings_renovation AS (
	SELECT * 
	FROM bag_pand_sample
	WHERE status = 'Verbouwing pand'
), 
buildings AS (
	SELECT * FROM buildings_construction_demolition
	UNION ALL 
	SELECT * FROM buildings_renovation
), 

-- get buildings with unknown function
buildings_unknown AS (
	SELECT b.*
	FROM buildings b
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM units u
	    WHERE u.id_pand = b.id_pand
	)
	ORDER BY b.id_pand, b.status, b.registration_start, b.document_number
),

-- find units within 50m radius of buildings_unknown
units_near_unknown AS (
    SELECT b.id_pand AS id_pand_b, u.*
    FROM units u
    JOIN buildings_unknown b 
	ON ST_DWithin(u.geom_28992, b.geom_28992, 50) AND u.neighborhood_code = b.neighborhood_code
), 
units_near_unknown_grouped AS (
	SELECT 
	    id_pand_b,
	    function,
	    COUNT(*) AS function_count
	FROM units_near_unknown
	GROUP BY id_pand_b, function
), 
units_near_unknown_ranked AS (
    SELECT 
        id_pand_b,
        function,
        function_count,
        ROW_NUMBER() OVER (PARTITION BY id_pand_b ORDER BY function_count DESC) AS rn
    FROM 
        units_near_unknown_grouped
), 


-- estimate unknown functions
housing_guesses AS (
	SELECT DISTINCT ON (id_pand_b)
		id_pand_b AS id_pand, function::TEXT AS function, NULL::BIGINT AS sqm
	FROM units_near_unknown_ranked 
	WHERE rn = 1 AND function = 'woonfunctie'
), 
housing_fromvbo AS (
	SELECT id_pand, SUM(sqm::INTEGER) AS sqm, 'woonfunctie' AS function
	FROM units
	WHERE function = 'woonfunctie'
	GROUP BY id_pand
), 
housing_combined AS (
	SELECT id_pand, function, sqm FROM housing_guesses
	UNION
	SELECT id_pand, function, sqm FROM housing_fromvbo 
), 
housing_final AS (
	SELECT 
		u.function AS function, u.sqm AS sqm, 
		b.*
	FROM buildings b 
	JOIN housing_combined u ON b.id_pand = u.id_pand 
)
SELECT * FROM housing_final; -- WHERE status = 'Verbouwing pand'; 