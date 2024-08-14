/* 
ADD TRANSFORMATIONS FROM FUNCTION CHANGE TO HOUSING_RENOVATION_NL
Identifies transformations where units were changed from non-housing to housing function.  

These are the steps: 
- get rows in bag_vbo where status = 'Verblijfsobject in gebruik'
- order these rows by registration_start 
- get rows where the current function = 'woonfunctie', but previous function isn't 
*/

-- Step 1: Create the table if it doesn't exist
CREATE TABLE IF NOT EXISTS housing_renovation_nl (
    id_vbo VARCHAR,
    id_num VARCHAR,
    id_pand VARCHAR,
    geometry VARCHAR,
    function VARCHAR,
    sqm VARCHAR,
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
    province VARCHAR,
    renovation TEXT
);

-- Step 2: Delete existing rows where municipality is 'Delft'
DELETE FROM housing_renovation_nl
WHERE 
	renovation = 'transformation - function change'
	AND municipality = 'Delft'; 

-- Step 3: Insert the result of the query into the table
INSERT INTO housing_renovation_nl
WITH bag_vbo_sample AS (
	SELECT * 
	FROM bag_vbo
	WHERE 
		municipality = 'Delft'
		AND status = 'Verblijfsobject in gebruik'
), 
	
ordered_vbo AS (
    SELECT 
        *,
        LAG(function) OVER (PARTITION BY id_vbo ORDER BY registration_start) AS previous_function
    FROM bag_vbo_sample
)

SELECT 
	id_vbo, id_num, id_pand, geometry, function, sqm, status, 
	document_date, document_number, registration_start, registration_end, 
	geom, geom_28992, neighborhood_code, neighborhood, municipality, province,
	'transformation - function change' AS renovation
FROM ordered_vbo
WHERE 
    function = 'woonfunctie'
	AND previous_function IS NOT NULL
	AND previous_function != 'woonfunctie'; 























-- WITH bag_vbo_municipality AS (
-- 	SELECT * 
-- 	FROM bag_vbo 
-- 	WHERE municipality = 'Delft'
-- ), 
-- function_counts AS (
--     SELECT 
--         id_vbo,
--         COUNT(DISTINCT function) AS n_functions
--     FROM bag_vbo_municipality
--     GROUP BY id_vbo
-- ), 
-- idvbos_functionchange AS (
-- 	SELECT DISTINCT id_vbo 
-- 	FROM function_counts 
-- 	WHERE n_functions > 1
-- ), 
-- bag_vbo_functionchange AS (
-- 	SELECT b.*
-- 	FROM bag_vbo_municipality b
-- 	JOIN idvbos_functionchange i
-- 	ON b.id_vbo = i.id_vbo
-- ), 
-- previousfunction AS (
-- 	SELECT 
-- 		LAG(function) OVER (PARTITION BY id_vbo ORDER BY registration_start) AS previous_function, 
-- 		* 
-- 	FROM bag_vbo_functionchange
-- ), 
-- transformed_housing_units AS (
-- 	SELECT * 
-- 	FROM previousfunction
-- 	WHERE 
-- 		function = 'woonfunctie' 
-- 		AND previous_function != 'woonfunctie'
-- 		AND previous_function IS NOT NULL
-- ), 
-- transformed_housing_buildings AS (
-- 	SELECT id_pand, LEFT(registration_start, 4) AS year
-- 	FROM transformed_housing_units
-- 	GROUP BY id_pand, LEFT(registration_start, 4)
-- )

-- SELECT * FROM transformed_housing_buildings; 

-- -- update renovation column of bag_pand_delft with transformation 
-- -- where there was a match in id_pand and registration year 
-- UPDATE bag_pand_delft b
-- SET renovation = 'transformation - function change'
-- FROM transformed_housing_buildings t 
-- WHERE 
-- 	b.id_pand = t.id_pand 
-- 	AND municipality = 'Delft'
-- 	AND LEFT(b.registration_start, 4) = t.year;

-- -- add new rows to bag_pand_delft on transformation 
-- -- where there was no match in id_pand and registration year 
-- INSERT INTO bag_pand_delft (
-- 	id_pand, geometry, build_year, status, document_date, registration_start, registration_end, 
-- 	geom, geom_28992, neighborhood_code, neighborhood, municipality, province, renovation
-- )
-- WITH bag_pand_municipality AS (
-- 	SELECT * 
-- 	FROM bag_pand
-- 	WHERE municipality = 'Delft'
-- ), 	
-- transformation_notinbag AS (
-- 	SELECT t.* 
-- 	FROM transformed_housing_buildings t 
-- 	LEFT JOIN bag_pand_municipality b 
-- 	ON 
-- 		b.id_pand = t.id_pand 
-- 		AND LEFT(b.registration_start, 4) = t.year
-- 	WHERE b.id_pand IS NULL
-- ), 
-- ranked_bag_pand AS (
--     SELECT 
-- 		t.id_pand, b.geometry, b.build_year, b.status, 
-- 		year || '-01-01' AS document_date, year || '-01-01' AS registration_start, year || '-12-31' AS registration_end, 
-- 		b.geom, b.geom_28992, b.neighborhood_code, b.neighborhood, b.municipality, b.province, 
-- 		'transformation - function change (inserted)' AS renovation, 
--         ROW_NUMBER() OVER (PARTITION BY t.id_pand ORDER BY b.registration_start DESC) AS rn
--     FROM 
--         transformation_notinbag t
--     JOIN 
--         bag_pand_municipality b 
--     ON 
--         b.id_pand = t.id_pand
--     WHERE 
--         b.registration_start <= t.year
-- )
-- SELECT 
-- 	id_pand, geometry, build_year, status, document_date, registration_start, registration_end, 
-- 	geom, geom_28992, neighborhood_code, neighborhood, municipality, province, renovation
-- FROM ranked_bag_pand
-- WHERE rn = 1;