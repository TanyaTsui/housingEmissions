/* 
result: table of buildings constructed, renovated, or demolished between 2011 - now, with the following columns: 
id_pand, build_year, sqm, n_units, function, registration_start, registration_end, geometry (both 28992 and 4326) 
*/ 

-- DROP TABLE IF EXISTS housing_emissions_nl;
-- CREATE TABLE housing_emissions_nl AS 

-- -- get all buildings constructed, renovated, or demolished between 2011 - now 
-- WITH buildings AS (
-- 	SELECT 
-- 		DISTINCT ON (id_pand, status)
-- 		id_pand, build_year, status, registration_start, registration_end, geom, geom_28992 
-- 	FROM bag_pand
-- 	WHERE status IN ('Bouw gestart', 'Verbouwing pand', 'Pand gesloopt')
-- ), 

-- -- select valid units that match the buildings 
-- units_valid AS (
-- 	SELECT vbo.* 
-- 	FROM bag_vbo vbo
-- 	JOIN buildings b ON vbo.id_pand = b.id_pand
-- 	WHERE 
-- 		vbo.status NOT IN ('Niet gerealiseerd verblijfsobject', 'Verblijfsobject ingetrokken', 'Verblijfsobject ten onrechte opgevoerd') AND 
-- 		CAST(vbo.sqm AS INTEGER) < 9999
-- ), 

-- -- remove duplicate units, selecting the most recent entry 
-- ranked_units AS (
-- 	SELECT 
--         *,
--         ROW_NUMBER() OVER (PARTITION BY id_vbo ORDER BY registration_start DESC) AS rn
--     FROM units_valid
-- ), 
-- units AS (
-- 	SELECT * FROM ranked_units WHERE rn = 1
-- ), 

-- -- group all the units by id_pand: sum sqm and concatenate function
-- units_grouped_allFunctions AS (
-- 	SELECT id_pand, SUM(CAST(sqm AS INTEGER)) AS sqm, STRING_AGG(function, ', ') AS function, COUNT(*) AS n_units
-- 	FROM units
-- 	GROUP BY id_pand
-- ), 


-- units_grouped_housing AS (
-- 	SELECT id_pand, SUM(CAST(sqm AS INTEGER)) AS sqm, COUNT(*) AS n_units
-- 	FROM units
-- 	WHERE function = 'woonfunctie'
-- 	GROUP BY id_pand
-- ), 

-- -- find units where function doesn't contain 'woonfunctie', get their id_pand, and remove corresponding buildings
-- buildings_notHousing AS (
-- 	SELECT * FROM units_grouped_allFunctions WHERE function NOT LIKE '%woonfunctie%'
-- ), 
-- housing AS (
-- 	SELECT * 
-- 	FROM buildings
-- 	WHERE id_pand NOT IN (SELECT id_pand FROM buildings_notHousing)
-- ), 

-- -- add sqm and function info to buildings 
-- housing_emissions AS (
-- 	SELECT h.*, u.sqm, u.n_units, u.sqm / u.n_units AS av_unit_sqm, ST_Area(geom_28992) AS footprint, sqm / ST_Area(geom_28992) AS n_floors
-- 	FROM housing h
-- 	LEFT JOIN units_grouped_housing u ON h.id_pand = u.id_pand
-- )

-- SELECT * FROM housing_emissions; 

-- -- get columns in the right data format
-- ALTER TABLE housing_emissions_nl
-- ALTER COLUMN build_year TYPE INTEGER USING build_year::INTEGER,
-- ALTER COLUMN registration_start TYPE DATE USING registration_start::DATE,
-- ALTER COLUMN registration_end TYPE DATE USING registration_end::DATE;

-- Step 1: Create a temporary table to hold the filtered data
CREATE TEMPORARY TABLE temp_housing_emissions_nl AS 
WITH housing AS (
    SELECT * 
    FROM housing_emissions_nl
), 
housing_valid AS (
    SELECT * 
    FROM housing 
    WHERE 
        registration_start >= '2011-01-01' AND 
        (build_year = 9999 OR

        -- select valid rows for bouw gestart 
        (status = 'Bouw gestart' AND build_year BETWEEN 
            EXTRACT(YEAR FROM registration_start)::INTEGER-3 AND 
            EXTRACT(YEAR FROM registration_end)::INTEGER+3) OR

        -- select valid rows for verbouwing pand 
        (status = 'Verbouwing pand' AND EXTRACT(YEAR FROM registration_start)::INTEGER >= build_year + 5) OR

        -- select valid rows for pand gesloopt 
        (status = 'Pand gesloopt' AND EXTRACT(YEAR FROM registration_start)::INTEGER >= build_year))
)
SELECT * FROM housing_valid;

-- delete old table and replace with data of temp table
DELETE FROM housing_emissions_nl;
INSERT INTO housing_emissions_nl
SELECT * FROM temp_housing_emissions_nl;
DROP TABLE temp_housing_emissions_nl;
