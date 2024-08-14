/*
SNAPSHOT OF VBO ON 1-JAN-2019
The following query takes ~5 minutes to run. It does the following: 
- extracts rows from pand and vbo that overlap with 1-jan-2019 
- group filtered vbo by id_pand
- joins vbo with pand to make snapshot 
- saves query result as a new table
*/ 

-- make samples
DROP TABLE IF EXISTS bag_vbo_subset; 
CREATE TABLE bag_vbo_subset AS (
	SELECT * FROM bag_vbo 
	WHERE registration_start::date <= '2019-01-01' AND (registration_end::date > '2019-01-01' OR registration_end IS NULL) 
	LIMIT 1000
);

-- join pand(status, build_year, geometry) to vbo
DROP TABLE IF EXISTS bag_20190101; 
SELECT 
	units.id_vbo, units.id_pand, units.function, units.sqm, 
	buildings.status, buildings.build_year, buildings.geometry
INTO bag_20190101
FROM bag_vbo_subset AS units 
LEFT JOIN bag_pand AS buildings
ON units.id_pand = buildings.id_pand
WHERE buildings.registration_start::date <= '2019-01-01' 
	AND (buildings.registration_end::date > '2019-01-01' OR buildings.registration_end IS NULL); 

-- create geometry from text 
ALTER TABLE bag_20190101 ADD COLUMN geom geometry(Polygon, 4326);
UPDATE bag_20190101
SET geom = ST_Transform(
             ST_SetSRID(
               ST_GeomFromText(
                 'POLYGON((' || RTRIM(REPLACE(geometry || ' ', ' 0.0 ', ','), ',') || '))'
               ), 28992
             ), 4326
           );

-- display 
SELECT * FROM bag_20190101; 















------------------ CODE DUMP, IGNORE ------------------
-- -- group vbo_2019 by id_pand
-- vbo_2019_groupedByPand AS (
--     SELECT
--         id_pand,
--         string_agg(DISTINCT unnested_functions, ', ') AS functions, -- Concatenates unique function values
--         SUM(sqm::numeric) AS total_sqm
--     FROM (
--         SELECT
--             id_pand,
--             unnest(string_to_array(function, ', ')) AS unnested_functions, -- Converts function to array and unnests
--             sqm
--         FROM
--             vbo_2019
--     ) AS subquery
--     GROUP BY
--         id_pand
-- )

-- -- join vbo_2019 with pand_2019 on id_pand
-- SELECT
-- 	pand_2019.*,
-- 	vbo_2019_groupedByPand.functions,
-- 	vbo_2019_groupedByPand.total_sqm
-- FROM pand_2019
-- LEFT JOIN vbo_2019_groupedByPand 
-- ON pand_2019.id_pand = vbo_2019_groupedByPand.id_pand; 
-- -- WHERE vbo_2019_groupedByPand.functions IS NOT NULL OR vbo_2019_groupedByPand.total_sqm IS NOT NULL;