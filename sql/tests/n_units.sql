-- -- TEST 1
-- -- Showing map units that changed status over time 

-- WITH bag_vbo_municipality AS (
-- 	SELECT * 
-- 	FROM bag_vbo 
-- 	WHERE municipality = 'Amstelveen'
-- ), 
-- bag_pand_municipality AS (
-- 	SELECT DISTINCT ON (id_pand) * 
-- 	FROM bag_pand
-- 	WHERE 
-- 		municipality = 'Amstelveen'
-- 		AND status = 'Pand in gebruik' 
-- 		AND LEFT(registration_start, 4)::INTEGER <= 2012
-- 		AND (registration_end IS NULL OR LEFT(registration_end, 4)::INTEGER > 2012)
-- ), 
-- housing_units_2012 AS (
-- 	SELECT DISTINCT ON (id_vbo) * 
-- 	FROM bag_vbo_municipality
-- 	WHERE 
-- 		-- status = 'Verblijfsobject in gebruik'
-- 		sqm::INTEGER < 9999
-- 		AND function = 'woonfunctie'
-- 		AND LEFT(registration_start, 4)::INTEGER <= 2012
-- 		AND (registration_end IS NULL OR LEFT(registration_end, 4)::INTEGER > 2012)
-- ), 
-- housing_units_2016 AS (
-- 	SELECT DISTINCT ON (id_vbo) * 
-- 	FROM bag_vbo_municipality
-- 	WHERE 
-- 		-- status = 'Verblijfsobject in gebruik'
-- 		sqm::INTEGER < 9999
-- 		AND function = 'woonfunctie'
-- 		AND LEFT(registration_start, 4)::INTEGER <= 2018
-- 		AND (registration_end IS NULL OR LEFT(registration_end, 4)::INTEGER > 2018)
-- ), 
-- -- housing_buildings AS (
-- -- 	SELECT 
-- -- 		id_pand, 
-- -- 		SUM(sqm::INTEGER) AS sqm, COUNT(*) AS n_units
-- -- 	FROM housing_units
-- -- 	GROUP BY id_pand
-- -- ), 
-- problem_buurt_2022 AS (
-- 	SELECT geometry
-- 	FROM cbs_map_2022 
-- 	WHERE "BU_CODE" = 'BU03620501'
-- ), 
-- problem_vbos_2012 AS (
-- 	SELECT a.id_vbo, a.status, a.sqm, a.geom
-- 	FROM housing_units_2012 a 
-- 	JOIN problem_buurt_2022 b
-- 	ON ST_Within(a.geom_28992, b.geometry)
-- ), 
-- problem_vbos_2016 AS (
-- 	SELECT a.id_vbo, a.status, a.sqm, a.geom
-- 	FROM housing_units_2016 a 
-- 	JOIN problem_buurt_2022 b
-- 	ON ST_Within(a.geom_28992, b.geometry)
-- )

-- SELECT ST_Transform(b.geom, 4326), * 
-- FROM problem_vbos_2012 a 
-- JOIN problem_vbos_2016 b 
-- ON a.id_vbo = b.id_vbo
-- WHERE a.status != b.status
-- 	AND a.status = 'Verblijfsobject in gebruik'
-- 	AND b.status = 'Verblijfsobject ingetrokken'



-- -- SELECT 2012 AS year, a.*, b.geom, b.geom_28992, b.neighborhood_code, b.wk_code, b.municipality
-- -- FROM housing_buildings a 
-- -- LEFT JOIN bag_pand_municipality b 
-- -- ON a.id_pand = b.id_pand 
-- -- WHERE b.id_pand IS NOT NULL -- remove 0.05 percent of buildings that are not in bag_pand_municipality 



-- TEST 2
-- Mismatches between n_units and population  

-- WITH problem_buurts AS (
-- 	SELECT bu_code, year, n_units, population
-- 	FROM cbs_map_all_buurt 
-- 	WHERE n_units > 0 AND population = 0
-- )

-- SELECT a.*, b.bu_code, b.year, b.n_units, b.population 
-- FROM problem_buurts a 
-- JOIN cbs_map_all_buurt b 
-- ON a.bu_code = b.bu_code
-- 	AND a.year + 1 = b.year
-- ORDER BY a.n_units DESC

-- SELECT * FROM cbs_map_all_buurt


-- TEST 3 
-- Abnormally high energy use values 
WITH problem_bu_codes AS (
	SELECT bu_code, bu_geom, n_units, 
		ROUND(tot_gas_m3 / n_units) AS gas_per_unit, ROUND(tot_elec_kwh / n_units) AS elec_per_unit 
	FROM cbs_map_all_buurt
	WHERE 
		year = 2015
		AND (tot_gas_m3 / n_units > 1326*5
		OR tot_elec_kwh / n_units > 2960 * 5)
	ORDER BY tot_gas_m3 / n_units DESC
), 
bag_vbo_year AS (
	SELECT * 
	FROM bag_vbo 
	WHERE 
		sqm::INTEGER < 9999
		AND function = 'woonfunctie'
		AND LEFT(registration_start, 4)::INTEGER <= 2015
		AND (registration_end IS NULL OR LEFT(registration_end, 4)::INTEGER > 2015)
), 
problem_vbos AS (
	SELECT * 
	FROM problem_bu_codes a 
	JOIN bag_vbo_year b 
	ON ST_Intersects(b.geom_28992, a.bu_geom)
), 
n_units_vbo AS (
	SELECT bu_code, COUNT(*) AS n_units_vbo 
	FROM problem_vbos 
	GROUP BY bu_code
)

SELECT * 
FROM problem_bu_codes a 
JOIN n_units_vbo b 
ON a.bu_code = b.bu_code 
WHERE a.n_units >= b.n_units_vbo 



-- SELECT a.bu_code, ST_Transform(a.bu_geom, 4326) AS bu_geom, 
-- 	b."BU_CODE", ST_Transform(b.geometry, 4326) AS bu_geom_2015, 
-- 	a.n_units, 
-- 	"AANTAL_HH", "WONINGEN", "G_GAS_TOT", "G_ELEK_TOT"
-- FROM problem_bu_codes a 
-- JOIN cbs_map_2015 b 
-- ON a.bu_geom && b.geometry 
-- 	AND ST_Intersects(a.bu_geom, b.geometry)

