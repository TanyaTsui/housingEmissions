-- -- TEST 1 
-- -- show buurts in cbs_map_all_buurt that do not have data for all 10 years
-- -- these could be industrial areas, or areas that were completely demolished at some point during the study period. 
-- WITH year_count AS (
-- 	SELECT municipality, bu_code, COUNT(*) AS n_years 
-- 	FROM cbs_map_all_buurt -- WHERE municipality = 'Venlo'
-- 	GROUP BY municipality, bu_code
-- 	ORDER BY municipality, bu_code
-- )

-- SELECT ST_Transform(bu_geom, 4326), * 
-- FROM year_count a
-- JOIN (SELECT DISTINCT ON (bu_code) * FROM cbs_map_all_buurt) b 
-- ON a.bu_code = b.bu_code
-- WHERE a.n_years < 10
-- ORDER BY a.n_years 


-- -- TEST 2 
-- -- show bu_codes that are mis-matching between housing_inuse and cbs_map_all 
-- -- there should be no mis-matches (no rows in resulting table)
-- WITH bu_codes_housing_inuse AS (
-- 	SELECT DISTINCT bu_code 
-- 	FROM housing_inuse_2012_2021 
-- 	--WHERE municipality = 'Delft'
-- ), 
-- bu_codes_cbs AS (
-- 	SELECT DISTINCT bu_code 
-- 	FROM cbs_map_all_buurt
-- 	--WHERE municipality = 'Delft'
-- )

-- SELECT * 
-- FROM bu_codes_housing_inuse a 
-- FULL JOIN bu_codes_cbs b 
-- ON a.bu_code = b.bu_code 
-- WHERE a.bu_code IS NULL OR b.bu_code IS NULL 


-- -- TEST 3
-- -- show mismatches between cbs_map_all_buurt, and bu_codes in cbs_map_2022 
-- -- make sure there are no bu_codes in cbs_map_all_buurt that don't show up 
-- WITH codes_2022 AS (
-- 	SELECT 
-- 		"GM_NAAM" AS municipality, "WK_CODE" AS wk_code, "BU_CODE" AS bu_code, 
-- 		ST_Transform(geometry, 4326) AS bu_geom
-- 	FROM cbs_map_2022 
-- 	WHERE "WATER" = 'NEE'
-- 	GROUP BY "GM_NAAM", "WK_CODE", "BU_CODE", geometry
-- ), 
-- codes_cbs_map_all_buurt AS (
-- 	SELECT municipality, wk_code, bu_code 
-- 	FROM cbs_map_all_buurt 
-- 	GROUP BY municipality, wk_code, bu_code
-- ), 
-- codes_housing_inuse AS (
-- 	SELECT municipality, bu_code
-- 	FROM housing_inuse_2012_2021 
-- 	GROUP BY municipality, bu_code
-- )

-- SELECT * 
-- FROM codes_2022 a 
-- FULL JOIN codes_housing_inuse b 
-- ON a.municipality = b.municipality
-- 	-- AND a.wk_code = b.wk_code
-- 	AND a.bu_code = b.bu_code 
-- WHERE a.bu_code IS NULL OR b.bu_code IS NULL 



-- TEST 4 
-- Are there any strange values for energy usage? 
-- SELECT ST_Transform(bu_geom, 4326), * FROM cbs_map_all_buurt 
-- WHERE population > 100 AND (tot_elec_kwh = 0)
-- ORDER BY bu_code, year

WITH problem_buurt_2022 AS (
	SELECT * FROM cbs_map_2022 
	WHERE "BU_CODE" = 'BU03620501'
)

SELECT * FROM housing_inuse_2012_2021 LIMIT 5

-- SELECT b."BU_CODE" AS bu_code_2022, a."BU_CODE" AS bu_code_2012, ST_Transform(a.geometry, 4326), 
-- 	a."G_GAS_TOT" AS gas, a."G_ELEK_TOT" AS elec 
-- FROM cbs_map_2020 a 
-- JOIN problem_buurt_2022 b 
-- ON a.geometry && b.geometry
-- 	AND ST_Intersects(a.geometry, b.geometry)






