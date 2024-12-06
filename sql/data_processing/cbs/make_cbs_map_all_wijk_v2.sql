-- DELETE FROM cbs_map_all_wijk WHERE year = 2013 AND municipality = 'Terneuzen'; 

-- INSERT INTO cbs_map_all_wijk (
-- 	year, municipality, wk_code, wk_geom, 
-- 	population, n_households, n_homes, 
-- 	gas_m3, elec_kwh, av_woz
-- )

-- get wijk geometry
WITH wijk_municipality AS (
	SELECT wk_code, gm_naam AS municipality, geom 
	FROM cbs_wijk_2012 -- I picked 2012 instead of 2024 because some 2024 wijks have become buurt sized
	WHERE water = 'NEE' AND gm_naam = 'Terneuzen'
), 
bbox AS (
	SELECT ST_Buffer(ST_MakeEnvelope(ST_XMin(extent), ST_YMin(extent), ST_XMax(extent), ST_YMax(extent), 28992), 500) AS geometry
	FROM (SELECT ST_Extent(geom) AS extent FROM wijk_municipality) AS subquery
), 

-- get buurt level data for n_homes and energy use - use buurt geoms from 2015 
cbs_buurt AS (
	SELECT "BU_CODE" AS neighborhood_code, "GM_NAAM" AS municipality, geometry, 
		COALESCE(NULLIF(CASE WHEN "AANT_INW" < 0 THEN NULL ELSE "AANT_INW" END, NULL), 0) AS population,
		COALESCE(NULLIF(CASE WHEN "AANTAL_HH" < 0 THEN NULL ELSE "AANTAL_HH" END, NULL), 0) AS n_households, 
		COALESCE(NULLIF(CASE WHEN "WONINGEN" < 0 THEN NULL ELSE "WONINGEN" END, NULL), 0) AS n_homes, 
		COALESCE(NULLIF(CASE WHEN "G_GAS_TOT" < 0 THEN NULL ELSE "G_GAS_TOT" END, NULL), 0) AS av_gas_m3, 
		COALESCE(NULLIF(CASE WHEN "G_ELEK_TOT" < 0 THEN NULL ELSE "G_ELEK_TOT" END, NULL), 0) AS av_elec_kwh, 
		COALESCE(NULLIF(CASE WHEN "WOZ" < 0 THEN NULL ELSE "WOZ" END, NULL), 0) AS woz 
	FROM cbs_map_2013
	WHERE "WATER" = 'NEE'
), 
cbs_buurt_municipality AS (
	SELECT a.*
	FROM cbs_buurt a
	JOIN bbox b 
	ON a.geometry && b.geometry
		AND ST_Within(a.geometry, b.geometry)
), 

-- get in use housing in municipality 
inuse_buildings_municipality AS (
	SELECT * 
	FROM housing_inuse_2012_2021 
	WHERE year = 2013
		AND municipality = 'Terneuzen'
), 

-- assign cbs data to each building proportionally based on building sqm 
buildings_with_buurt_code AS (
	SELECT a.year, a.id_pand, a.sqm, a.n_units, a.geom_28992, b.neighborhood_code
	FROM inuse_buildings_municipality a
	LEFT JOIN cbs_buurt_municipality b 
	ON a.geom_28992 && b.geometry
		AND ST_Within(a.geom_28992, b.geometry)
), 
inuse_sqm_per_buurt AS (
	SELECT neighborhood_code, SUM(sqm) AS inuse_sqm
	FROM buildings_with_buurt_code
	WHERE neighborhood_code IS NOT NULL
	GROUP BY neighborhood_code 
), 
buurt_stats AS (
	SELECT a.*, 
		a.av_gas_m3 * a.n_homes AS tot_gas_m3, 
		a.av_elec_kwh * a.n_homes AS tot_elec_kwh, 
		b.inuse_sqm
	FROM cbs_buurt_municipality a 
	LEFT JOIN inuse_sqm_per_buurt b 
	ON a.neighborhood_code = b.neighborhood_code 
), 
building_stats AS (
	SELECT a.year, a.id_pand, a.sqm, a.n_units, a.geom_28992,  
		a.sqm/b.inuse_sqm * population AS population, 
		a.sqm/b.inuse_sqm * n_homes AS n_homes, 
		a.sqm/b.inuse_sqm * tot_gas_m3 AS tot_gas_m3, 
		a.sqm/b.inuse_sqm * tot_elec_kwh AS tot_elec_kwh, woz
	FROM buildings_with_buurt_code a 
	JOIN buurt_stats b 
	ON a.neighborhood_code = b.neighborhood_code
), 

buildings_stats_with_wijk AS (
	SELECT * 
	FROM building_stats a 
	LEFT JOIN (SELECT DISTINCT ON (wk_code) wk_code, municipality, wk_geom 
		FROM key_buurt2022_to_wijk2012 
		WHERE municipality = 'Terneuzen') b 
	ON a.geom_28992 && b.wk_geom
		AND ST_Within(a.geom_28992, b.wk_geom)
)

SELECT wk_code, gm_naam AS municipality, 
FROM cbs_wijk_2012 LIMIT 10







-- assign buildings to wijk and aggregate data by wijk 


-- SELECT 
-- 	ROUND(ST_Area(ST_Intersection(a.geometry, b.geom))) AS intersection_area, 
-- 	ROUND(ST_Area(ST_Intersection(a.geometry, b.geom)) / ST_Area(a.geometry) * 100) AS intersection_pct, 
-- 	-- ROUND(ST_Area(a.geometry)) AS buurt_area, 
-- 	-- ROUND(ST_Area(b.geom)) AS wk_area, 
-- 	a.neighborhood_code, ST_Transform(a.geometry, 4326) AS buurt_geom, 
-- 	a.population, a.n_households, a.n_homes, a.woz, 
-- 	a.n_homes * a.av_gas_m3 AS gas_m3, a.n_homes * a.av_elec_kwh AS elec_kwh, 
-- 	b.wk_code, ST_Transform(b.geom, 4326) AS wk_geom, b.municipality
-- FROM cbs_buurt_municipality a
-- JOIN wijk_municipality b
-- ON ST_Intersects(a.geometry, b.geom)
-- WHERE ROUND(ST_Area(ST_Intersection(a.geometry, b.geom)) / ST_Area(a.geometry) * 100) > 5




-- -- aggregate cbs_buurt_municipality to wijk 
-- cbs_buurt_with_wijk AS (
-- 	SELECT a.neighborhood_code, a.geometry AS buurt_geom, a.population, a.n_households, a.n_homes, a.woz, 
-- 		a.n_homes * a.av_gas_m3 AS gas_m3, a.n_homes * a.av_elec_kwh AS elec_kwh, 
-- 		b.wk_code, b.geom AS wk_geom, b.municipality
-- 	FROM cbs_buurt_municipality a
-- 	JOIN LATERAL (
-- 		SELECT b.wk_code, b.geom, b.municipality, ST_Area(ST_Intersection(a.geometry, b.geom)) AS intersection_area
-- 		FROM wijk_municipality b
-- 		WHERE ST_Intersects(a.geometry, b.geom)
-- 		ORDER BY intersection_area DESC
-- 		LIMIT 1
-- 	) AS b ON true
-- ), 
-- cbs_wijk AS (
-- 	SELECT 2013 AS year, municipality, wk_code, wk_geom, 
-- 		SUM(population) AS population, SUM(n_households) AS n_households, SUM(n_homes) AS n_homes, 
-- 		SUM(gas_m3) AS gas_m3, SUM(elec_kwh) AS elec_kwh, ROUND(AVG(woz)) AS av_woz
-- 	FROM cbs_buurt_with_wijk
-- 	GROUP BY municipality, wk_code, wk_geom
-- )
-- SELECT * FROM cbs_wijk;  

