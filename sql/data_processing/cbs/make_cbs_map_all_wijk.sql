DELETE FROM cbs_map_all_wijk WHERE year = %s AND municipality = %s; 

INSERT INTO cbs_map_all_wijk (
	year, municipality, wk_code, wk_geom, 
	population, n_households, n_homes, 
	gas_m3, elec_kwh, av_woz
)

-- get wijk geometry
WITH wijk_municipality AS (
	SELECT wk_code, gm_naam AS municipality, geom 
	FROM cbs_wijk_2012 -- I picked 2012 instead of 2024 because some 2024 wijks have become buurt sized
	WHERE water = 'NEE' AND gm_naam = %s
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
	FROM cbs_map_%s
	WHERE "WATER" = 'NEE'
), 
cbs_buurt_municipality AS (
	SELECT a.*
	FROM cbs_buurt a
	JOIN bbox b 
	ON a.geometry && b.geometry
		AND ST_Within(a.geometry, b.geometry)
), 

-- aggregate cbs_buurt_municipality to wijk 
cbs_buurt_with_wijk AS (
	SELECT a.neighborhood_code, a.geometry AS buurt_geom, a.population, a.n_households, a.n_homes, a.woz, 
		a.n_homes * a.av_gas_m3 AS gas_m3, a.n_homes * a.av_elec_kwh AS elec_kwh, 
		b.wk_code, b.geom AS wk_geom, b.municipality
	FROM cbs_buurt_municipality a
	JOIN LATERAL (
		SELECT b.wk_code, b.geom, b.municipality, ST_Area(ST_Intersection(a.geometry, b.geom)) AS intersection_area
		FROM wijk_municipality b
		WHERE ST_Intersects(a.geometry, b.geom)
		ORDER BY intersection_area DESC
		LIMIT 1
	) AS b ON true
), 
cbs_wijk AS (
	SELECT %s AS year, municipality, wk_code, wk_geom, 
		SUM(population) AS population, SUM(n_households) AS n_households, SUM(n_homes) AS n_homes, 
		SUM(gas_m3) AS gas_m3, SUM(elec_kwh) AS elec_kwh, ROUND(AVG(woz)) AS av_woz
	FROM cbs_buurt_with_wijk
	GROUP BY municipality, wk_code, wk_geom
)
SELECT * FROM cbs_wijk;  

