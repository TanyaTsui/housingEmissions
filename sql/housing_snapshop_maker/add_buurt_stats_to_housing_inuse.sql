-- get 2022 buurt geometry for Delft and make bbox 
WITH buurt2022_municipality AS (
	SELECT "BU_CODE" AS bu_code, geometry AS bu_geom, "GM_NAAM" AS municipality
	FROM cbs_map_2022 
	WHERE "WATER" = 'NEE' AND "GM_NAAM" = 'Delft'
), 
bbox_buurt2022 AS (
	SELECT ST_Buffer(ST_MakeEnvelope(ST_XMin(extent), ST_YMin(extent), ST_XMax(extent), ST_YMax(extent), 28992), 500) AS bbox_geom
	FROM (SELECT ST_Extent(bu_geom) AS extent FROM buurt2022_municipality) AS subquery
), 

-- use bbox to select buurt-level cbs data from cbs_map_2013 (and other years as well)
cbs_data_year AS (
	SELECT b.*, a.geometry, a."BU_CODE" AS bu_code, 
		COALESCE(NULLIF(CASE WHEN "AANT_INW" < 0 THEN NULL ELSE "AANT_INW" END, NULL), 0) AS population,
		COALESCE(NULLIF(CASE WHEN "WONINGEN" < 0 THEN NULL ELSE "WONINGEN" END, NULL), 0) AS n_homes, 
		COALESCE(NULLIF(CASE WHEN "WOZ" < 0 THEN NULL ELSE "WOZ" END, NULL), 0) AS woz, 
		COALESCE(NULLIF(CASE WHEN "G_GAS_TOT" < 0 THEN NULL ELSE "G_GAS_TOT" END, NULL), 0) AS av_gas_m3, 
		COALESCE(NULLIF(CASE WHEN "G_ELEK_TOT" < 0 THEN NULL ELSE "G_ELEK_TOT" END, NULL), 0) AS av_elec_kwh, 
		COALESCE(NULLIF(CASE WHEN "P_STADVERW" < 0 THEN NULL ELSE "P_STADVERW" END, NULL), 0) AS p_stadverw
	FROM cbs_map_2013 a 
	JOIN bbox_buurt2022 b 
	ON a.geometry && b.bbox_geom
), 
bbox_cbs_data_year AS (
	SELECT ST_Buffer(ST_MakeEnvelope(ST_XMin(extent), ST_YMin(extent), ST_XMax(extent), ST_YMax(extent), 28992), 500) AS bbox_geom
	FROM (SELECT ST_Extent(geometry) AS extent FROM cbs_data_year) AS subquery
), 

-- use bbox to select building-level data from housing_inuse_2012_2022 
-- (this is the only option. Municipality names in housing_inuse are wrong (based on 2012 wijks), I checked)
housing_inuse_in_bbox AS (
	SELECT a.*
	FROM (SELECT * FROM housing_inuse_2012_2021 WHERE year = 2013) a
	JOIN bbox_cbs_data_year b 
	ON a.pd_geom && b.bbox_geom
), 

-- assign 2013 bu_codes to inuse buildings, get sqm in use per bu_code
housing_inuse_with_bucodes AS (
	SELECT a.year, a.id_pand, a.sqm, a.n_units, a.pd_geom, 
		b.bu_code AS bu_code_year
	FROM housing_inuse_in_bbox a
	JOIN cbs_data_year b 
	ON a.pd_geom && b.geometry
		AND ST_Intersects(a.pd_geom, b.geometry)
	WHERE ST_Area(ST_Intersection(a.pd_geom, b.geometry)) / ST_Area(a.pd_geom) * 100 > 50
), 
inuse_sqm_per_bucode AS (
	SELECT bu_code_year, SUM(sqm) AS inuse_sqm 
	FROM housing_inuse_with_bucodes
	GROUP BY bu_code_year
), 

-- assign 2013 cbs data to buildings proportionally with inuse sqm 
cbs_stats_year AS (
	SELECT a.*, b.geometry AS bu_geom, 
		b.population, b.n_homes, b.woz, b.p_stadverw, 
		b.n_homes*b.av_gas_m3 AS tot_gas_m3, b.n_homes*b.av_elec_kwh AS tot_elec_kwh
	FROM inuse_sqm_per_bucode a 
	LEFT JOIN cbs_data_year b 
	ON a.bu_code_year = b.bu_code
), 
building_stats AS (
	SELECT a.year, a.id_pand, 
		a.sqm, a.n_units, a.pd_geom,  
		b.population * a.sqm / b.inuse_sqm AS population, 
		b.n_homes * a.sqm / b.inuse_sqm AS n_homes,
		b.tot_gas_m3 * a.sqm / b.inuse_sqm AS tot_gas_m3,
		b.tot_elec_kwh * a.sqm / b.inuse_sqm AS tot_elec_kwh,
		b.woz AS woz, b.p_stadverw AS p_stadverw
	FROM housing_inuse_with_bucodes a 
	JOIN cbs_stats_year b
	ON a.bu_code_year = b.bu_code_year
), 

-- add 2022 buurt code to building_stats 
building_stats_with_2022bucode AS (
	SELECT a.*, b.bu_code, b.municipality
	FROM building_stats a 
	JOIN buurt2022_municipality b 
	ON a.pd_geom && b.bu_geom 
		AND ST_Intersects(a.pd_geom, b.bu_geom)
	WHERE ST_Area(ST_Intersection(a.pd_geom, b.bu_geom)) / ST_Area(a.pd_geom) * 100 > 50
)

UPDATE housing_inuse_2012_2021 AS h
SET 
    population = b.population,
    n_homes = b.n_homes,
    tot_gas_m3 = b.tot_gas_m3,
    tot_elec_kwh = b.tot_elec_kwh,
    woz = b.woz,
    p_stadverw = b.p_stadverw,
    bu_code = b.bu_code,
    municipality = b.municipality
FROM building_stats_with_2022bucode AS b
WHERE h.id_pand = b.id_pand AND h.year = b.year


-- -- testing bu_codes 
-- bu_codes_building_stats AS (
-- 	SELECT DISTINCT bu_code FROM building_stats_with_2022bucode
-- ), 
-- bu_codes_buurt2022_municipality AS (
-- 	SELECT DISTINCT bu_code FROM buurt2022_municipality
-- ), 
-- missing_bu_codes AS (
-- 	SELECT b.bu_code AS bu_code_2022
-- 	FROM bu_codes_building_stats a 
-- 	FULL JOIN bu_codes_buurt2022_municipality b 
-- 	ON a.bu_code = b.bu_code 
-- 	WHERE a.bu_code IS NULL OR b.bu_code IS NULL 
-- )

-- SELECT ST_Transform(bu_geom, 4326), * 
-- FROM missing_bu_codes a 
-- JOIN buurt2022_municipality b 
-- ON a.bu_code_2022 = b.bu_code 



-- -- testing id pands 
-- id_pands_housing_inuse AS (
-- 	SELECT DISTINCT id_pand AS id_pand_housing_inuse
-- 	FROM (SELECT * FROM housing_inuse_2012_2021 WHERE year = 2013) a 
-- 	JOIN buurt2022_municipality b 
-- 	ON a.pd_geom && b.bu_geom
-- 		AND ST_Within(a.pd_geom, b.bu_geom)
-- 	WHERE ST_Area(ST_Intersection(a.pd_geom, b.bu_geom)) / ST_Area(a.pd_geom) * 100 > 50
-- ), 
-- id_pands_building_stats AS (
-- 	SELECT DISTINCT id_pand AS id_pand_building_stats 
-- 	FROM building_stats_with_2022bucode a 
-- 	JOIN buurt2022_municipality b 
-- 	ON a.pd_geom && b.bu_geom
-- 		AND ST_Within(a.pd_geom, b.bu_geom)
-- ), 
-- missing_id_pands AS (
-- 	SELECT * 
-- 	FROM id_pands_housing_inuse a 
-- 	FULL JOIN id_pands_building_stats b 
-- 	ON a.id_pand_housing_inuse = b.id_pand_building_stats
-- 	WHERE a.id_pand_housing_inuse IS NULL
-- 		OR b.id_pand_building_stats IS NULL 
-- )

-- SELECT * 
-- FROM missing_id_pands a 
-- JOIN (SELECT * FROM housing_inuse_2012_2021 WHERE year = 2013) b 
-- ON a.id_pand_housing_inuse = b.id_pand 








