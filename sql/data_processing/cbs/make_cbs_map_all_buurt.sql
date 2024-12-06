-- delete and insert rows into cbs_map_all_buurt, the following query: 
DELETE FROM cbs_map_all_buurt WHERE municipality = 'Delft';  
INSERT INTO cbs_map_all_buurt (
	municipality, wk_code, bu_code, bu_geom, year, n_units, population, woz, tot_gas_m3, tot_elec_kwh
)

-- select housinginuse (now with cbs data) in Delft, in year 2013 (and other years as well)
WITH building_stats AS (
	SELECT * FROM housing_inuse_2012_2021 WHERE municipality = 'Delft'
), 

-- group by bu_code to get buurt-level data 
buurt2022_stats AS (
	SELECT municipality, bu_code, year, 
		SUM(n_units) AS n_units, ROUND(SUM(population)) AS population, ROUND(AVG(woz)) AS woz, 
		ROUND(SUM(tot_gas_m3)) AS tot_gas_m3, ROUND(SUM(tot_elec_kwh)) AS tot_elec_kwh
	FROM building_stats 
	GROUP BY municipality, bu_code, year 
	ORDER BY bu_code, year
), 

-- add buurt info: wk_code, municipality, bu_geom 
buurt2022_info AS (
	SELECT "BU_CODE" AS bu_code, "WK_CODE" AS wk_code, geometry AS bu_geom
	FROM cbs_map_2022 WHERE "GM_NAAM" = 'Delft'
), 
buurt_stats_and_geom AS (
	SELECT a.*, b.wk_code, b.bu_geom 
	FROM buurt2022_stats a 
	JOIN buurt2022_info b 
	ON a.bu_code = b.bu_code
)

SELECT  
	municipality, wk_code, bu_code, bu_geom, year, 
	n_units, population, woz, tot_gas_m3, tot_elec_kwh
FROM buurt_stats_and_geom 

