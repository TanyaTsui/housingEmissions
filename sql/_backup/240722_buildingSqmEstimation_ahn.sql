WITH buildings AS (
	SELECT *
	FROM housing_delft
), 	

-- assign rows to ahn version
buildings_ahnversion AS (
	SELECT 
		CASE 	
			-- some rows already have sqm data 
			WHEN sqm > 0 OR sqm IS NOT NULL THEN 'not_needed'
	
			-- cases for bouw gestart 
			WHEN status = 'Bouw gestart' AND registration_start BETWEEN '2011-01-01' AND '2014-01-01' THEN 'ahn3'
			WHEN status = 'Bouw gestart' AND registration_start BETWEEN '2014-01-01' AND '2020-01-01' THEN 'ahn4'
			-- must be NULL, definitely don't have height data for buildings built after 2020 
			WHEN status = 'Bouw gestart' AND registration_start > '2020-01-01' THEN 'no ahn available' 
	
			-- cases for verbouwing pand 
			WHEN status = 'Verbouwing pand' AND registration_start BETWEEN '2011-01-01' AND '2013-06-30' THEN 'ahn2'
			WHEN status = 'Verbouwing pand' AND registration_start BETWEEN '2013-07-01' AND '2019-12-31' THEN 'ahn3' 
			WHEN status = 'Verbouwing pand' AND registration_start > '2020-01-01' THEN 'ahn4'
	
			-- cases for pand gesloopt 
			-- buildings demolished pre-2013 could use ahn2, but there is a risk that the elevation data was recorded during / after demolition. 
			-- for 100% accuracy, AHN1 data is needed, but (easily) available. (online has only DSM at 5m resolution) 
			WHEN status = 'Pand gesloopt' AND registration_start BETWEEN '2011-01-01' AND '2012-12-31' THEN 'ahn2' -- not 100% accurate 
			WHEN status = 'Pand gesloopt' AND registration_start BETWEEN '2013-01-01' AND '2019-12-31' THEN 'ahn2'
			WHEN status = 'Pand gesloopt' AND registration_start BETWEEN '2020-01-01' AND '2022-12-31' THEN 'ahn3'
			WHEN status = 'Pand gesloopt' AND registration_start >= '2023-01-01' THEN 'ahn4'
	
			-- other null cases 
			ELSE 'unforseen case'
		END AS ahn_version, 
		* 
	FROM buildings
), 

-- clip rasters by building footprint 
buildings_intersecting_raster AS (
	SELECT DISTINCT ON (b.id_pand) b.*, r.rast
	FROM buildings_ahnversion b 
	LEFT JOIN ahn_objectheight r 
	ON ST_Intersects(r.rast, b.geom_28992)
)
SELECT * FROM ahn_objectheight -- WHERE rast IS NULL

-- clipped_rasters AS (
-- 	SELECT 
-- 		b.id_pand, b.geom_28992, b.ahn_version, 
-- 		ST_Union(ST_Clip(r.rast, 1, b.geom_28992, -9999)) AS raster
-- 	FROM buildings_intersecting_raster b
-- 	JOIN ahn_objectheight r
-- 	ON ST_Intersects(r.rast, b.geom_28992) AND b.ahn_version = r.ahn_version 
-- 	GROUP BY b.id_pand, b.geom_28992, b.ahn_version
-- ), 

-- estimate n floors, sqm 
-- building_heights AS (
-- 	SELECT
-- 		*, ST_Area(geom_28992) AS footprint_sqm, 
-- 		(ST_SummaryStats(raster)).count AS n_pixels, (ST_SummaryStats(raster)).max AS height
-- 	FROM clipped_rasters
-- ), 
-- building_nfloors AS (
-- 	SELECT 
-- 		CASE
-- 			WHEN height < 4 THEN 1 
-- 			WHEN height > 4 THEN ROUND(height / 3)
-- 			ELSE 1 
-- 		END AS n_floors, 
-- 		* 
-- 	FROM building_heights
-- ), 
-- building_sqm AS (
-- 	SELECT
-- 		n_floors * footprint_sqm AS sqm_ahn, * 
-- 	FROM building_nfloors
-- ), 

-- -- make final table for sqm 	
-- housing_emissions_nl_sqm AS (
-- 	SELECT 
-- 		b.sqm_ahn, h.ahn_version, 
-- 		h.sqm, h.id_pand, h.build_year, h.status, 
-- 		h.registration_start, h.registration_end, 
-- 		h.geom, h.geom_28992, h.rast
-- 	FROM buildings_intersecting_raster h
-- 	LEFT JOIN building_sqm b 
-- 	ON b.id_pand = h.id_pand 
-- )

-- SELECT * -- DISTINCT ahn_version
-- FROM housing_emissions_nl_sqm 
-- -- WHERE sqm_ahn IS NULL AND sqm IS NULL












-- CHECK RESULT QUALITY - did query work? 

-- -- get percentage of NULL rows 
-- result AS (
-- 	SELECT COUNT(*) AS nrows_total
-- 	FROM housing_emissions_nl_sqm
-- ), 
-- result_null AS (
-- 	SELECT COUNT(*) AS nrows_null
-- 	FROM housing_emissions_nl_sqm
-- 	WHERE sqm_ahn IS NULL AND sqm IS NULL 
-- )
-- SELECT r.nrows_total, n.nrows_null, ROUND(n.nrows_null::numeric / r.nrows_total::numeric * 100) AS perc_null
-- FROM result r, result_null n

-- -- check n rows
-- query1 AS (
-- 	SELECT COUNT(*) AS query1_count FROM buildings_intersecting_raster
-- ), 
-- query2 AS (
-- 	SELECT COUNT(*) AS query2_count FROM housing_emissions_nl_sqm
-- )
-- SELECT 
-- 	q1.query1_count, 
-- 	q2.query2_count
-- FROM query1 q1, query2 q2; 