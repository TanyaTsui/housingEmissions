WITH cbs AS (
	SELECT "BU_CODE" AS cbs, ST_Transform(geometry, 4326) AS geometry
	FROM cbs_map_2022
	WHERE "H2O" = 'NEE'
), 
cbs_map_all AS (
	SELECT year, 
		bu_code AS cbs_all, geometry
	FROM cbs_map_all
	-- WHERE municipality = 'Almere'
), 
nl_buurten AS (
	SELECT neighborhood_code AS nl_buurten, municipality, 
		ST_Transform(neighborhood_geom, 4326) AS neighborhood_geom
	FROM nl_buurten
	-- WHERE municipality = 'Almere'
),
housing_nl AS (
	SELECT neighborhood_code AS housing_nl, ST_Centroid(geom) AS geom, municipality
	FROM housing_nl
	--WHERE municipality = 'Amsterdam'
),
test AS (
	SELECT *
	FROM cbs_map_all a 
	FULL JOIN nl_buurten b 
	ON a.cbs_all = b.nl_buurten
)
SELECT * 
FROM test  
WHERE cbs_all IS NULL