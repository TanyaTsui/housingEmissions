WITH cbs_map_all AS (
	SELECT *
	FROM cbs_map_all
	WHERE gm_naam = 'Delft'
), 
nl_buurten AS (
	SELECT * 
	FROM nl_buurten
	WHERE municipality = 'Delft'
)

SELECT 
    cbs_map_all.*, 
	nl_buurten.*, 
    nl_buurten.neighborhood_geom AS neighborhood_geometry
FROM 
    cbs_map_all
LEFT JOIN 
    nl_buurten 
ON 
	cbs_map_all.geometry && nl_buurten.neighborhood_geom
    AND ST_Within(ST_Centroid(cbs_map_all.geometry), nl_buurten.neighborhood_geom)
