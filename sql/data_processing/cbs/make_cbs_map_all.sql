DELETE FROM cbs_map_all WHERE year = %s; 

INSERT INTO cbs_map_all (
	year, municipality, neighborhood, neighborhood_code, neighborhood_geom, 
	population, n_households, woz, gas_m3, electricity_kwh
)

WITH nl_buurten AS (
    SELECT * FROM nl_buurten 
), 
cbs_map_%s AS ( 
    SELECT * 
	FROM cbs_map_%s
), 
intersections AS (
    SELECT 
        b_new.neighborhood_code AS buurt_id_new,
        ST_Transform(b_new.neighborhood_geom, 4326) AS geom_new,
        b_new.neighborhood, 
        b_new.municipality, 
        
		b_old."BU_CODE" AS buurt_id_old,
        ST_Transform(b_old.geometry, 4326) AS geom_old, 

        CASE WHEN b_old."AANT_INW" < -9999 THEN 0 ELSE b_old."AANT_INW" END AS population_old, 
        CASE WHEN b_old."AANTAL_HH" < -9999 THEN 0 ELSE b_old."AANTAL_HH" END AS n_households_old, 
        CASE WHEN b_old."WOZ" < -9999 THEN 0 ELSE b_old."WOZ" END AS woz_old, 
        CASE WHEN b_old."G_GAS_TOT" < -9999 THEN 0 ELSE b_old."G_GAS_TOT" END AS gas_m3_old, 
        CASE WHEN b_old."G_ELEK_TOT" < -9999 THEN 0 ELSE b_old."G_ELEK_TOT" END AS electricity_kwh_old, 
        
		ST_Area(b_old.geometry) AS area_old,
		ST_Transform(ST_Intersection(b_new.neighborhood_geom, b_old.geometry), 4326) AS geom_intersection,
        ST_Area(ST_Intersection(b_new.neighborhood_geom, b_old.geometry)) AS intersection_area
    FROM nl_buurten b_new
    JOIN cbs_map_%s b_old
    ON ST_Intersects(b_new.neighborhood_geom, b_old.geometry)
), 
total_intersection_area AS (
    SELECT 
        buurt_id_old,
        SUM(intersection_area) AS total_intersection_area
    FROM intersections
    GROUP BY buurt_id_old
),
new_attributes AS (
    SELECT 
        ROUND(intersection_area / total_intersection_area * population_old) AS population_new, 
	    ROUND(intersection_area / total_intersection_area * n_households_old) AS n_households_new,
        ROUND(intersection_area / total_intersection_area * woz_old) AS woz_new,
        ROUND(intersection_area / total_intersection_area * gas_m3_old) AS gas_m3_new,
        ROUND(intersection_area / total_intersection_area * electricity_kwh_old) AS electricity_kwh_new,
        intersections.*
    FROM intersections
    JOIN total_intersection_area
    ON intersections.buurt_id_old = total_intersection_area.buurt_id_old
), 
grouped AS (
	SELECT 
		%s AS year, 
		municipality, neighborhood, buurt_id_new AS neighborhood_code, 
		geom_new AS neighborhood_geom, 
        SUM(population_new) AS population,
        SUM(n_households_new) AS n_households,
        SUM(woz_new) AS woz,
        SUM(gas_m3_new) AS gas_m3,
        SUM(electricity_kwh_new) AS electricity_kwh
	FROM new_attributes
	GROUP BY municipality, neighborhood, buurt_id_new, geom_new
)
SELECT * FROM grouped;

