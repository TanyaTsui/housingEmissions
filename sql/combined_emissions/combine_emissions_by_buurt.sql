DELETE FROM emissions_all 
WHERE year = %s
AND municipality = %s;

WITH operational_emissions AS (
	SELECT * 
	FROM cbs_map_all 
	WHERE year = %s AND municipality = %s
), 
embodied_emissions AS (
	SELECT * 
	FROM emissions_embodied_housing_nl
	WHERE year = %s AND municipality = %s
), 
embodied_emissions_buurt AS (
	SELECT 
		neighborhood_code, 
		SUM(emissions_embodied_kg) AS embodied_emissions_kg, 
		SUM(sqm) AS sqm
	FROM embodied_emissions
	GROUP BY neighborhood_code 
)

INSERT INTO emissions_all (
    year, neighborhood_code, neighborhood_name, municipality, 
	geometry, geom_4326, 
    emissions_operational, emissions_embodied, emissions_total, 
    n_households, n_residents, sqm_total, av_value
) 
SELECT 
	o.year, o.neighborhood_code, o.neighborhood, o.municipality, 
	ST_Transform(o.neighborhood_geom, 28992) AS geometry, o.neighborhood_geom AS geom_4326, 
	COALESCE(o.emissions_kg_total, 0) AS emissions_operational, 
    COALESCE(e.embodied_emissions_kg, 0) AS emissions_embodied, 
    COALESCE(o.emissions_kg_total, 0) + COALESCE(e.embodied_emissions_kg, 0) AS emissions_total, 
	o.n_households, o.population, e.sqm, o.woz
FROM operational_emissions o 
FULL JOIN embodied_emissions_buurt e  
ON o.neighborhood_code = e.neighborhood_code 