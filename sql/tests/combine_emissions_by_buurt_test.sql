WITH operational_emissions AS (
	SELECT * 
	FROM cbs_map_all 
	WHERE year = 2012 AND municipality = 'Amsterdam'
), 
embodied_emissions AS (
	SELECT * 
	FROM emissions_embodied_housing_nl
	WHERE year = 2012 AND municipality = 'Amsterdam'
), 
embodied_emissions_buurt AS (
	SELECT 
		neighborhood_code, 
		SUM(emissions_embodied_kg) AS embodied_emissions_kg, 
		SUM(sqm) AS sqm
	FROM embodied_emissions
	GROUP BY neighborhood_code 
), 
emissions_all AS (
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
)

SELECT * FROM emissions_all

-- TEST: do all rows in operational_emissions have matching neighborhood_code in nl_buurten? 
-- TEST: do all rows in embodied_emissions_buurt have matching neighborhood_code in nl_buurten? 
-- I'm a bit suspicious of the FULL JOIN in emissions_all. 
-- Would it be better to join each emissions table to nl_buurten? 