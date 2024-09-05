DELETE FROM emissions_all
WHERE municipality = 'Amsterdam'; 

INSERT INTO emissions_all
WITH nl_buurten_without_year AS (
	SELECT * FROM nl_buurten WHERE municipality = 'Amsterdam'
), 
years AS (
    SELECT generate_series(2012, 2021) AS year
), 
nl_buurten AS (
	SELECT 
	    n.*, 
	    y.year
	FROM nl_buurten_without_year n
	CROSS JOIN years y
), 
operational_emissions AS (
	SELECT * 
	FROM cbs_map_all 
	WHERE municipality = 'Amsterdam'
), 
operational_emissions_buurt AS (
	SELECT 
		neighborhood_code, year, 
		SUM(emissions_kg_total) AS emissions_operational_kg, 
		SUM(population) AS population, 
		SUM(electricity_kwh) AS electricity_kwh, SUM(woz) AS woz, SUM(gas_m3) AS gas_m3, 
		SUM(n_households) AS n_households, SUM(emissions_kg_gas) AS emissions_kg_gas, 
		SUM(emissions_kg_electricity) AS emissions_kg_electricity 
	FROM operational_emissions
	GROUP BY neighborhood_code, year
), 
embodied_emissions AS (
	SELECT * 
	FROM emissions_embodied_housing_nl
	WHERE municipality = 'Amsterdam' 
), 
embodied_emissions_buurt AS (
	SELECT 
		neighborhood_code, year, 
		SUM(emissions_embodied_kg) AS emissions_embodied_kg, 
		SUM(sqm) AS sqm
	FROM embodied_emissions
	GROUP BY neighborhood_code, year
), 
buurten_embodied AS (
	SELECT 
		n.*, 
		CASE WHEN e.sqm IS NULL THEN 0 ELSE e.sqm END AS sqm, 
		CASE 
			WHEN e.emissions_embodied_kg IS NULL THEN 0 
			ELSE e.emissions_embodied_kg 
		END AS emissions_embodied_kg
	FROM nl_buurten n 
	LEFT JOIN embodied_emissions_buurt e 
	ON 
		n.neighborhood_code = e.neighborhood_code
		AND n.year = e.year 
), 
buurten_embodied_operational AS (
	SELECT 
		b.*, 
		o.emissions_operational_kg, o.electricity_kwh, o.woz, o.gas_m3, o.population, 
		o.n_households, o.emissions_kg_gas, o.emissions_kg_electricity, 
		b.emissions_embodied_kg + o.emissions_operational_kg AS emissions_total_kg 
	FROM buurten_embodied b
	LEFT JOIN operational_emissions_buurt o 
	ON 
		b.neighborhood_code = o.neighborhood_code 
		AND b.year = o.year
)	
SELECT 
	year, neighborhood_code, neighborhood, municipality, 
	neighborhood_geom, ST_Transform(neighborhood_geom, 4326), 
	emissions_operational_kg, emissions_embodied_kg, emissions_total_kg,
	n_households, population, sqm, woz
FROM buurten_embodied_operational;