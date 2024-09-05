WITH nl_buurten AS (
	SELECT * FROM nl_buurten WHERE municipality = 'Amsterdam'
), 
operational_emissions AS (
	SELECT * 
	FROM cbs_map_all 
	WHERE municipality = 'Amsterdam'
), 
embodied_emissions AS (
	SELECT * 
	FROM emissions_embodied_housing_nl
	WHERE municipality = 'Amsterdam' 
), 
embodied_emissions_buurt AS (
	SELECT 
		neighborhood_code, 
		SUM(emissions_embodied_kg) AS embodied_emissions_kg, 
		SUM(sqm) AS sqm
	FROM embodied_emissions
	GROUP BY neighborhood_code 
), 
buurten_embodied AS (
	SELECT 
		ST_Transform(n.neighborhood_geom, 4326), n.*, e.sqm, 
		CASE 
			WHEN e.embodied_emissions_kg IS NULL THEN 0 
			ELSE e.embodied_emissions_kg 
		END AS emissions_embodied_kg
	FROM nl_buurten n 
	LEFT JOIN embodied_emissions_buurt e 
	ON n.neighborhood_code = e.neighborhood_code 
), 
buurten_embodied_operational AS (
	SELECT 
		b.*, 
		o.emissions_kg_total AS emissions_operational_kg, 
		b.emissions_embodied_kg + o.emissions_kg_total AS emissions_total_kg, 
		o.electricity_kwh, o.woz, o.gas_m3, 
		o.year, o.population, o.n_households, 
		o.emissions_kg_electricity, o.emissions_kg_gas, 
		o.emissions_kg_pp
	FROM buurten_embodied b
	LEFT JOIN operational_emissions o 
	ON b.neighborhood_code = o.neighborhood_code 
)

