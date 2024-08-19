DELETE FROM emissions_all 
WHERE year = 2012
AND municipality = 'Montfoort';

WITH operational_emissions AS (
	SELECT * 
	FROM cbs_map_all 
	WHERE year = 2012 AND gm_naam = 'Montfoort'
), 
embodied_emissions AS (
	SELECT * 
	FROM emissions_embodied_housing_nl
	WHERE year = 2012 AND municipality = 'Montfoort'
), 
embodied_emissions_buurt AS (
	SELECT 
		neighborhood_code, 
		SUM(emissions_embodied_tons) AS embodied_emissions_tons, 
		SUM(sqm) AS sqm
	FROM embodied_emissions
	GROUP BY neighborhood_code 
)

INSERT INTO emissions_all (
    year, neighborhood_code, neighborhood_name, municipality, geometry, geom_4326, 
    emissions_operational, emissions_embodied, emissions_total, 
    n_households, n_residents, sqm_total, av_value
) 
SELECT 
	o.year, o.bu_code, o.bu_naam, o.gm_naam, o.geometry, o.geom_4326, 
	o.emissions_kg_total, e.embodied_emissions_tons, 
	o.emissions_kg_total + e.embodied_emissions_tons AS emissions_total, 
	o.aantal_hh, o.aant_inw, e.sqm, o.woz
FROM operational_emissions o 
FULL JOIN embodied_emissions_buurt e  
ON o.bu_code = e.neighborhood_code 