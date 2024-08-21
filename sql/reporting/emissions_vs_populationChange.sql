WITH population_2021 AS (
	SELECT gm_naam AS municipality, SUM(aant_inw) AS population_2021
	FROM cbs_map_all 
	WHERE year = 2021
	GROUP BY gm_naam
), 
population_2012 AS (
	SELECT gm_naam AS municipality, SUM(aant_inw) AS population_2012
	FROM cbs_map_all 
	WHERE year = 2012
	GROUP BY gm_naam
), 
population_change AS (
	SELECT 
		po.municipality, 
		po.population_2012, pn.population_2021, 
		pn.population_2021 - po.population_2012 AS population_change
	FROM population_2012 po
	JOIN population_2021 pn
	ON po.municipality = pn.municipality
), 
emissions_by_municipality AS (
	SELECT municipality, 
		SUM(emissions_operational) AS operational, 
		SUM(emissions_embodied) AS embodied
	FROM emissions_all
	WHERE year IS NOT NULL
	GROUP BY municipality
)
SELECT
	e.*, e.operational + e.embodied AS total_emissions, 
	p.population_change
FROM emissions_by_municipality e 
LEFT JOIN population_change p  
ON e.municipality = p.municipality
