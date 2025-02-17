DROP TABLE IF EXISTS reporting_emissions_population_sqm; 
CREATE TABLE reporting_emissions_population_sqm AS 

-- get population change
WITH population_2021 AS (
	SELECT municipality, SUM(population) AS population_2021
	FROM cbs_map_all 
	WHERE year = 2021
	GROUP BY municipality
), 
population_2012 AS (
	SELECT municipality, SUM(population) AS population_2012
	FROM cbs_map_all 
	WHERE year = 2012
	GROUP BY municipality
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

-- get sqm change 
sqm_2012 AS (
	SELECT municipality, SUM(sqm) AS sqm_2012
	FROM housing_nl_2012
	GROUP BY municipality
), 
sqm_2021 AS (
	SELECT municipality, SUM(sqm) AS sqm_2021
	FROM housing_nl_2021
	GROUP BY municipality
), 
sqm_change AS (
	SELECT 
		a.municipality, a.sqm_2012, b.sqm_2021, 
		b.sqm_2021 - a.sqm_2012 AS sqm_change
	FROM sqm_2012 a
	JOIN sqm_2021 b 
	ON a.municipality = b.municipality
), 

-- get n_households_change
n_homes_2012 AS (
	SELECT municipality, SUM(n_households) AS n_homes_2012
	FROM cbs_map_all
	WHERE year = 2012
	GROUP BY municipality
), 
n_homes_2021 AS (
	SELECT municipality, SUM(n_households) AS n_homes_2021
	FROM cbs_map_all
	WHERE year = 2021
	GROUP BY municipality
), 
n_homes_change AS (
	SELECT 
		a.municipality, a.n_homes_2012, b.n_homes_2021, 
		b.n_homes_2021 - a.n_homes_2012 AS n_homes_change
	FROM n_homes_2012 a
	JOIN n_homes_2021 b 
	ON a.municipality = b.municipality 
), 

-- get emissions 
emissions_by_municipality AS (
	SELECT municipality, 
		SUM(emissions_operational) AS operational, 
		SUM(emissions_embodied) AS embodied
	FROM emissions_all
	WHERE year IS NOT NULL
	GROUP BY municipality
), 

-- join emissions, population change, and sqm change
emissions_populationchange AS (
	SELECT
		e.*, e.operational + e.embodied AS total_emissions, 
		p.population_2012, p.population_2021, 
		p.population_change
	FROM emissions_by_municipality e 
	LEFT JOIN population_change p  
	ON e.municipality = p.municipality
), 
emissions_populationchange_sqmchange AS (
	SELECT e.*, s.sqm_change, s.sqm_2012, s.sqm_2021 
	FROM emissions_populationchange e 
	LEFT JOIN sqm_change s 
	ON e.municipality = s.municipality
), 
emissions_popchnage_sqmchange_homeschange AS (
	SELECT e.*, h.n_homes_change, h.n_homes_2012, h.n_homes_2021 
	FROM emissions_populationchange e 
	LEFT JOIN n_homes_change h 
	ON e.municipality = h.municipality
)
	
SELECT * FROM emissions_popchnage_sqmchange_homeschange

