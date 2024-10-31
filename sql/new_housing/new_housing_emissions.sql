INSERT INTO new_housing_emissions (
    id_pand, sqm, status, year, geom, geom_28992, 
    neighborhood_code, neighborhood, municipality, 
    emissions_operational_kg, emissions_embodied_kg
)

WITH housing_nl_sample AS (
	SELECT * 
	FROM housing_nl
	WHERE municipality = 'Amsterdam'
), 
new_homes_created AS (
	SELECT 
		id_pand, sqm, status, LEFT(registration_end, 4)::INTEGER AS year, 
		geom, geom_28992, neighborhood_code, neighborhood, municipality
	FROM housing_nl_sample
	WHERE status IN ('Bouw gestart', 'transformation - adding units', 'transformation - function change')
		AND registration_end IS NOT NULL
		AND LEFT(registration_end, 4)::INTEGER BETWEEN 2012 AND 2021
		AND sqm > 20
), 
expanded_homes AS (
    SELECT 
        id_pand, sqm, 'in use' AS status, generate_series(year + 1, 2021) AS year, 
		geom, geom_28992, neighborhood_code, neighborhood, municipality
    FROM new_homes_created
), 
new_homes AS (
	SELECT * FROM expanded_homes
	UNION ALL 
	SELECT * FROM new_homes_created
	ORDER BY id_pand, year
), 

inuse_homes_sample AS (
	SELECT * 
	FROM housing_inuse_2012_2022 
	WHERE municipality = 'Amsterdam'
), 
sqm_neighborhood AS (
	SELECT municipality, neighborhood_code, year, SUM(sqm) AS sqm_neighborhood
	FROM inuse_homes_sample
	GROUP BY municipality, neighborhood_code, year
), 
new_homes_with_neighborhood_sqm AS (
	SELECT a.*, b.sqm_neighborhood
	FROM new_homes a 
	LEFT JOIN sqm_neighborhood b
	ON a.neighborhood_code = b.neighborhood_code
		AND a.year = b.year
), 

energy_sample AS (
	SELECT year, neighborhood_code, municipality, 
		n_households, 
		electricity_kwh AS electricity_kwh_av, gas_m3 AS gas_m3_av, 
		electricity_kwh * n_households AS electricity_kwh_buurt, 
		gas_m3 * n_households AS gas_m3_buurt 
	FROM cbs_map_all 
	WHERE municipality = 'Amsterdam'
), 
new_homes_energy AS (
	SELECT a.*,  
		a.sqm / a.sqm_neighborhood * b.electricity_kwh_buurt AS electricity_kwh_pand, 
		a.sqm / a.sqm_neighborhood * b.gas_m3_buurt AS gas_m3_pand
	FROM new_homes_with_neighborhood_sqm a 
	LEFT JOIN energy_sample b 
	ON a.neighborhood_code = b.neighborhood_code 
		AND a.year = b.year
	ORDER BY id_pand, year
), 
	
new_homes_emissions AS (
	SELECT
		id_pand, sqm, status, year, geom, geom_28992, neighborhood_code, neighborhood, municipality,
		ROUND(gas_m3_pand * 1.9 + electricity_kwh_pand * 0.45) AS emissions_operational_kg, 
		CASE 
			WHEN status IN ('transformation - adding units', 'transformation - function change') THEN sqm * 126
			WHEN status = 'Bouw gestart' THEN sqm * 316
			ELSE 0 
		END AS emissions_embodied_kg
	FROM new_homes_energy
)

SELECT * FROM new_homes_emissions

-- SELECT year, SUM(emissions_operational_kg) AS emissions_operational_kg, SUM(emissions_embodied_kg) AS emissions_embodied_kg
-- FROM new_homes_emissions 
-- GROUP BY year


	




