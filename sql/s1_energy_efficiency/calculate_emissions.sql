WITH construction_sample AS (
	SELECT id_pand, 
		CASE 
			WHEN status = 'Pand gesloopt' THEN LEFT(registration_start, 4)::INTEGER
			WHEN status != 'Pand gesloopt' AND registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
			ELSE LEFT(registration_start, 4)::INTEGER
		END AS year, 
		status, sqm, geom, geom_28992, neighborhood_code, municipality
	FROM housing_nl
	WHERE municipality = 'Amsterdam'
), 
inuse_sample AS (
	SELECT id_pand, year, status, sqm, geom, geom_28992, neighborhood_code, municipality
	FROM housing_inuse_2012_2022
	WHERE municipality = 'Amsterdam'
		AND year < 2022
), 
inuse_lowenergy AS (
	SELECT 
		b.id_pand, b.year, 'Pand in gebruik - low energy' AS status, b.sqm, b.geom, b.geom_28992, b.neighborhood_code, b.municipality
	FROM construction_sample a 
	LEFT JOIN inuse_sample b 
	ON a.id_pand = b.id_pand
	WHERE a.status != 'Pand gesloopt'
		AND a.year < 2022
		AND b.id_pand IS NOT NULL
), 
inuse_normalenergy AS (
	SELECT b.*
	FROM construction_sample a 
	RIGHT JOIN inuse_sample b 
	ON a.id_pand = b.id_pand
	WHERE a.id_pand IS NULL 
), 
buildings_sample AS (
	SELECT * FROM construction_sample
	UNION ALL 
	SELECT * FROM inuse_lowenergy
	UNION ALL 
	SELECT * FROM inuse_normalenergy
), 

-- join neighborhood sqm and energy into to buildings 
sqm_neighborhood AS (
	SELECT municipality, neighborhood_code, year, SUM(sqm) AS sqm_neighborhood
	FROM inuse_sample
	GROUP BY municipality, neighborhood_code, year
), 
energy_sample AS (
	SELECT year, neighborhood_code, municipality, 
		electricity_kwh * n_households AS electricity_kwh_buurt, 
		gas_m3 * n_households AS gas_m3_buurt 
	FROM cbs_map_all 
	WHERE municipality = 'Amsterdam'
), 
neighborhood_stats AS (
	SELECT b.*, a.sqm_neighborhood
	FROM sqm_neighborhood a 
	JOIN energy_sample b 
	ON a.neighborhood_code = b.neighborhood_code
		AND a.year = b.year
), 


-- emissions 
emissions_input AS (
	SELECT a.id_pand, a.year, a.status, a.sqm, 
		ROUND(a.sqm / b.sqm_neighborhood * gas_m3_buurt) AS gas_m3_s0, 
		CASE 
			WHEN status IN ('Pand in gebruik', 'Pand in gebruik - low energy') THEN ROUND(a.sqm / b.sqm_neighborhood * electricity_kwh_buurt) 
			ELSE 0 
		END AS electricity_kwh_s0, 
		CASE
			WHEN status = 'Pand in gebruik' THEN ROUND(a.sqm / b.sqm_neighborhood * gas_m3_buurt)
			WHEN status = 'Pand in gebruik - low energy' THEN a.sqm * 5
			ELSE 0 
		END AS gas_m3_s1, 
		CASE 
			WHEN status IN ('Pand in gebruik', 'Pand in gebruik - low energy') THEN ROUND(a.sqm / b.sqm_neighborhood * electricity_kwh_buurt) 
			ELSE 0 
		END AS electricity_kwh_s1
	FROM buildings_sample a 
	JOIN neighborhood_stats b
	ON a.neighborhood_code = b.neighborhood_code
		AND a.year = b.year
), 
emissions_kg AS (
	SELECT id_pand, year, status, sqm, 
	
		ROUND(gas_m3_s0 * 1.9 + electricity_kwh_s0 * 0.45) AS operational_kg_s0, 
		CASE 
			WHEN status IN ('transformation - adding units', 'transformation - function change') THEN sqm * 126
			WHEN status = 'Bouw gestart' THEN sqm * 316
			ELSE 0 
		END AS embodied_kg_s0, 
		
		ROUND(gas_m3_s1 * 1.9 + electricity_kwh_s1 * 0.45) AS operational_kg_s1, 
		CASE 
			WHEN status IN ('transformation - adding units', 'transformation - function change') THEN sqm * 200
			WHEN status = 'Bouw gestart' THEN sqm * 800
			ELSE 0 
		END AS embodied_kg_s1
	FROM emissions_input 
)

SELECT DISTINCT status FROM emissions_kg

	
