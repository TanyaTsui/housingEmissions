DELETE FROM emissions_all_buurt_s1 WHERE year = {self.year} AND municipality = %s;
INSERT INTO emissions_all_buurt_s1 (
	year, municipality, wk_code, bu_code, bu_geom, population, woz, n_homes, inuse, 
	construction, transformation, renovation, demolition,
	gas_m3_s0, gas_m3_s1, electricity_kwh_s0, electricity_kwh_s1, 
	operational_kg_s0, operational_kg_s1, embodied_kg_s0, embodied_kg_s1
)

-- get buurt_stats: buurt level energy use and in-use sqm data 
WITH cbs_stats_buurt AS (
	SELECT * FROM cbs_map_all_buurt WHERE municipality = %s AND year = {self.year}
), 
housing_inuse AS (
	SELECT * FROM housing_inuse_2012_2021 WHERE municipality = %s AND year = {self.year}
), 
housing_inuse_buurt AS (
	SELECT municipality, bu_code, year, SUM(sqm) AS sqm, SUM(n_units) AS n_units
	FROM housing_inuse
	GROUP BY municipality, bu_code, year
), 
buurt_stats AS (
	SELECT b.*, a.sqm
	FROM housing_inuse_buurt a 
	JOIN cbs_stats_buurt b 
	ON a.municipality = b.municipality 
		AND a.year = b.year
		AND a.bu_code = b.bu_code 
), 

-- get all constructions and renovations that happened before year  
construction_municipality AS ( -- all construction activity in year
	SELECT id_pand, 
		CASE 
			WHEN registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
			ELSE LEFT(registration_start, 4)::INTEGER
		END AS year, 
		status, sqm, n_units, pand_geom, bu_code, wk_code, municipality
	FROM housing_nl
	WHERE municipality = %s
		AND ahn_version IS NULL
), 
construction_sample AS (
	SELECT * FROM construction_municipality 
	WHERE year <= {self.year}
), 

-- identify in-use buildings that were previously constructed or renovated (low-energy)
inuse_lowenergy AS (
	SELECT 
		b.municipality, b.bu_code, b.id_pand, b.year, 
		'Pand in gebruik - low energy' AS status, 
		b.tot_gas_m3, b.tot_elec_kwh, 
		b.sqm, b.n_units
	FROM (SELECT DISTINCT ON (id_pand) id_pand, status FROM construction_sample) a 
	LEFT JOIN housing_inuse b 
	ON a.id_pand = b.id_pand
	WHERE a.status != 'Pand gesloopt'
		AND b.id_pand IS NOT NULL
), 
inuse_normalenergy AS (
	SELECT 
		b.municipality, b.bu_code, b.id_pand, 
		b.year, 'Pand in gebruik' AS status, 
		b.tot_gas_m3, b.tot_elec_kwh, 
		b.sqm, b.n_units
	FROM construction_sample a 
	RIGHT JOIN housing_inuse b 
	ON a.id_pand = b.id_pand
	WHERE a.id_pand IS NULL 
), 
buildings_all AS (
	-- all construction / renovation / transformation / demolition activity in year
	SELECT municipality, bu_code, id_pand, year, status, 
		0 AS tot_gas_m3, 0 AS tot_elec_kwh, 
		sqm, n_units
	FROM construction_sample 
	WHERE year = {self.year}
	
	UNION ALL 
	
	-- low energy in use buildings in year
	SELECT * FROM inuse_lowenergy
	
	UNION ALL 
	
	-- non-low energy in use buildings in year
	SELECT * FROM inuse_normalenergy
), 

-- calculate energy use per building according to low or normal energy use status
energy_use_per_building AS (
	SELECT 
		COALESCE(a.municipality, b.municipality) AS municipality, b.wk_code, 
		COALESCE(a.bu_code, b.bu_code) AS bu_code, b.bu_geom, 
		a.id_pand, a.year, a.status, a.sqm, b.n_homes, b.population, b.woz, 
	
		-- a.tot_gas_m3 AS gas_m3_s0, -- not needed, this is already in emissions_all_buurt
		CASE
			WHEN status = 'Pand in gebruik' THEN a.tot_gas_m3 
			WHEN status = 'Pand in gebruik - low energy' AND a.sqm * 5 < a.tot_gas_m3  THEN a.sqm * 5
			WHEN status = 'Pand in gebruik - low energy' AND a.sqm * 5 >= a.tot_gas_m3  THEN a.tot_gas_m3 
			ELSE 0 
		END AS gas_m3_s1,
	
		-- a.tot_elec_kwh AS electricity_kwh_s0, 
		CASE 
			WHEN status IN ('Pand in gebruik', 'Pand in gebruik - low energy') THEN a.tot_elec_kwh 
			ELSE 0 
		END AS electricity_kwh_s1
	FROM buildings_all a 
	FULL JOIN buurt_stats b 
	ON a.bu_code = b.bu_code
), 
emissions_per_building AS (
	SELECT 
		municipality, wk_code, bu_code, bu_geom, id_pand, 
		year, status, sqm, n_homes, population, woz,
		-- gas_m3_s0, electricity_kwh_s0, 
		-- ROUND(gas_m3_s0 * 1.9 + electricity_kwh_s0 * 0.45) AS operational_kg_s0, 	
		gas_m3_s1, electricity_kwh_s1, 
		ROUND(gas_m3_s1 * 1.9 + electricity_kwh_s1 * 0.45) AS operational_kg_s1, 
		
		CASE 
			WHEN status IN ('transformation - adding units', 'transformation - function change', 
							'renovation - pre2020', 'renovation - post2020') THEN sqm * 126
			WHEN status = 'Bouw gestart' THEN sqm * 325
			WHEN status = 'Pand gesloopt' THEN sqm * 77
			ELSE 0 
		END AS embodied_kg_s0, 
		CASE 
			WHEN status IN ('transformation - adding units', 'transformation - function change', 
							'renovation - pre2020', 'renovation - post2020') THEN sqm * 200
			WHEN status = 'Bouw gestart' THEN sqm * 550
			WHEN status = 'Pand gesloopt' THEN sqm * 77
			ELSE 0 
		END AS embodied_kg_s1
		
	FROM energy_use_per_building 
), 
stats_per_building AS (
	SELECT 
		CASE 
			WHEN status IN ('Pand in gebruik', 'Pand in gebruik - low energy') THEN sqm ELSE 0
		END AS inuse, 
		CASE 
			WHEN status = 'Bouw gestart' THEN sqm ELSE 0 
		END AS construction, 
		CASE 
			WHEN status IN ('transformation - adding units', 'transformation - function change') THEN sqm ELSE 0
		END AS transformation, 
		CASE 
			WHEN status IN ('renovation - pre2020', 'renovation - post2020') THEN sqm ELSE 0 
		END AS renovation, 
		CASE 
			WHEN status = 'Pand gesloopt' THEN sqm ELSE 0 
		END AS demolition, *
	FROM emissions_per_building 
), 
s1_results AS (
	SELECT {self.year} AS year, municipality, wk_code, bu_code, bu_geom,  
		ROUND(AVG(population)) AS population, ROUND(AVG(woz)) AS woz, 
		
		SUM(n_homes) AS n_homes, SUM(inuse) AS inuse, SUM(construction) AS construction, SUM(transformation) AS transformation, 
		SUM(renovation) AS renovation, SUM(demolition) AS demolition, 
		
		-- ROUND(SUM(gas_m3_s0)) AS gas_m3_s0, 
		ROUND(SUM(gas_m3_s1)) AS gas_m3_s1, 
		-- ROUND(SUM(electricity_kwh_s0)) AS electricity_kwh_s0, 
		ROUND(SUM(electricity_kwh_s1)) AS electricity_kwh_s1, 
	
		-- SUM(operational_kg_s0) AS operational_kg_s0, 
		SUM(operational_kg_s1) AS operational_kg_s1, 
		SUM(embodied_kg_s0) AS embodied_kg_s0, SUM(embodied_kg_s1) AS embodied_kg_s1
		
	FROM stats_per_building
	GROUP BY municipality, wk_code, bu_code, bu_geom
), 
s0_results AS (
	SELECT bu_code, 
		tot_gas_m3 AS gas_m3_s0, 
		tot_elec_kwh AS electricity_kwh_s0, 
		ROUND(embodied_kg) AS embodied_kg_s0, 
		ROUND(operational_kg) AS operational_kg_s0 
	FROM emissions_all_buurt 
	WHERE municipality = %s AND year = {self.year}
)

SELECT 
	a.year, a.municipality, a.wk_code, a.bu_code, a.bu_geom, 
	a.population, a.woz, a.n_homes, a.inuse, 
	a.construction, a.transformation, a.renovation, a.demolition, 
	b.gas_m3_s0, a.gas_m3_s1, b.electricity_kwh_s0, a.electricity_kwh_s1, 
	b.operational_kg_s0, a.operational_kg_s1, b.embodied_kg_s0, a.embodied_kg_s1	
FROM s1_results a 
LEFT JOIN s0_results b
ON a.bu_code = b.bu_code 