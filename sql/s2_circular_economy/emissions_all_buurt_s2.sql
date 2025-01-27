DELETE FROM emissions_all_buurt_s2
WHERE municipality = %s AND year = {self.year};

INSERT INTO emissions_all_buurt_s2 (
    year, municipality, wk_code, bu_code, bu_geom,
    construction, demolition, transformation, renovation,
    operational_kg_s0, operational_kg_s1, operational_kg_s2,
    embodied_kg_s0, embodied_kg_s1, embodied_kg_s2,
    inuse, gas_m3_s0, gas_m3_s1, gas_m3_s2,
    electricity_kwh_s0, electricity_kwh_s1, electricity_kwh_s2,
    n_homes, population, woz
)

-- 1. Make record of in use buildings 
-- add construction activity from housing_nl_s2 
WITH housing_nl AS (
	SELECT bu_code, id_pand, status, function, sqm, pand_geom AS pd_geom, 
		CASE 
			WHEN registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
			ELSE LEFT(registration_start, 4)::INTEGER
		END AS year
	FROM housing_nl_s2
	WHERE municipality = %s 
), 
inuse AS (
	SELECT * FROM housing_inuse_2012_2021 
	WHERE municipality = %s AND year = {self.year}
),  
-- remove invalid in-use buildings (buildings where status = 'construction - invalid') check years before as well
invalid_constructions AS (
	SELECT DISTINCT id_pand FROM housing_nl 
	WHERE status = 'construction - invalid' AND year <= {self.year}
), 
inuse_without_invalid_constructions AS (
	SELECT * FROM inuse
	WHERE id_pand NOT IN (
		SELECT id_pand FROM invalid_constructions 
	)
), 

-- add in-use buildings where status = 'renovation - s1' (be sure to check years before as well)
new_renovations AS (
	SELECT DISTINCT ON (id_pand) 
		id_pand, sqm
	FROM housing_nl 
	WHERE status = 'renovation - s1' AND year <= {self.year}
), 
inuse_with_new_renovations AS (
	SELECT 
		{self.year} AS year, id_pand, 'in use - s' AS status, sqm, 
		NULL AS tot_gas_m3, NULL AS tot_elec_kwh, 
		bu_code, %s AS municipality 
	FROM housing_nl 
	WHERE status = 'renovation - s1'

	UNION ALL 

	SELECT 
		{self.year} AS year, id_pand, 'in use' AS status, sqm, 
		tot_gas_m3, tot_elec_kwh, 
		bu_code, %s AS municipality 
	FROM inuse_without_invalid_constructions
), 

-- 2. Calculate operational emissions using in-use buildings 
-- find av_gas_per_sqm and av_elec_per_sqm for inuse buildings that don't have energy use data 
energy_data_buurt AS (
	SELECT * FROM cbs_map_all_buurt
	WHERE municipality = %s AND year = {self.year}
), 
inuse_sqm_per_buurt AS (
	SELECT bu_code, SUM(sqm) AS inuse_sqm FROM inuse GROUP BY bu_code
), 
per_sqm_energy_use_for_buurt AS (
	SELECT a.bu_code, a.year, b.inuse_sqm, 
		tot_gas_m3 / inuse_sqm AS gas_m3_per_sqm, 
		tot_elec_kwh / inuse_sqm AS elec_kwh_per_sqm, 
		a.tot_gas_m3, a.tot_elec_kwh
	FROM energy_data_buurt a 
	LEFT JOIN inuse_sqm_per_buurt b 
	ON a.bu_code = b.bu_code
), 
inuse_with_per_sqm_energy_use AS (
	SELECT a.*, 
		b.gas_m3_per_sqm, b.elec_kwh_per_sqm 
	FROM inuse_with_new_renovations a 
	LEFT JOIN per_sqm_energy_use_for_buurt b 
	ON a.bu_code = b.bu_code 
), 
-- estimate energy use using av_gas_per_sqm and av_elec_per_sqm 
inuse_with_energy_use AS (
	SELECT year, id_pand, status, sqm, 
		CASE 
			WHEN tot_gas_m3 IS NULL THEN gas_m3_per_sqm * sqm 
			ELSE tot_gas_m3 
		END AS tot_gas_m3, 
		CASE 
			WHEN tot_elec_kwh IS NULL THEN elec_kwh_per_sqm * sqm 
			ELSE tot_elec_kwh 
		END AS tot_elec_kwh, 
		bu_code, municipality
	FROM inuse_with_per_sqm_energy_use
), 
-- calculate operational emissions using energy use data 
inuse_with_operational_emissions AS (
	SELECT *, 
		tot_gas_m3 * 1.9 + tot_elec_kwh * 0.45 AS operational_kg 
	FROM inuse_with_energy_use
), 

-- 3. Calculate embodied emissions 
-- get building activity data 
building_activity AS (
	SELECT 
		year, id_pand, 
		CASE 
			WHEN status = 'Pand gesloopt' THEN 'demolition' 
			WHEN status IN ('renovation - pre{self.year}', 'renovation - post{self.year}', 'renovation - s1') THEN 'renovation' 
			WHEN status IN ('transformation - function change', 'transformation - adding units') THEN 'transformation' 
			WHEN status = 'Bouw gestart' THEN 'construction' 
			ELSE NULL 
		END AS status, 
		function, sqm, bu_code, %s AS municipality
	FROM housing_nl
	WHERE status != 'construction - invalid' AND year = {self.year}
), 
-- calculate embodied emissions according to status 
building_activity_with_embodied_emissions AS (
	SELECT *, 
		CASE 
			WHEN status = 'construction' THEN sqm * 316 
			WHEN status IN ('renovation', 'transformation') THEN sqm * 126 
			WHEN status = 'demolition' THEN sqm * 77 
			ELSE NULL 
		END AS embodied_kg 
	FROM building_activity
), 

-- 4. Aggregate numbers to emissions_all_buurt_s2 
emissions_all_per_pand AS (
	SELECT year, id_pand, status, embodied_kg, 0 AS operational_kg, sqm, 
		NULL AS tot_gas_m3, NULL AS tot_elec_kwh, bu_code
	FROM building_activity_with_embodied_emissions

	UNION ALL 

	SELECT year, id_pand, status, 0 AS embodied_kg, ROUND(operational_kg) AS operational_kg, 
		sqm, tot_gas_m3, tot_elec_kwh, bu_code 
	FROM inuse_with_operational_emissions
), 
emissions_per_pand_with_status_columns AS (
	SELECT *, 
		CASE WHEN status = 'construction' THEN sqm ELSE 0 END AS construction, 
		CASE WHEN status = 'renovation' THEN sqm ELSE 0 END AS renovation, 
		CASE WHEN status = 'transformation' THEN sqm ELSE 0 END AS transformation, 
		CASE WHEN status = 'demolition' THEN sqm ELSE 0 END AS demolition, 
		CASE WHEN status IN ('in use - s', 'in use') THEN sqm ELSE 0 END AS inuse
	FROM emissions_all_per_pand
), 
emissions_per_buurt AS (
	SELECT {self.year} AS year, bu_code, 
		SUM(construction) AS construction, SUM(renovation) AS renovation, 
		SUM(transformation) AS transformation, SUM(demolition) AS demolition, 
		SUM(inuse) AS inuse, 
		SUM(tot_gas_m3) AS tot_gas_m3, SUM(tot_elec_kwh) AS tot_elec_kwh, 
		SUM(embodied_kg) AS embodied_kg_s2, SUM(operational_kg) AS operational_kg_s2
	FROM emissions_per_pand_with_status_columns
	GROUP BY bu_code 
), 
emissions_other_scenarios AS (
	SELECT * FROM emissions_all_buurt_s1
	WHERE municipality = %s AND year = {self.year}
), 
final_table AS (
	SELECT a.year, b.municipality, b.wk_code, b.bu_code, b.bu_geom, 
		a.construction, a.demolition, a.transformation, a.renovation, 
		b.operational_kg_s0, b.operational_kg_s1, a.operational_kg_s2, 
		b.embodied_kg_s0, b.embodied_kg_s1, a.embodied_kg_s2, 
		a.inuse, 
		b.gas_m3_s0, b.gas_m3_s1, ROUND(a.tot_gas_m3) AS gas_m3_s2, 
		b.electricity_kwh_s0, b.electricity_kwh_s1, ROUND(a.tot_elec_kwh) AS electricity_kwh_s2, 
		b.n_homes, b.population, b.woz 
	FROM emissions_per_buurt a 
	FULL JOIN emissions_other_scenarios b 
	ON a.bu_code = b.bu_code 
)

SELECT * FROM final_table 

