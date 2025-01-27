-- DELETE FROM emissions_all_buurt_s3 WHERE municipality = 'Delft' and year = 2013;
-- INSERT INTO emissions_all_buurt_s3 (
-- 	year, municipality, wk_code, bu_code, bu_geom,
--     embodied_kg_s0, embodied_kg_s1, embodied_kg_s2, embodied_kg_s3,
--     operational_kg_s0, operational_kg_s1, operational_kg_s2, operational_kg_s3,
--     construction, construction_s2, construction_s3, transformation, transformation_s2, transformation_s3,
--     renovation, renovation_s2, demolition, demolition_s2, inuse, inuse_s3,
--     tot_gas_m3, tot_gas_m3_s3, tot_elec_kwh, tot_elec_kwh_s3,
--     population, population_change, woz, n_homes
-- )

WITH emissions_all_buurt AS (
	SELECT  
		CASE 
			WHEN construction = 0 AND transformation = 0 THEN 0 
			ELSE ROUND(construction / (construction + transformation), 3) 
		END AS construction_perc, 
		CASE 
			WHEN construction = 0 AND transformation = 0 THEN 0 
			ELSE ROUND(transformation / (construction + transformation), 3)
		END AS transformation_perc, 
		* 
	FROM emissions_all_buurt_s2
	WHERE municipality = 'Delft' AND year = 2013
), 
emissions_all_buurt_nextyear AS (
	SELECT * 
	FROM emissions_all_buurt_s2
	WHERE municipality = 'Delft' AND year = 2013 + 1
), 

-- population increase 
population_change AS (
	SELECT b.population - a.population AS population_change,
		a.* 
	FROM emissions_all_buurt a 
	LEFT JOIN emissions_all_buurt_nextyear b 
	ON a.bu_code = b.bu_code 
), 


-- sqm of construction and transformation in scenario 3 
sqm AS (
	SELECT 
		CASE
			WHEN construction = 0 THEN 0 
			WHEN population_change IS NULL THEN construction
			WHEN population_change <= 0 THEN construction 
			WHEN population_change > 0 AND (population_change * 25 * construction_perc) < construction 
				THEN ROUND(population_change * 25 * construction_perc)
			WHEN population_change > 0 AND (population_change * 25 * construction_perc) >= construction 
				THEN construction 
		END AS construction_s3, 
		CASE
			WHEN transformation = 0 THEN 0 
			WHEN population_change IS NULL THEN transformation
			WHEN population_change <= 0 THEN transformation 
			WHEN population_change > 0 AND (population_change * 25 * transformation_perc) < transformation 
				THEN ROUND(population_change * 25 * transformation_perc)
			WHEN population_change > 0 AND (population_change * 25 * transformation_perc) >= transformation 
				THEN transformation 
		END AS transformation_s3, 
		* 
	FROM population_change
), 

-- calculate embodied emissions
embodied_emissions AS (
	SELECT 
		embodied_kg_s0, embodied_kg_s1, embodied_kg_s2,  
		operational_kg_s0, operational_kg_s1, operational_kg_s2, 
		construction_s3*316 + transformation_s3*126 + renovation*126 + demolition*77 AS embodied_kg_s3, 
		construction, construction_s2, construction_s3, 
		transformation, transformation_s2, transformation_s3, 
		renovation, renovation_s2, demolition, demolition_s2, 
		population, population_change, n_homes, 
		gas_m3_s0 AS tot_gas_m3, electricity_kwh_s0 AS tot_elec_kwh, 
		woz, year, municipality, wk_code, bu_code, bu_geom 
	FROM sqm
), 
emissions_all_buurt_lastyear AS (
	SELECT * 
	FROM emissions_all_buurt_s3
	WHERE municipality = 'Delft' AND year = 2013 - 1
), 
inuse_lastyear_s3 AS (
	SELECT bu_code, SUM(inuse) AS inuse 
	FROM emissions_all_buurt_lastyear
	GROUP BY bu_code
), 
embodied_emissions_with_inuse_lastyear AS (
	SELECT b.inuse AS inuse_lastyear, a.*
	FROM embodied_emissions a 
	LEFT JOIN inuse_lastyear_s3 b 
	ON a.bu_code = b.bu_code
), 
building_activity_lastyear_s3 AS (
	SELECT year, bu_code, construction_s3, transformation_s3
	FROM emissions_all_buurt_lastyear
), 
embodied_emissions_with_values_lastyear AS (
	SELECT 
		b.construction_s3 AS construction_lastyear, 
		b.transformation_s3 AS transformation_lastyear, 
		a.*
	FROM embodied_emissions_with_inuse_lastyear a 
	LEFT JOIN building_activity_lastyear_s3 b 
	ON a.bu_code = b.bu_code
), 
inuse_s0 AS (
	SELECT bu_code, SUM(sqm) AS inuse
	FROM housing_inuse_2012_2021
	WHERE municipality = 'Delft' AND year = 2013
	GROUP BY bu_code
), 
embodied_emissions_with_inuse_s0 AS (
	SELECT
		b.inuse AS inuse_s0, a.*
	FROM embodied_emissions_with_values_lastyear a 
	LEFT JOIN inuse_s0 b 
	ON a.bu_code = b.bu_code
), 

-- calculate in-use sqm for scenario 3 
s3_inuse AS (
	SELECT 
		CASE 
			WHEN year = 2012 THEN inuse_s0
			-- WHEN construction = construction_s3 AND transformation = transformation_s3 THEN inuse_s0
			ELSE inuse_lastyear + construction_lastyear + transformation_lastyear
		END AS inuse_s3,
		* 
	FROM embodied_emissions_with_inuse_s0
), 
s3_inuse_adjusted AS (
	SELECT 
		CASE 
			WHEN inuse_s3 > inuse_s0 THEN inuse_s0
			ELSE inuse_s3
		END AS inuse_s3_adjusted, 
	*
	FROM s3_inuse
), 

-- calculate energy usage (gas and electricity) for s3 
s3_energy AS (
	SELECT 
		ROUND(tot_gas_m3 / inuse_s0 * inuse_s3_adjusted) AS tot_gas_m3_s3, 
		ROUND(tot_elec_kwh / inuse_s0 * inuse_s3_adjusted) AS tot_elec_kwh_s3, 
		* 
	FROM s3_inuse_adjusted
), 
-- calculate operational emissions for s3 
s3_operational_emissions AS (
	SELECT 
		ROUND(tot_gas_m3_s3 * 1.9 + tot_elec_kwh_s3 * 0.45) AS operational_kg_s3, 
		* 
	FROM s3_energy
), 
final_table AS (
	SELECT 
		year, municipality, wk_code, bu_code, bu_geom, 
		embodied_kg_s0, embodied_kg_s1, embodied_kg_s2, embodied_kg_s3, 
		ROUND(operational_kg_s0) AS operational_kg_s0, 
		operational_kg_s1, operational_kg_s2, operational_kg_s3, 
		construction, construction_s2, construction_s3, transformation, transformation_s2, transformation_s3, 
		renovation, renovation_s2, demolition, demolition_s2, inuse_s0, inuse_s3_adjusted, 
		tot_gas_m3, tot_gas_m3_s3, tot_elec_kwh, tot_elec_kwh_s3, 
		population, population_change, woz, n_homes 
	FROM s3_operational_emissions
)

SELECT year, bu_code, 
	inuse_s0, inuse_s3_adjusted, 
	operational_kg_s0, operational_kg_s3
FROM final_table 
WHERE operational_kg_s3 != operational_kg_s0


