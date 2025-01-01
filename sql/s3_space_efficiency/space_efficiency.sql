DELETE FROM emissions_all_buurt_s3 WHERE municipality = 'Delft';
INSERT INTO emissions_all_buurt_s3 (
    year, municipality, wk_code, bu_code, bu_geom,
    embodied_kg_s0, embodied_kg_s1, embodied_kg_s2, embodied_kg_s3,
    operational_kg_s0, operational_kg_s1, operational_kg_s2, operational_kg_s3,
    construction, construction_s3, transformation, transformation_s3,
    renovation, demolition, inuse, inuse_s3,
    tot_gas_m3, tot_gas_m3_s3, tot_elec_kwh, tot_elec_kwh_s3,
    population, population_change, woz, n_homes
)

WITH emissions_all_buurt_without_population AS (
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
	WHERE municipality = 'Delft'
), 
emissions_all_buurt AS (
	SELECT a.*, 
		b.population, b.n_homes, b.tot_gas_m3, b.tot_elec_kwh, b.woz
	FROM emissions_all_buurt_without_population a 
	LEFT JOIN (SELECT * FROM emissions_all_buurt WHERE municipality = 'Delft') b 
	ON a.year = b.year AND a.bu_code = b.bu_code 
), 

-- population increase 
population_change AS (
	SELECT b.population - a.population AS population_change, 
		a.* 
	FROM emissions_all_buurt a 
	LEFT JOIN (SELECT bu_code, year, population FROM emissions_all_buurt) b 
	ON a.bu_code = b.bu_code 
		AND a.year + 1 = b.year
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
		construction, construction_s3, transformation, transformation_s3, renovation, demolition, 
		population, population_change, n_homes, tot_gas_m3, tot_elec_kwh, woz, 
		year, municipality, wk_code, bu_code, bu_geom 
	FROM sqm
), 

-- get real sqm in-use values for each bu_code + year
housing_inuse AS (
	SELECT bu_code, year, SUM(sqm) AS inuse
	FROM housing_inuse_2012_2021 
	WHERE municipality = 'Delft'
	GROUP BY bu_code, year
), 
embodied_emissions_with_inuse AS (
	SELECT a.*, b.inuse 
	FROM embodied_emissions a 
	FULL JOIN housing_inuse b -- TODO: double check this join 
	ON a.bu_code = b.bu_code AND a.year = b.year 
), 
values_yearbefore AS (
	SELECT a.*, 
		b.inuse AS inuse_lastyear, b.construction AS construction_lastyear, 
		b.transformation AS transformation_lastyear 
	FROM embodied_emissions_with_inuse a 
	LEFT JOIN embodied_emissions_with_inuse b 
	ON a.bu_code = b.bu_code AND a.year - 1 = b.year
), 

-- calculate in-use sqm for scenario 3 
s3_inuse AS (
	SELECT 
		CASE 
			WHEN year = 2012 THEN inuse 
			WHEN construction = construction_s3 AND transformation = transformation_s3 THEN inuse
			ELSE inuse_lastyear + construction_lastyear + transformation_lastyear
		END AS inuse_s3,
		* 
	FROM values_yearbefore
), 
s3_inuse_adjusted AS (
	SELECT 
		CASE 
			WHEN inuse_s3 > inuse THEN inuse 
			ELSE inuse_s3
		END AS inuse_s3, 
	*
	FROM s3_inuse_adjusted
)

-- calculate energy usage (gas and electricity) for s3 
s3_energy AS (
	SELECT 
		ROUND(tot_gas_m3 / inuse * inuse_s3) AS tot_gas_m3_s3, 
		ROUND(tot_elec_kwh / inuse * inuse_s3) AS tot_elec_kwh_s3, 
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
		construction, construction_s3, transformation, transformation_s3, 
		renovation, demolition, inuse, inuse_s3, 
		tot_gas_m3, tot_gas_m3_s3, tot_elec_kwh, tot_elec_kwh_s3, 
		population, population_change, woz, n_homes 
	FROM s3_operational_emissions
)

SELECT * FROM final_table 


