DROP TABLE IF EXISTS buurt_stats; 
CREATE TABLE buurt_stats AS 

WITH cbs AS (
	SELECT * FROM cbs_map_all_buurt 
), 
inuse AS (
	SELECT year, bu_code, SUM(sqm) AS inuse
	FROM housing_inuse_2012_2021 
	WHERE year >= 2012 AND year <= 2021
	GROUP BY year, bu_code
), 
actual_emissions AS (
	SELECT year, bu_code, 
		construction, renovation, transformation, demolition, 
		embodied_kg, operational_kg
	FROM emissions_all_buurt 
), 
merge_cbs_buildingactivity AS (
	SELECT a.*, b.construction, b.renovation, b.transformation, b.demolition, 
		b.embodied_kg, b.operational_kg
	FROM cbs a 
	JOIN actual_emissions b 
	ON a.bu_code = b.bu_code AND a.year = b.year 
), 
merge_inuse AS (
	SELECT a.*, b.inuse
	FROM merge_cbs_buildingactivity a 
	JOIN inuse b 
	ON a.bu_code = b.bu_code AND a.year = b.year 
), 
av_build_year AS (
	SELECT bu_code, AVG(build_year::INTEGER) AS av_build_year
	FROM bag_pand 
	WHERE 
		status = 'Pand in gebruik'
		AND build_year != '9999'
		AND build_year::INTEGER <= 2021
	GROUP BY bu_code 
), 
merge_build_year AS (
	SELECT a.*, b.av_build_year 
	FROM merge_inuse a 
	LEFT JOIN av_build_year b 
	ON a.bu_code = b.bu_code 
)

SELECT * FROM merge_build_year


