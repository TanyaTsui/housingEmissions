SELECT year, 
	SUM(construction) AS construction, SUM(renovation) AS renovation, 
	SUM(transformation) AS transformation, SUM(demolition) AS demolition, 
	SUM(embodied_kg) AS embodied, SUM(operational_kg) AS operational
FROM emissions_all_buurt 
WHERE municipality = '''s-Gravenhage'
GROUP BY year
ORDER BY year


-- SELECT 
-- 	LEFT(registration_start, 4) AS year,
-- 	SUM(CASE WHEN status = 'Bouw gestart' THEN sqm ELSE 0 END) AS construction, 
-- 	SUM(CASE WHEN status IN ('transformation - adding units', 'transformation - function change') THEN sqm ELSE 0 END) AS transformation, 
-- 	SUM(CASE WHEN status IN ('renovation - pre2020', 'renovation - post2020') THEN sqm ELSE 0 END) AS renovation, 
-- 	SUM(CASE WHEN status = 'Pand gesloopt' THEN sqm ELSE 0 END) AS demolition
-- FROM housing_nl 
-- WHERE municipality = '''s-Gravenhage'
-- 	AND LEFT(registration_start, 4)::INTEGER >= 2012
-- 	AND LEFT(registration_start, 4)::INTEGER <= 2021
-- GROUP BY LEFT(registration_start, 4)
-- ORDER BY LEFT(registration_start, 4)


-- SELECT year, COUNT(*) 
-- FROM bag_pand 
-- WHERE municipality = '''s-Gravenhage'
-- GROUP BY year 
-- ORDER BY year