-- SELECT year, 
-- 	SUM(construction) AS construction, SUM(renovation) AS renovation, 
-- 	SUM(transformation) AS transformation, SUM(demolition) AS demolition, 
-- 	SUM(embodied_kg) AS embodied, SUM(operational_kg) AS operational
-- FROM emissions_all_buurt 
-- WHERE municipality = '''s-Gravenhage'
-- GROUP BY year
-- ORDER BY year

-- WITH housing_nl AS (
-- 	SELECT 
-- 		CASE 
-- 			WHEN registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
-- 	        ELSE LEFT(registration_start, 4)::INTEGER
-- 	    END AS year, 
-- 		CASE WHEN status = 'Bouw gestart' THEN sqm ELSE 0 END AS construction, 
-- 		CASE WHEN status IN ('transformation - adding units', 'transformation - function change') THEN sqm ELSE 0 END AS transformation, 
-- 		CASE WHEN status IN ('renovation - pre2020', 'renovation - post2020') THEN sqm ELSE 0 END AS renovation, 
-- 		CASE WHEN status = 'Pand gesloopt' THEN sqm ELSE 0 END AS demolition
-- 	FROM housing_nl 
-- 	WHERE municipality = '''s-Gravenhage'
-- )
-- SELECT year, 
-- 	SUM(construction) AS construction, SUM(transformation) AS transformation, 
-- 	SUM(renovation) AS renovation, SUM(demolition) AS demolition 
-- FROM housing_nl
-- WHERE year >= 2012 AND year <= 2021
-- GROUP BY year
-- ORDER BY year



-- SELECT year, status, COUNT(*)
-- FROM bag_pand
-- WHERE municipality = '''s-Gravenhage'
-- 	AND status IN ('Bouw gestart', 'Bouwvergunning verleend') --  IN ('Pand gesloopt', 'Sloopvergunning verleend')
-- 	AND (year BETWEEN 2012 AND 2021)
-- GROUP BY year, status 
-- ORDER BY year, status



WITH buurt2022_municipality AS (
	SELECT "GM_NAAM" AS municipality, "WK_CODE" AS wk_code, 
		"BU_CODE" AS bu_code, geometry AS bu_geom
	FROM cbs_map_2022 
	WHERE "WATER" = 'NEE' AND "GM_NAAM" = '''s-Gravenhage'
), 
bag_pand_with_admin_boundaries AS (
	SELECT a.id_pand, a.build_year, a.status, a.document_date, a.document_number, 
		a.registration_start, a.registration_end, a.pand_geom, 
		b.*
	FROM bag_pand a 
	JOIN buurt2022_municipality b 
	ON a.pand_geom && b.bu_geom
		AND ST_Within(a.pand_geom, b.bu_geom)
), 
bag_pand_final AS (
	SELECT 
		CASE 
			WHEN registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
			ELSE LEFT(registration_start, 4)::INTEGER
		END AS year, *
	FROM bag_pand_with_admin_boundaries
)

SELECT * FROM bag_pand_final WHERE year = 2012



-- SELECT year, COUNT(*) 
-- FROM bag_pand 
-- WHERE municipality = '''s-Gravenhage'
-- GROUP BY year 
-- ORDER BY year