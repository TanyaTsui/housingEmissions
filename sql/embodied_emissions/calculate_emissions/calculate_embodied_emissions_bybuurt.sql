-- DELETE FROM emissions_embodied_bybuurt
-- WHERE municipality = 'Amsterdam';

-- INSERT INTO emissions_embodied_bybuurt(
-- 	municipality, neighborhood_code, year, in_use,
--     construction, renovation, transformation, demolition, emissions_embodied_kg
-- )
	
WITH housing_events_raw AS (
	SELECT 
		CASE
	        WHEN status = 'Bouw gestart' THEN LEFT(registration_end, 4)::INTEGER
	        ELSE LEFT(registration_start, 4)::INTEGER
	    END AS year, 
		CASE
			WHEN status = 'Bouw gestart' THEN 'construction'
			WHEN status = 'Pand gesloopt' THEN 'demolition'
			WHEN status IN ('renovation - post2020', 'renovation - pre2020') THEN 'renovation'
			WHEN status IN ('transformation - adding units', 'transformation - function change') THEN 'transformation'
		END AS status, 
		sqm, neighborhood_code, municipality 
	FROM housing_nl
	WHERE municipality = 'Amsterdam'
), 
housing_events AS (
	SELECT 
	    municipality, neighborhood_code, year,
	    SUM(CASE WHEN status = 'construction' THEN sqm ELSE 0 END) AS construction,
	    SUM(CASE WHEN status = 'renovation' THEN sqm ELSE 0 END) AS renovation,
		SUM(CASE WHEN status = 'transformation' THEN sqm ELSE 0 END) AS transformation,
	    SUM(CASE WHEN status = 'demolition' THEN sqm ELSE 0 END) AS demolition
	FROM housing_events_raw
	WHERE year >= 2012
		AND year <= 2021
	GROUP BY municipality, neighborhood_code, year
), 
demolition_previous AS (
	SELECT neighborhood_code, year, demolition AS demolition_previous_year 
	FROM housing_events
), 
housing_stats AS (
	SELECT e.*, 
		e.construction - d.demolition_previous_year AS net_construction_p, 
		e.construction - e.demolition AS net_construction_t
		-- CASE 
		-- 	WHEN e.construction - d.demolition_previous_year >= 0 THEN d.demolition_previous_year
		-- 	ELSE e.construction
		-- END AS replacement_construction
	FROM housing_events e 
	LEFT JOIN demolition_previous d 
	ON e.neighborhood_code = d.neighborhood_code 
		AND e.year = d.year + 1
)
SELECT SUM(net_construction_p), SUM(net_construction_t) FROM housing_stats 
WHERE (net_construction_p IS NOT NULL OR net_construction_t IS NOT NULL) 
	AND (net_construction_p != 0 OR net_construction_t != 0)

-- housing_emissions AS (
-- 	SELECT *,
-- 		(construction_new+construction_replacement)*316 
-- 		+ renovation*126 
-- 		+ transformation*126 
-- 		+ demolition*77 AS emissions_embodied_kg 
-- 	FROM housing_bybuurt_construction 
-- )
-- SELECT * FROM housing_emissions