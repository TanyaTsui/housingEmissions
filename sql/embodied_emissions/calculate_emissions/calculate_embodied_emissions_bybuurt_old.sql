-- DELETE FROM emissions_embodied_bybuurt
-- WHERE municipality = 'Amsterdam';

-- INSERT INTO emissions_embodied_bybuurt(
-- 	municipality, neighborhood_code, year, in_use,
--     construction, renovation, transformation, demolition, emissions_embodied_kg
-- )
	
WITH housing_inuse_raw AS (
	SELECT * FROM housing_inuse_2012_2022
	WHERE municipality = 'Amsterdam'
), 
housing_inuse AS (
	SELECT municipality, neighborhood_code, year, SUM(sqm) AS in_use
	FROM housing_inuse_raw
	GROUP BY municipality, neighborhood_code, year
), 
housing_events_raw AS (
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
housing_bybuurt AS (
	SELECT i.*,
		COALESCE(e.construction, 0) AS construction, 
	    COALESCE(e.renovation, 0) AS renovation, 
		COALESCE(e.transformation, 0) AS transformation, 
	    COALESCE(e.demolition, 0) AS demolition
	FROM housing_inuse i
	FULL JOIN housing_events e
	ON i.neighborhood_code = e.neighborhood_code
		AND i.year = e.year
), 
in_use_next AS (
	SELECT 
        neighborhood_code, 
        year, 
        in_use AS in_use_next_year
    FROM housing_inuse 
), 
housing_bybuurt_construction AS (
	SELECT 
	    h.*,
		CASE 
			WHEN construction > 0 THEN LEAST(p.in_use_next_year - h.in_use, construction) 
			ELSE 0 
		END AS construction_new, 
		CASE 
			WHEN construction > 0 THEN h.construction - LEAST(h.in_use - p.in_use_next_year, construction) 
			ELSE 0 
		END AS construction_replacement
	FROM housing_bybuurt h 
	LEFT JOIN in_use_next p
	    ON h.neighborhood_code = p.neighborhood_code
	    AND h.year = p.year + 1  -- Join with the previous year
), 
housing_emissions AS (
	SELECT *,
		(construction_new+construction_replacement)*316 
		+ renovation*126 
		+ transformation*126 
		+ demolition*77 AS emissions_embodied_kg 
	FROM housing_bybuurt_construction 
)
SELECT * FROM housing_emissions