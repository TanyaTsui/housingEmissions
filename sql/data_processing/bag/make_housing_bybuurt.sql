DELETE FROM housing_bybuurt
WHERE municipality = 'Amsterdam';

INSERT INTO housing_bybuurt(
	municipality, neighborhood_code, year, in_use,
    construction, renovation, demolition
)
	
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
			WHEN status IN ('renovation - post2020', 'renovation - pre2020', 
							'transformation - adding units', 'transformation - function change')
				THEN 'renovation'
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
	    SUM(CASE WHEN status = 'demolition' THEN sqm ELSE 0 END) AS demolition
	FROM housing_events_raw
	WHERE year >= 2012
		AND year <= 2021
	GROUP BY municipality, neighborhood_code, year
)

SELECT i.*, 
	COALESCE(e.construction, 0) AS construction, 
    COALESCE(e.renovation, 0) AS renovation, 
    COALESCE(e.demolition, 0) AS demolition
FROM housing_inuse i
FULL JOIN housing_events e
ON i.neighborhood_code = e.neighborhood_code
	AND i.year = e.year

