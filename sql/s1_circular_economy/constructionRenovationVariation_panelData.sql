WITH housing_nl AS (
	SELECT
		CASE 
	        WHEN status IN ('renovation - pre2020', 'renovation - post2020', 'transformation - adding units', 
							'transformation - function change') 
				THEN 'renovation'
			WHEN status = 'Bouw gestart' THEN 'construction'
	        ELSE status
	    END AS status_n, 
		CASE 
			WHEN registration_end IS NULL THEN 2024::INTEGER
			ELSE LEFT(registration_end, 4)::INTEGER
		END AS year, 
		*
	FROM housing_nl
	WHERE status != 'Pand gesloopt'
		AND LEFT(registration_end, 4)::INTEGER >= 2012
		AND LEFT(registration_end, 4)::INTEGER < 2022
), 
events AS (
	SELECT neighborhood_code, year, status_n
	FROM housing_nl
	WHERE status_n = 'construction'
	GROUP BY neighborhood_code, year, status_n
), 
nyears AS (
	SELECT neighborhood_code, COUNT(*) AS n_years
	FROM events 
	GROUP BY neighborhood_code
	ORDER BY n_years DESC
), 
nneighborhoods AS (
	SELECT n_years, COUNT(*) AS n_neighborhoods
	FROM nyears
	GROUP BY n_years
	ORDER BY n_years
)


SELECT SUM(n_neighborhoods)
FROM nneighborhoods
WHERE n_years > 1



