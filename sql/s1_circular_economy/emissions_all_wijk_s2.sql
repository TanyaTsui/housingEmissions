DELETE FROM emissions_all_wijk_s2
WHERE municipality = 'Delft';

INSERT INTO emissions_all_wijk_s2 (
	year, municipality, wk_code, wk_geom, 
	construction, demolition, transformation, renovation, 
	embodied_kg_s2, operational_kg_s2
)

WITH housing_nl_s2 AS (
    SELECT 
		CASE 
			WHEN status = 'Pand gesloopt' THEN LEFT(registration_start, 4)::INTEGER
			WHEN status != 'Pand gesloopt' AND registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
			ELSE LEFT(registration_start, 4)::INTEGER
		END AS year, 
        CASE
            WHEN status = 'Pand gesloopt' THEN 'demolition'
            WHEN status = 'Bouw gestart' THEN 'construction'
            WHEN status IN ('renovation - pre2020', 'renovation - post2020', 'renovation - s1') THEN 'renovation'
            WHEN status IN ('transformation - function change', 'transformation - adding units') THEN 'transformation'
            ELSE status
        END AS status, 
        sqm, wk_code, wk_geom, municipality
    FROM housing_nl_s2
    WHERE LEFT(registration_start, 4)::INTEGER BETWEEN 2012 AND 2022 
		AND municipality = 'Delft'
), 
sqm AS (
	SELECT 
	    year, municipality, wk_code, wk_geom,
	    SUM(CASE WHEN status = 'construction' THEN sqm ELSE 0 END) AS construction,
	    SUM(CASE WHEN status = 'demolition' THEN sqm ELSE 0 END) AS demolition,
	    SUM(CASE WHEN status = 'transformation' THEN sqm ELSE 0 END) AS transformation,
	    SUM(CASE WHEN status = 'renovation' THEN sqm ELSE 0 END) AS renovation
	FROM housing_nl_s2
	GROUP BY year, municipality, wk_code, wk_geom
), 
embodied_emissions AS (
	SELECT *, 
		construction*325 + renovation*126 + transformation*126 + demolition*77 AS embodied_kg_s2
	FROM sqm
), 

wijk_stats AS (
	SELECT 
		year, municipality, wk_code, n_units, operational_kg_s0, inuse, population, av_woz
	FROM emissions_all_wijk_s1 
	WHERE municipality = 'Delft'
)

SELECT 
	a.*, b.operational_kg_s0 AS operational_kg_s2
FROM embodied_emissions a 
LEFT JOIN wijk_stats b 
ON a.year = b.year 
	AND a.municipality = b.municipality
	AND a.wk_code = b.wk_code 
    	