DELETE FROM emissions_all_buurt_s2
WHERE municipality = 'Delft';

INSERT INTO emissions_all_buurt_s2 (
	year, municipality, wk_code, bu_code, bu_geom,  
	construction, demolition, transformation, renovation, 
	operational_kg_s0, operational_kg_s1, operational_kg_s2, 
	embodied_kg_s0, embodied_kg_s1, embodied_kg_s2
)

WITH housing_nl_s2 AS (
    SELECT 
		CASE 
			WHEN registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
			ELSE LEFT(registration_start, 4)::INTEGER
		END AS year, 
        CASE
            WHEN status = 'Pand gesloopt' THEN 'demolition'
            WHEN status = 'Bouw gestart' THEN 'construction'
            WHEN status IN ('renovation - pre2020', 'renovation - post2020', 'renovation - s1') THEN 'renovation'
            WHEN status IN ('transformation - function change', 'transformation - adding units') THEN 'transformation'
            ELSE status
        END AS status, 
        sqm, id_pand, bu_code, wk_code, municipality
    FROM housing_nl_s2
    WHERE LEFT(registration_start, 4)::INTEGER BETWEEN 2012 AND 2021
		AND municipality = 'Delft'
), 
sqm AS (
	SELECT 
	    year, municipality, wk_code, bu_code,
	    SUM(CASE WHEN status = 'construction' THEN sqm ELSE 0 END) AS construction,
	    SUM(CASE WHEN status = 'demolition' THEN sqm ELSE 0 END) AS demolition,
	    SUM(CASE WHEN status = 'transformation' THEN sqm ELSE 0 END) AS transformation,
	    SUM(CASE WHEN status = 'renovation' THEN sqm ELSE 0 END) AS renovation
	FROM housing_nl_s2
	WHERE year BETWEEN 2012 AND 2021
	GROUP BY year, municipality, wk_code, bu_code 
), 
embodied_emissions AS (
	SELECT *, 
		construction*325 + renovation*126 + transformation*126 + demolition*77 AS embodied_kg_s2
	FROM sqm
), 
buurt_stats AS (
	SELECT 
		year, municipality, wk_code, bu_code, bu_geom, 
		n_homes, inuse, population, woz, 
		operational_kg_s0, operational_kg_s1, embodied_kg_s0, embodied_kg_s1
	FROM emissions_all_buurt_s1 
	WHERE municipality = 'Delft'
), 
final_stats AS (
	SELECT 
	    COALESCE(a.year, b.year) AS year,
	    COALESCE(a.municipality, b.municipality) AS municipality,
	    COALESCE(a.wk_code, b.wk_code) AS wk_code,
	    COALESCE(a.bu_code, b.bu_code) AS bu_code, b.bu_geom, 
		
	    COALESCE(a.construction, 0) AS construction,
	    COALESCE(a.demolition, 0) AS demolition,
	    COALESCE(a.transformation, 0) AS transformation,
	    COALESCE(a.renovation, 0) AS renovation, 
		
		b.operational_kg_s0, b.operational_kg_s1, b.operational_kg_s0 AS operational_kg_s2, 
		
	    COALESCE(b.embodied_kg_s0, 0) AS embodied_kg_s0,
	    b.embodied_kg_s1 AS embodied_kg_s1,
	    COALESCE(a.embodied_kg_s2, 0) AS embodied_kg_s2
		
	FROM embodied_emissions a 
	FULL JOIN buurt_stats b 
	ON a.year = b.year 
		AND a.municipality = b.municipality
		AND a.bu_code = b.bu_code 
)

SELECT * -- bu_code, bu_geom, COUNT(*)
FROM final_stats 
-- WHERE bu_geom IS NULL 
-- GROUP BY bu_code, bu_geom
-- ORDER BY COUNT(*)


