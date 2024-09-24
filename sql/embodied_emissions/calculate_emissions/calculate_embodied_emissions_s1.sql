INSERT INTO emissions_embodied_housing_nl_s1 (
    year, province, municipality, neighborhood, neighborhood_code, 
    status, emissions_embodied_kg, sqm
)

WITH housing_nl AS (
    SELECT * 
    FROM housing_nl_s1
    WHERE municipality = 'Amsterdam'
), 
emissions AS (
    SELECT 
        CASE 
            WHEN status = 'Bouw gestart' THEN (sqm * 316)::NUMERIC
            WHEN status = 'renovation - post2020' THEN (sqm * 126)::NUMERIC
            WHEN status = 'renovation - pre2020' THEN (sqm * 126)::NUMERIC
            WHEN status = 'transformation - adding units' THEN (sqm * 126)::NUMERIC
            WHEN status = 'transformation - function change' THEN (sqm * 126)::NUMERIC
            WHEN status = 'Pand gesloopt' THEN (sqm * 77)::NUMERIC
            ELSE NULL 
        END AS emissions_embodied_kg, 
        LEFT(registration_start, 4)::INTEGER AS year, 
        * 
    FROM housing_nl 
), 
emissions_grouped AS (
    SELECT 
        year, province, municipality, neighborhood, neighborhood_code, status, 
        ROUND(SUM(emissions_embodied_kg), 3) AS emissions_embodied_kg, 
        SUM(sqm) AS sqm
    FROM emissions
    GROUP BY year, province, municipality, neighborhood, neighborhood_code, status
)

SELECT * 
FROM emissions_grouped