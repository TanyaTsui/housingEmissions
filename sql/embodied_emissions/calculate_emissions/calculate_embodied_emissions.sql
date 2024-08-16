INSERT INTO emissions_embodied_housing_nl (
    year, province, municipality, neighborhood, neighborhood_code, 
    status, emissions_embodied_tons, sqm
)

WITH housing_nl AS (
    SELECT * 
    FROM housing_nl
    WHERE municipality = %s
), 
emissions AS (
    SELECT 
        CASE 
            WHEN status = 'Bouw gestart' THEN (sqm * 316 / 1000.0)::NUMERIC
            WHEN status = 'Verbouwing pand' THEN (sqm * 126 / 1000.0)::NUMERIC
            WHEN status = 'Pand gesloopt' THEN (sqm * 77 / 1000.0)::NUMERIC
            ELSE NULL 
        END AS emissions_embodied_tons, 
        LEFT(registration_start, 4)::INTEGER AS year, 
        * 
    FROM housing_nl 
), 
emissions_grouped AS (
    SELECT 
        year, province, municipality, neighborhood, neighborhood_code, status, 
        ROUND(SUM(emissions_embodied_tons), 3) AS emissions_embodied_tons, 
        SUM(sqm) AS sqm
    FROM emissions
    GROUP BY year, province, municipality, neighborhood, neighborhood_code, status
)

SELECT * 
FROM emissions_grouped
