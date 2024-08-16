INSERT INTO housing_nl (
    landuse, function, sqm, id_pand, geometry,
    build_year, status, document_date, document_number,
    registration_start, registration_end, geom, geom_28992,
    neighborhood_code, neighborhood, municipality, province,
    ahn_version
)

WITH housing_test AS (
    SELECT * 
    FROM housing_nl
    WHERE 
        municipality = %s
        AND landuse IS NULL
), 
residential_land AS (
    SELECT * 
    FROM landuse_nl
    WHERE municipality = %s AND description = 'Residential'
)

-- get buildings within residential area from housing_test 
-- insert these buildings back into housing_nl 
SELECT 
    l.description AS landuse, 
    h.function, h.sqm, 
    h.id_pand, h.geometry,
    h.build_year, h.status, h.document_date, h.document_number,
    h.registration_start, h.registration_end, h.geom, h.geom_28992,
    h.neighborhood_code, h.neighborhood, h.municipality, h.province,
    h.ahn_version
FROM housing_test h
JOIN residential_land l
ON 
    h.geom_28992 && l.geom_28992
    AND ST_Within(h.geom_28992, l.geom_28992)
ORDER BY h.sqm DESC;

-- remove all rows from the municipality where landuse IS NULL 
DELETE FROM housing_nl
WHERE landuse IS NULL AND municipality = %s; 
