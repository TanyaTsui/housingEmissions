ALTER TABLE housing_delft ADD COLUMN IF NOT EXISTS landuse VARCHAR; 

-- assign the landuse 'Residential' to all buildings where function was not guessed
UPDATE housing_delft
SET landuse = 'Residential'
WHERE ahn_version IS NULL;

-- take buildings where function was guessed - housing_test
-- filter buildings in housing_test that are in residential areas 
-- insert these filtered buildings back into housing_nl 
INSERT INTO housing_delft (
    landuse, function, sqm, id_pand, geometry,
    build_year, status, document_date, document_number,
    registration_start, registration_end, geom, geom_28992,
    neighborhood_code, neighborhood, municipality, province,
    ahn_version
)

WITH housing_test AS (
	SELECT * 
	FROM housing_delft
	WHERE 
		municipality = 'Delft'
		AND landuse IS NULL
), 
residential_land AS (
	SELECT * 
	FROM landuse_nl
	WHERE municipality = 'Delft' AND description = 'Residential'
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
DELETE FROM housing_delft
WHERE landuse IS NULL AND municipality = 'Delft'; 