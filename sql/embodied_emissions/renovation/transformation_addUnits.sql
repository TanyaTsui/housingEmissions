/* 
ADD TRANSFORMATIONS (ADDING UNITS) TO HOUSING_RENOVATION_NL
This query idetified transformations where extra housing unit(s) are added to a building. 
The extra addition either increases the total sqm of the building footprint, or it doesn't. 
In both scenarios it is counted as a transformation. 

These are the steps: 
- find rows in bag_vbo where housing units were formed after 2010
- for these rows, get build_year of the building, but joining table bag_pand on id_pand 
- select rows where registration_year > build_year 
*/ 

-- Delete existing rows where municipality is 'Delft'
DELETE FROM housing_nl
WHERE 
	status = 'transformation - adding units'
	AND municipality = 'Delft'; 

INSERT INTO housing_nl (
    function, sqm, n_units, id_pand, build_year, status, 
	document_date, document_number, registration_start, registration_end, 
	pand_geom, bu_code, wk_code, municipality
)

-- Insert the result of the query into the table
WITH bag_vbo_sample AS (
    SELECT * 
    FROM 
        bag_vbo
    WHERE 
        municipality = 'Delft'
        AND status = 'Verblijfsobject gevormd'
        AND function = 'woonfunctie'
		AND sqm::INTEGER < 9999
), 
bag_pand_sample AS (
    SELECT * 
    FROM bag_pand
    WHERE 
        municipality = 'Delft'
        AND build_year::INTEGER < 9999
), 
bag_pand_build_year AS (
    SELECT id_pand, MIN(build_year) AS build_year
    FROM bag_pand_sample
    GROUP BY id_pand
), 
units_build_year AS (
    SELECT p.build_year, v.*
    FROM bag_vbo_sample v
    LEFT JOIN bag_pand_build_year p 
    ON v.id_pand = p.id_pand
), 
units_added AS (
	SELECT 
		id_vbo, id_num, id_pand, function, sqm, status, 
		document_date, document_number, registration_start, registration_end, 
		vbo_geom, bu_code, wk_code, municipality, 
		'transformation - adding units' AS renovation
	FROM units_build_year
	WHERE build_year::INTEGER + 5 < LEFT(registration_start, 4)::INTEGER
), 
units_added_aggregated AS (
    SELECT 
        id_pand, LEFT(registration_start, 4) AS registration_start,
        SUM(sqm::INTEGER) AS sqm, COUNT(*) AS n_units 
    FROM units_added
    GROUP BY id_pand, LEFT(registration_start, 4)
), 
buildings_municipality AS (
    SELECT * 
    FROM bag_pand
    WHERE 
        municipality = 'Delft'
),
units_added_withinfo AS (
    SELECT 
        'woonfunctie' AS function, u.sqm, u.n_units, u.id_pand, 
        m.build_year, 
        'transformation - adding units' AS status, 
        m.document_date, m.document_number, 
        u.registration_start, NULL as registration_end, 
        m.pand_geom, m.bu_code, m.wk_code, m.municipality
    FROM units_added_aggregated u 
    LEFT JOIN LATERAL (
        SELECT * 
        FROM buildings_municipality m 
        WHERE 
            u.id_pand = m.id_pand 
            AND u.registration_start > LEFT(m.registration_start, 4)
		LIMIT 1
    ) m ON TRUE
), 
units_added_withinfo_filtered AS (
    SELECT * 
    FROM units_added_withinfo
    WHERE municipality IS NOT NULL
)

SELECT *
FROM units_added_withinfo_filtered WHERE n_units IS NOT NULL 