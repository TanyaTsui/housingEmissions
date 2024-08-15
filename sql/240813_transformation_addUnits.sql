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

-- Step 2: Delete existing rows where municipality is 'Delft'
DELETE FROM housing_nl
WHERE 
	status = 'transformation - adding units'
	AND municipality = 'Delft'; 

-- Step 3: Insert the result of the query into the table
INSERT INTO housing_nl
WITH bag_vbo_sample AS (
    SELECT * 
    FROM 
        bag_vbo
    WHERE 
        municipality = 'Delft'
        AND status = 'Verblijfsobject gevormd'
        AND LEFT(registration_start, 4)::INTEGER > 2010
        AND function = 'woonfunctie'
), 
bag_pand_sample AS (
    SELECT * 
    FROM bag_pand
    WHERE 
        municipality = 'Delft'
        AND status = 'Pand in gebruik'
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
)
SELECT 
	id_vbo, id_num, id_pand, geometry, function, sqm, status, 
	document_date, document_number, registration_start, registration_end, 
	geom, geom_28992, neighborhood_code, neighborhood, municipality, province, 
	'transformation - adding units' AS renovation
FROM units_build_year
WHERE build_year < LEFT(registration_start, 4);
