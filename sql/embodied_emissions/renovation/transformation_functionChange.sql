/* 
ADD TRANSFORMATIONS FROM FUNCTION CHANGE TO HOUSING_RENOVATION_NL
Identifies transformations where units were changed from non-housing to housing function.  

These are the steps: 
- get rows in bag_vbo where status = 'Verblijfsobject in gebruik'
- order these rows by registration_start 
- get rows where the current function = 'woonfunctie', but previous function isn't 
*/

-- Delete existing rows where municipality is municipality name
DELETE FROM housing_nl
WHERE 
	status = 'transformation - function change'
	AND municipality = 'Delft'; 

-- Insert the result of the query into the table
WITH bag_vbo_sample AS (
	SELECT * 
	FROM bag_vbo
	WHERE 
		municipality = 'Delft'
		AND status = 'Verblijfsobject in gebruik'
), 
vbo_ordered AS (
    SELECT 
        *,
        LAG(function) OVER (PARTITION BY id_vbo ORDER BY registration_start) AS previous_function
    FROM bag_vbo_sample
), 
transformed_units AS (
	SELECT 
		id_vbo, id_num, id_pand, geometry, function, sqm, status, 
		document_date, document_number, registration_start, registration_end, 
		geom, geom_28992, neighborhood_code, neighborhood, municipality, province
	FROM vbo_ordered
	WHERE 
	    function = 'woonfunctie'
		AND previous_function IS NOT NULL
		AND previous_function != 'woonfunctie'
), 
buildings_transformed AS (
	SELECT 
		id_pand, LEFT(registration_start, 4) AS registration_year, 
		SUM(sqm::INTEGER) AS sqm 
	FROM transformed_units
	GROUP BY id_pand, LEFT(registration_start, 4)
), 
buildings_municipality AS (
	SELECT * 
	FROM bag_pand
	WHERE 
		municipality = 'Delft'
), 
buildings_transformed_withinfo AS (
	SELECT 
		'woonfunctie' AS function, t.sqm, t.id_pand, 
		m.geometry, m.build_year, 
		'transformation - function change' AS status, 
		m.document_date, m.document_number, 
		m.registration_start, m.registration_end, 
		m.geom, m.geom_28992, m.neighborhood_code, m.neighborhood, m.municipality, m.province
	FROM buildings_transformed t 
	LEFT JOIN LATERAL (
	    SELECT * 
	    FROM buildings_municipality m 
	    WHERE 
			t.id_pand = m.id_pand 
			AND t.registration_year > LEFT(m.registration_start, 4)
		ORDER BY LEFT(m.registration_start, 4) DESC
	    LIMIT 1
	) m ON true
),
buildings_transformed_withinfo_fitered AS (
	SELECT * 
	FROM buildings_transformed_withinfo
	WHERE municipality IS NOT NULL
)

INSERT INTO housing_nl (
	function, sqm, id_pand, geometry, build_year, status, 
	document_date, document_number, registration_start, registration_end, 
	geom, geom_28992, neighborhood_code, neighborhood, municipality, province
)
SELECT * 
FROM buildings_transformed_withinfo_fitered