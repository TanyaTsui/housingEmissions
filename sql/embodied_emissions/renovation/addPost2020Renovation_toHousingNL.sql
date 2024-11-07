-- Delete rows where municipality = 'Delft' AND status = 'renovation - pre2020'
DELETE FROM housing_nl
WHERE 
	municipality = 'Delft' 
	AND status = 'renovation - post2020';

INSERT INTO housing_nl (
	function, sqm, n_units, id_pand, geometry, build_year, status, 
	document_date, document_number, registration_start, registration_end, 
	geom, geom_28992, neighborhood_code, wk_code, municipality
)

-- Insert rows from the query into housing_nl
WITH building_renovations AS (
	SELECT 
		id_pand, geometry, build_year, 
		CASE 
			WHEN status = 'Verbouwing pand' THEN 'renovation - post2020'
			ELSE status
		END AS status, 
		document_date, document_number, registration_start, registration_end, 
		geom, geom_28992, neighborhood_code, wk_code, municipality
	FROM bag_pand
	WHERE 
		municipality = 'Delft' 
		AND status = 'Verbouwing pand'
		AND LEFT(registration_start, 4)::INTEGER >= 2020
), 
housing_units_inuse AS (
	SELECT * 
	FROM bag_vbo 
	WHERE 
		municipality = 'Delft' 
		AND status = 'Verblijfsobject in gebruik'
		AND function = 'woonfunctie'
		AND sqm::INTEGER < 9999
), 
housing_units_unique AS (
	SELECT DISTINCT ON (id_vbo) * 
	FROM housing_units_inuse
), 
housing_sqm AS (
	SELECT id_pand, SUM(sqm::INTEGER) AS sqm, COUNT(*) AS n_units
	FROM housing_units_unique
	GROUP BY id_pand
), 
housing_sqm_function AS (
	SELECT 'woonfunctie' AS function, *
	FROM housing_sqm
), 
housing_sqm_function_withinfo AS (
	SELECT 
		h.function, h.sqm, h.n_units, 
		r.id_pand, r.geometry, r.build_year, r.status, 
		r.document_date, r.document_number, r.registration_start, r.registration_end, 
		r.geom, r.geom_28992, r.neighborhood_code, r.wk_code, r.municipality
	FROM building_renovations r
	LEFT JOIN housing_sqm_function h
	ON r.id_pand = h.id_pand
)

SELECT * FROM housing_sqm_function_withinfo WHERE n_units IS NOT NULL 
