-- DROP TABLE IF EXISTS temp_bag_vbo; 
-- CREATE TEMP TABLE temp_bag_vbo AS 
-- 	SELECT * FROM bag_vbo TABLESAMPLE BERNOULLI(0.5);
-- DROP TABLE IF EXISTS temp_bag_pand; 
-- CREATE TEMP TABLE temp_bag_pand AS 
-- 	SELECT * FROM bag_pand TABLESAMPLE BERNOULLI(2);

DROP TABLE IF EXISTS renovated_units; 
CREATE TABLE renovated_units AS 
WITH renovated_units AS (
	SELECT 
		u.*, b.id_pand AS b_id_pand, 
		b.build_year AS b_build_year, b.status AS b_status, b.document_date AS b_document_date, 
		b.document_number AS b_document_number, b.registration_start AS b_registration_start, 
		b.registration_end AS b_registration_end 
	FROM bag_vbo u
	LEFT JOIN bag_pand b ON u.id_pand = b.id_pand
	WHERE 
		b.status NOT IN ('Bouw gestart', 'Bouwvergunning verleend') AND 
		u.status = 'Verbouwing verblijfsobject'
)

SELECT * FROM renovated_units; 