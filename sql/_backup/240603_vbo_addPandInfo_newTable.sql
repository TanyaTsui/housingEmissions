-- drop old columns
ALTER TABLE bag_vbo
DROP COLUMN p_id_pand, 
DROP COLUMN build_year, 
DROP COLUMN p_status, 
DROP COLUMN p_document_date, 
DROP COLUMN p_registration_start, 
DROP COLUMN p_registration_end;

-- create new table vbo_new 
DROP TABLE IF EXISTS bag_vbo_new; 
CREATE TABLE bag_vbo_new AS 

SELECT 
    vbo.*, 
    pand.id_pand AS p_id_pand, 
	pand.build_year AS p_build_year, 
    pand.status AS p_status, 
	pand.document_date AS p_document_date, 
	pand.registration_start AS p_registration_start, 
	pand.registration_end AS p_registration_end 
FROM 
    bag_vbo vbo
INNER JOIN 
    bag_pand pand
ON 
    vbo.id_pand = pand.id_pand; 