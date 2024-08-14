-- drop columns if exists
ALTER TABLE bag_vbo
DROP COLUMN IF EXISTS p_document_date,
DROP COLUMN IF EXISTS p_registration_start,
DROP COLUMN IF EXISTS p_registration_end,
DROP COLUMN IF EXISTS p_id_pand,
DROP COLUMN IF EXISTS build_year,
DROP COLUMN IF EXISTS p_status;

-- set up new columns in vbo 
ALTER TABLE bag_vbo
ADD COLUMN p_document_date CHARACTER VARYING, 
ADD COLUMN p_registration_start CHARACTER VARYING, 
ADD COLUMN p_registration_end CHARACTER VARYING, 
ADD COLUMN p_id_pand CHARACTER VARYING,
ADD COLUMN build_year CHARACTER VARYING,
ADD COLUMN p_status CHARACTER VARYING;

-- select pand rows with the appropriate status (I guess pand in gebruik?)
WITH pand_filtered AS (
	SELECT 
		id_pand, build_year, status, 
		document_date, registration_start, registration_end
	FROM bag_pand
)

-- join pand 'build_year' column to vbo on id_pand 
UPDATE bag_vbo v 
SET p_id_pand = p.id_pand,
    build_year = p.build_year,
    p_status = p.status, 
	p_document_date = p.document_date, 
	p_registration_start = p.registration_start, 
	p_registration_end = p.registration_end
FROM pand_filtered p
WHERE v.id_pand = p.id_pand;

-- -- check output
-- SELECT * 
-- FROM temp_bag_vbo
-- WHERE p_id_pand IS NOT NULL