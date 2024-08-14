-- -- set up new columns in vbo 
-- ALTER TABLE temp_bag_vbo
-- ADD COLUMN p_id_pand CHARACTER VARYING,
-- ADD COLUMN build_year CHARACTER VARYING,
-- ADD COLUMN p_status CHARACTER VARYING;

-- select pand rows with the appropriate status (I guess pand in gebruik?)
WITH pand_filtered AS (
	SELECT id_pand, build_year, status
	FROM temp_bag_pand
)

-- join pand 'build_year' column to vbo on id_pand 
UPDATE temp_bag_vbo v 
SET p_id_pand = p.id_pand,
    build_year = p.build_year,
    p_status = p.status
FROM pand_filtered p
WHERE v.id_pand = p.id_pand;

-- check
WITH gb1 AS (
	SELECT p_id_pand, p_status, COUNT(p_id_pand)
	FROM temp_bag_vbo
	WHERE p_id_pand IS NOT NULL
	GROUP BY p_id_pand, p_status
	ORDER BY count DESC
)

SELECT p_id_pand, COUNT(p_status)
FROM gb1
GROUP BY p_id_pand
ORDER BY count DESC
