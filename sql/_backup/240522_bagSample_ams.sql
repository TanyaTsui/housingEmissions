-- DROP TABLE IF EXISTS temp_bag_pand; 
-- CREATE TEMP TABLE temp_bag_pand AS 
-- 	WITH amsterdam_boundary AS (
-- 	    SELECT ST_GeomFromText('POLYGON((
-- 	        4.892668 52.368216,  
-- 	        4.897668 52.368216,  
-- 	        4.897668 52.373216,  
-- 	        4.892668 52.373216,  
-- 	        4.892668 52.368216    
-- 	    ))', 4326) AS geom
-- 	)
-- 	SELECT b.*, a.geom AS a_geom 
-- 	FROM bag_pand b, amsterdam_boundary a
-- 	WHERE ST_Intersects(b.geom, a.geom);

DROP TABLE IF EXISTS final_result;
CREATE TABLE final_result AS

WITH vbo_ams AS (
	SELECT v.*
	FROM bag_vbo v 
	RIGHT JOIN temp_bag_pand p 
	ON v.id_pand = p.id_pand 
), 
vbo_ams_unique AS (
	SELECT id_pand, ST_Union(geom) as geom
	FROM vbo_ams
	GROUP BY id_pand 
)

SELECT 
	p.id_pand, p.build_year, p.status, p.document_date, p.document_number, 
	p.registration_start, p.registration_end, p.geom, 
	v.id_pand AS v_id_pand
FROM temp_bag_pand p
LEFT JOIN vbo_ams_unique v
ON p.id_pand = v.id_pand; 