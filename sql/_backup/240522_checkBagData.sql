-- DROP TABLE IF EXISTS temp_bag_vbo; 
-- CREATE TEMP TABLE temp_bag_vbo AS 
-- 	SELECT * FROM bag_vbo TABLESAMPLE BERNOULLI(0.5);
-- DROP TABLE IF EXISTS temp_bag_pand; 
-- CREATE TEMP TABLE temp_bag_pand AS 
-- 	SELECT * FROM bag_pand TABLESAMPLE BERNOULLI(0.5);


WITH ranked_buildings AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id_pand ORDER BY CAST(document_date AS DATE) DESC) AS rn
    FROM 
        temp_bag_pand
), 
unique_buildings AS (
	SELECT * FROM ranked_buildings WHERE rn = 1
), 
ranked_units AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id_pand ORDER BY CAST(document_date AS DATE) DESC) AS rn
    FROM 
        temp_bag_vbo
), 
unique_units AS (
	SELECT * FROM ranked_units WHERE rn = 1
)

SELECT *
FROM unique_buildings b 
LEFT JOIN unique_units u
ON b.id_pand = u.id_pand 
WHERE u.id_pand IS NULL 