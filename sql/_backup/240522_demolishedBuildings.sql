-- DROP TABLE IF EXISTS temp_bag_vbo; 
-- CREATE TEMP TABLE temp_bag_vbo AS 
-- 	SELECT * FROM bag_vbo TABLESAMPLE BERNOULLI(0.5);
-- DROP TABLE IF EXISTS temp_bag_pand; 
-- CREATE TEMP TABLE temp_bag_pand AS 
-- 	SELECT * FROM bag_pand TABLESAMPLE BERNOULLI(0.5);

DROP TABLE IF EXISTS demolished_buildings; 
CREATE TABLE demolished_buildings AS 

-- get unique units + aggregate sqm 
WITH valid_vbo AS (
	SELECT * 
	FROM bag_vbo
	WHERE 
		status NOT IN (
			'Niet gerealiseerd verblijfsobject', 
			'Verblijfsobject ingetrokken', 
			'Verblijfsobject ten onrechte opgevoerd') AND 
		CAST(sqm AS INT) < 999999
), 
ranked_vbo AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
			PARTITION BY id_vbo 
			ORDER BY CAST(document_date AS DATE) DESC) AS rn
    FROM valid_vbo
),
filtered_vbo AS (
    SELECT *
    FROM ranked_vbo
    WHERE rn = 1
), 
buildings_sqm AS (
	SELECT id_pand, SUM(CAST(sqm AS INTEGER)) AS sqm, ST_Union(geom) AS geom
	FROM filtered_vbo  
	GROUP BY id_pand
), 

-- get demolished buildings 
demolished_buildings_raw AS (
	SELECT * 
	FROM bag_pand  
	WHERE 
		status = 'Pand gesloopt' 
), 

demolished_buildings AS (
	SELECT a.*, b.sqm, b.geom
	FROM demolished_buildings_raw a
	LEFT JOIN buildings_sqm b
	ON a.id_pand = b.id_pand 
)

SELECT * FROM demolished_buildings; 

















-- demolished_units AS (
--     SELECT 
--         u.id_vbo AS u_id_vbo, 
--         u.id_pand AS u_id_pand, 
--         u.geometry AS u_geometry, 
-- 		u.geom AS u_geom, 
--         u.function AS u_function, 
--         u.sqm AS u_sqm, 
--         -- u.status AS u_status, 
--         u.document_date AS u_document_date, 
--         -- u.document_number AS u_document_number, 
--         -- u.registration_start AS u_registration_start, 
--         -- u.registration_end AS u_registration_end,
--         b.id_pand AS b_id_pand, 
--         -- b.geometry AS b_geometry, 
--         b.build_year AS b_build_year, 
--         b.status AS b_status, 
--         b.document_date AS b_document_date, 
--         b.document_number AS b_document_number, 
--         b.registration_start AS b_registration_start, 
--         b.registration_end AS b_registration_end
--     FROM units u
--     JOIN demolished_buildings_raw b ON u.id_pand = b.id_pand
-- ), 

-- -- get unique demolished units  
-- ranked_demolished_units AS (
--     SELECT
--         *,
--         ROW_NUMBER() OVER (PARTITION BY u_id_vbo ORDER BY CAST(u_document_date AS DATE) DESC) AS rnk
--     FROM demolished_units
-- ),
-- unique_demolished_units AS (
--     SELECT *
--     FROM ranked_demolished_units
--     WHERE rnk = 1
-- )