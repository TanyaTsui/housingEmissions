CREATE TABLE housing_delft AS 

-- get relevant units and buildings  
WITH units AS (
	SELECT DISTINCT ON (id_vbo) * 
	FROM bag_vbo_delft
	WHERE status = 'Verblijfsobject in gebruik' AND sqm::INTEGER < 9999
), 
buildings AS (
	SELECT DISTINCT ON (id_pand) * 
	FROM bag_pand_delft
	WHERE status IN ('Bouw gestart', 'Verbouwing pand', 'Pand gesloopt')
), 

-- get pand ids for housing and non housing  
id_pand_housing AS (
	SELECT DISTINCT id_pand
	FROM units
	WHERE function = 'woonfunctie'
), 
id_pand_nonhousing AS (
	SELECT DISTINCT id_pand
	FROM units
	WHERE 
		function != 'woonfunctie' AND 
		id_pand NOT IN (SELECT id_pand FROM id_pand_housing)
), 

-- identify function for buildings (woonfunctie, non_woonfunctie, unknown)
buildings_withfunction AS (
	SELECT 
	    CASE 
	        WHEN h.id_pand IS NOT NULL THEN 'woonfunctie'
	        WHEN nh.id_pand IS NOT NULL THEN 'non_woonfunctie'
	        ELSE 'unknown'
	    END AS function, 
	    b.*
	FROM 
	    buildings b
	LEFT JOIN 
	    id_pand_housing h ON b.id_pand = h.id_pand
	LEFT JOIN 
	    id_pand_nonhousing nh ON b.id_pand = nh.id_pand
), 	

-- find buildings with no sqm or function data 
buildings_unknown AS (
	SELECT * 
	FROM buildings_withfunction b
	WHERE function = 'unknown'
), 

-- find units within 50m radius of buildings_unknown
units_near_unknown AS (
	SELECT b.id_pand AS id_pand_b, u.* 
	FROM units u
	JOIN buildings_unknown b 
	ON ST_DWithin(u.geom_28992, b.geom_28992, 50)
), 

units_near_unknown_grouped AS (
	SELECT 
	    id_pand_b,
	    function,
	    COUNT(*) AS function_count
	FROM units_near_unknown
	GROUP BY id_pand_b, function
	ORDER BY id_pand_b, function DESC 
), 

units_near_unknown_ranked AS (
    SELECT 
        id_pand_b,
        function,
        function_count,
        ROW_NUMBER() OVER (PARTITION BY id_pand_b ORDER BY function_count DESC) AS rn
    FROM 
        units_near_unknown_grouped
), 

-- estimate unknown functions
function_guesses AS (
	SELECT id_pand_b AS id_pand, function
	FROM units_near_unknown_ranked 
	WHERE rn = 1 
), 

buildings_final AS (
	SELECT 
	    COALESCE(g.function, b.function) AS function, 
		b.id_pand, b.build_year, b.status, b.registration_start, b.registration_end, 
		b.geom, b.geom_28992
	FROM 
	    buildings_withfunction b
	LEFT JOIN 
	    function_guesses g ON b.id_pand = g.id_pand
), 
housing AS (
	SELECT * 
	FROM buildings_final 
	WHERE function = 'woonfunctie'
), 

-- estimate housing sqm
housing_sqm AS (
	SELECT id_pand, SUM(sqm::INTEGER) AS sqm
	FROM units
	WHERE function = 'woonfunctie'
	GROUP BY id_pand
), 

-- add sqm column to housing table 
housing_final AS (
	SELECT s.sqm, h.*
	FROM housing h 
	LEFT JOIN housing_sqm s 
	ON h.id_pand = s.id_pand
)

SELECT * FROM housing_final 