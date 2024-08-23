DELETE FROM housing_nl
WHERE 
	municipality = %s 
	AND status IN ('Bouw gestart', 'Pand gesloopt');

INSERT INTO housing_nl (
    function, sqm, id_pand, geometry, build_year, status, 
    document_date, document_number, registration_start, registration_end, 
    geom, geom_28992, neighborhood_code, neighborhood, municipality, province
)
    
-- select relevant units and buildings from BAG
WITH bag_vbo_sample AS (
    SELECT *
    FROM bag_vbo
    WHERE municipality = %s
), 
units AS (
    SELECT DISTINCT ON (id_vbo) * 
    FROM bag_vbo_sample
    WHERE 
        status = 'Verblijfsobject in gebruik' AND sqm::INTEGER < 9999
    ORDER BY id_vbo, status, registration_start, document_number
), 
bag_pand_sample AS (
    SELECT * 
    FROM bag_pand
    WHERE municipality = %s 
), 
buildings AS (
    SELECT DISTINCT ON (id_pand, status) * 
    FROM bag_pand_sample
    WHERE status IN ('Bouw gestart', 'Pand gesloopt')
    ORDER BY id_pand, status, registration_start, document_number
), 

-- get buildings with unknown function
buildings_unknown AS (
    SELECT b.*
    FROM buildings b
    WHERE NOT EXISTS (
        SELECT 1
        FROM units u
        WHERE u.id_pand = b.id_pand
    )
    ORDER BY b.id_pand, b.status, b.registration_start, b.document_number
),

-- find units within 50m radius of buildings_unknown
units_near_unknown AS (
    SELECT b.id_pand AS id_pand_b, u.*
    FROM units u
    JOIN buildings_unknown b 
    ON ST_DWithin(u.geom_28992, b.geom_28992, 50) AND u.neighborhood_code = b.neighborhood_code
), 
units_near_unknown_grouped AS (
    SELECT 
        id_pand_b,
        function,
        COUNT(*) AS function_count
    FROM units_near_unknown
    GROUP BY id_pand_b, function
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
housing_guesses AS (
    SELECT DISTINCT ON (id_pand_b)
        id_pand_b AS id_pand, function::TEXT AS function, NULL::BIGINT AS sqm
    FROM units_near_unknown_ranked 
    WHERE rn = 1 AND function = 'woonfunctie'
), 
housing_fromvbo AS (
    SELECT id_pand, SUM(sqm::INTEGER) AS sqm, 'woonfunctie' AS function
    FROM units
    WHERE function = 'woonfunctie'
    GROUP BY id_pand
), 
housing_combined AS (
    SELECT id_pand, function, sqm FROM housing_guesses
    UNION
    SELECT id_pand, function, sqm FROM housing_fromvbo 
), 
housing_final AS (
    SELECT 
        u.function AS function, u.sqm AS sqm, 
        b.id_pand, b.geometry, b.build_year, b.status, 
        b.document_date, b.document_number, 
        b.registration_start, b.registration_end, 
        b.geom, b.geom_28992, b.neighborhood_code, b.neighborhood, 
        b.municipality, b.province
    FROM buildings b 
    JOIN housing_combined u 
    ON b.id_pand = u.id_pand 
)
SELECT * FROM housing_final; 
