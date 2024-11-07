DELETE FROM housing_nl
WHERE 
	municipality = 'Delft' 
	AND status IN ('Bouw gestart', 'Pand gesloopt');

INSERT INTO housing_nl (
    function, sqm, n_units, id_pand, geometry, build_year, status, 
    document_date, document_number, registration_start, registration_end, 
    geom, geom_28992, neighborhood_code, wk_code, municipality
)
    
-- select relevant units and buildings from BAG
WITH bag_vbo_sample AS (
    SELECT *
    FROM bag_vbo
    WHERE municipality = 'Delft'
), 
units AS (
    SELECT DISTINCT ON (id_vbo) * 
    FROM bag_vbo_sample
    WHERE 
        status = 'Verblijfsobject in gebruik' AND sqm::INTEGER < 9999
), 
bag_pand_sample AS (
    SELECT * 
    FROM bag_pand
    WHERE municipality = 'Delft' 
), 
buildings AS (
    SELECT DISTINCT ON (id_pand, status) * 
    FROM bag_pand_sample
    WHERE status IN ('Bouw gestart', 'Pand gesloopt')
    ORDER BY id_pand, status, registration_start, document_number
), 
housing_stats AS (
    SELECT id_pand, SUM(sqm::INTEGER) AS sqm, COUNT(*) AS n_units, 'woonfunctie' AS function
    FROM units
    WHERE function = 'woonfunctie'
    GROUP BY id_pand
), 
housing_final AS (
    SELECT 
        u.function, u.sqm, u.n_units, 
        b.id_pand, b.geometry, b.build_year, b.status, 
        b.document_date, b.document_number, 
        b.registration_start, b.registration_end, 
        b.geom, b.geom_28992, b.neighborhood_code, b.wk_code, 
        b.municipality
    FROM buildings b 
    JOIN housing_stats u 
    ON b.id_pand = u.id_pand 
)
SELECT * FROM housing_final; 
