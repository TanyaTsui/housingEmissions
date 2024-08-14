DROP TABLE IF EXISTS demolished_buildings_ams; 
CREATE TABLE demolished_buildings_ams AS 

WITH demolished_units AS (
	SELECT 
		*, 
		EXTRACT(YEAR FROM p_registration_start) AS demolition_year, 
		EXTRACT(YEAR FROM p_registration_start) - p_build_year AS age
	FROM bag_all_ams
	WHERE 
		p_status = 'Pand gesloopt' AND 
		p_build_year < 9999 AND 
		p_build_year > 1800 AND 
		EXTRACT(YEAR FROM p_registration_start) < 2024
), 
demolished_buildings AS (
    SELECT 
        du.id_pand,
        SUM(du.sqm) AS total_sqm,
        (
            SELECT STRING_AGG(function, ', ')
            FROM (
                SELECT DISTINCT function
                FROM demolished_units sub
                WHERE sub.id_pand = du.id_pand
                ORDER BY function
            ) AS subquery
        ) AS function_list,
        ANY_VALUE(du.p_build_year) AS p_build_year,
        ANY_VALUE(du.p_status) AS p_status,
        ANY_VALUE(du.demolition_year) AS demolition_year,
        ANY_VALUE(du.age) AS age
    FROM demolished_units du
    GROUP BY du.id_pand
)

SELECT * 
FROM demolished_buildings; 