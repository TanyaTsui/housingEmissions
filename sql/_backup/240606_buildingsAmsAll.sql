DROP TABLE IF EXISTS buildings_ams_all; 
CREATE TABLE buildings_ams_all AS 

WITH units AS (
	SELECT *, 
		ST_X(geom) AS lng, ST_Y(geom) AS lat
	FROM bag_all_ams
	WHERE p_status IN ('Bouw gestart', 'Pand in gebruik', 'Verbouwing pand', 'Pand gesloopt')
)
	
SELECT 
	id_pand, p_status AS status, 
	ANY_VALUE(p_registration_start) AS registration_start,
	ANY_VALUE(p_registration_end) AS registration_end,
	ANY_VALUE(p_build_year) AS build_year,
	ANY_VALUE(lat) AS lat,
	ANY_VALUE(lng) AS lng,
	SUM(sqm) AS sqm
FROM units
GROUP BY (id_pand, p_status)
ORDER BY id_pand; 

SELECT * FROM buildings_ams_all; 