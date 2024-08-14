DROP TABLE IF EXISTS bag_pand_delft; 
CREATE TABLE bag_pand_delft AS (
	SELECT * 
	FROM bag_pand
	WHERE municipality = 'Delft'
); 

ALTER TABLE bag_pand DROP COLUMN IF EXISTS renovation_pandingebruiknietingemeten;
ALTER TABLE bag_pand DROP COLUMN IF EXISTS renovation;
ALTER TABLE bag_pand ADD COLUMN IF NOT EXISTS renovation VARCHAR;

INSERT INTO bag_pand (
	renovation, id_pand, geometry, build_year, status, document_date, document_number, 
	registration_start, registration_end, geom, geom_28992, 
	neighborhood_code, neighborhood, municipality, province
)

WITH bag_pand_municipality AS (
	SELECT * FROM bag_pand
	WHERE 
		municipality = 'Delft' 
		AND LEFT(registration_start, 4)::INTEGER < 2020
), 
ranked_pand_sample AS (
    SELECT *,
           LAG(status) OVER (PARTITION BY id_pand ORDER BY registration_start) AS previous_status
    FROM bag_pand_municipality
)
SELECT 
	CASE 
		WHEN 
        	status = 'Pand in gebruik (niet ingemeten)' 
    		AND previous_status IS NOT NULL 
            AND previous_status = 'Pand in gebruik'  
        THEN 'Verbouwing pand'
		ELSE 'Not verbouwing pand'
	END AS renovation, 
	id_pand, geometry, build_year, status, document_date, document_number, 
	registration_start, registration_end, geom, geom_28992, 
	neighborhood_code, neighborhood, municipality, province
FROM ranked_pand_sample; 

DELETE FROM bag_pand
WHERE 
	municipality = 'Delft'
	AND LEFT(registration_start, 4)::INTEGER < 2020
	AND renovation IS NULL; 

UPDATE bag_pand
SET renovation = 'Verbouwing pand'
WHERE status = 'Verbouwing pand' AND municipality = 'Delft';