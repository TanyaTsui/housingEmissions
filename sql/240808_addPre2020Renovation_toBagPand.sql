DROP TABLE IF EXISTS bag_pand_delft;
CREATE TABLE bag_pand_delft AS 
	SELECT * FROM bag_pand WHERE municipality = 'Delft';

ALTER TABLE bag_pand_delft DROP COLUMN IF EXISTS renovation;
ALTER TABLE bag_pand_delft ADD COLUMN IF NOT EXISTS renovation BOOLEAN; 

-- insert pre-2020 records with additional column 'renovation'
INSERT INTO bag_pand_delft (
	renovation, id_pand, geometry, build_year, status, document_date, document_number, 
	registration_start, registration_end, geom, geom_28992, 
	neighborhood_code, neighborhood, municipality, province
)

-- get pre-2020 records from Delft 
WITH bag_pand_municipality AS (
	SELECT * FROM bag_pand_delft 
	WHERE municipality = 'Delft' AND LEFT(registration_start, 4)::INTEGER < 2020
), 

-- find out previous status for each record 
ranked_pand_sample AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY id_pand) AS row_count,
           LAG(status) OVER (PARTITION BY id_pand ORDER BY registration_start) AS previous_status
    FROM bag_pand_municipality
	ORDER BY row_count DESC, id_pand, registration_start
)

-- determine whether each record is a renovation or not 
SELECT 
	CASE 
		WHEN status = 'Pand in gebruik (niet ingemeten)' 
			AND previous_status IS NOT NULL 
			AND previous_status = 'Pand in gebruik' 
		THEN TRUE
		ELSE FALSE
	END AS renovation, 
	id_pand, geometry, build_year, status, document_date, document_number, 
	registration_start, registration_end, geom, geom_28992, 
	neighborhood_code, neighborhood, municipality, province
FROM ranked_pand_sample
ORDER BY 
    row_count DESC,
    id_pand,
    registration_start;

-- remove pre-2020 records where renovation was not yet determined
DELETE FROM bag_pand_delft
WHERE municipality = 'Delft' 
	AND LEFT(registration_start, 4)::INTEGER < 2020
	AND renovation IS NULL; 