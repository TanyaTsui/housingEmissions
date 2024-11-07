WITH buurt_to_wijk_key AS (
    SELECT *
	FROM key_buurt2022_to_wijk2012 
	WHERE municipality = 'Delft'
)
    
UPDATE bag_pand
SET 
    wk_code = k.wk_code, 
    municipality = k.municipality
FROM buurt_to_wijk_key k
WHERE 
	bag_pand.neighborhood_code = k.neighborhood_code; 