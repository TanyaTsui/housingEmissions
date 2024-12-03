WITH buurt_to_wijk_key AS (
    SELECT *
	FROM key_buurt2022_to_wijk2012 
	WHERE municipality = 'Delft'
)
    
UPDATE housing_inuse_2012_2021
SET 
    wk_code = k.wk_code, 
    municipality = k.municipality
FROM buurt_to_wijk_key k
WHERE 
	housing_inuse_2012_2021.neighborhood_code = k.neighborhood_code 