CREATE TABLE bag_pand_delft AS 
WITH bbox AS (
    SELECT ST_MakeEnvelope(4.346521,52.005251,4.369094,52.019251, 4326) AS geom
)
SELECT bag_pand.*
FROM bag_pand, bbox
WHERE ST_Within(bag_pand.geom, bbox.geom)