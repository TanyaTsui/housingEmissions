DROP TABLE IF EXISTS ahn_elevation_delft; 
CREATE TABLE ahn_elevation_delft AS

WITH delft AS (
    SELECT * FROM nl_gemeentegebied WHERE naam = 'Delft'
)

SELECT 
    d.naam AS municipality, d.ligt_in_provincie_naam AS province, 
    a.*
FROM ahn_elevation a 
JOIN delft d 
ON ST_Intersects(a.rast_geom, d.geom); 