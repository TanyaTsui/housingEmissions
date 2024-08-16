ALTER TABLE ahn_elevation ADD COLUMN IF NOT EXISTS municipality VARCHAR;
ALTER TABLE ahn_elevation ADD COLUMN IF NOT EXISTS province VARCHAR;

WITH municipality AS (
    SELECT * FROM nl_gemeentegebied WHERE naam = %s 
)
    
UPDATE ahn_elevation 
SET 
    municipality = m.naam, 
    province = m.ligt_in_provincie_naam
FROM municipality m 
WHERE ST_Intersects(ahn_elevation.rast_geom, m.geom); 