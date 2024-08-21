WITH municipality AS (
    SELECT * FROM nl_buurten WHERE municipality = %s
)
    
UPDATE bag_pand
SET 
    neighborhood_code = m.neighborhood_code, 
    neighborhood = m.neighborhood, 
    municipality = m.municipality
FROM municipality m
WHERE 
    bag_pand.geom_28992 && m.neighborhood_geom
    AND ST_Intersects(bag_pand.geom_28992, m.neighborhood_geom);

WITH municipality AS (
    SELECT * FROM nl_buurten WHERE municipality = %s
)
    
UPDATE bag_vbo
SET 
    neighborhood_code = m.neighborhood_code, 
    neighborhood = m.neighborhood, 
    municipality = m.municipality
FROM municipality m
WHERE 
    bag_vbo.geom_28992 && m.neighborhood_geom
    AND ST_Intersects(bag_vbo.geom_28992, m.neighborhood_geom);