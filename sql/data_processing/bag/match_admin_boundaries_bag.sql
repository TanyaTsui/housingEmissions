WITH municipality AS (
    SELECT * FROM nl_buurten WHERE municipality_name = %s
)
    
UPDATE bag_pand
SET 
    neighborhood_code = b.neighborhood_code, 
    neighborhood = b.neighborhood, 
    municipality = b.municipality_name,
    province = b.province
FROM municipality b
WHERE 
    bag_pand.geom_28992 && b.neighborhood_geom
    AND ST_Intersects(bag_pand.geom_28992, b.neighborhood_geom);

WITH municipality AS (
    SELECT * FROM nl_buurten WHERE municipality_name = %s
)
    
UPDATE bag_vbo
SET 
    neighborhood_code = b.neighborhood_code, 
    neighborhood = b.neighborhood, 
    municipality = b.municipality_name,
    province = b.province
FROM municipality b
WHERE 
    bag_vbo.geom_28992 && b.neighborhood_geom
    AND ST_Intersects(bag_vbo.geom_28992, b.neighborhood_geom);
