INSERT INTO landuse_nl (
    gml_id, description, geom_28992, municipality, province
)

WITH municipality AS (
    SELECT * 
    FROM nl_gemeentegebied
    WHERE naam = %s 
), 
landuse_municipality AS (
    SELECT 
        l.gml_id, l.description AS landuse, 
        l.geometry AS geom_28992, 
        m.naam AS municipality, m.ligt_in_provincie_naam AS province
    FROM landuse_nl_raw l 
    JOIN municipality m 
    ON 
        l.geometry && m.geom 
        AND ST_Intersects(l.geometry, m.geom)
)

SELECT * FROM landuse_municipality