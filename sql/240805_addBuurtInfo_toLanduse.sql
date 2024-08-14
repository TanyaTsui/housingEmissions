CREATE INDEX IF NOT EXISTS idx_landuse_geometry ON existinglanduseobject USING gist (geometry);
CREATE INDEX IF NOT EXISTS idx_landuse_description ON existinglanduseobject (description);
CREATE INDEX IF NOT EXISTS idx_nl_buurten_municipality_name ON nl_buurten (municipality_name);
CREATE INDEX IF NOT EXISTS idx_nl_buurten_geom ON nl_buurten USING gist (municipality_geom);
CREATE INDEX IF NOT EXISTS idx_nl_gemeentegebied_naam ON nl_gemeentegebied (naam);
CREATE INDEX IF NOT EXISTS idx_nl_gemeentegebied_geom ON nl_gemeentegebied USING gist (geom);

DROP TABLE IF EXISTS landuse_delft; 
CREATE TABLE landuse_delft AS 

WITH municipality AS (
	SELECT * 
	FROM nl_gemeentegebied
	WHERE naam = 'Delft'
), 
landuse_municipality AS (
	SELECT 
    	l.gml_id, l.description AS landuse, l.geom_28992, 
    	m.naam AS municipality, m.ligt_in_provincie_naam AS province
	FROM existinglanduseobject l 
	JOIN municipality m 
	ON 
		l.geom_28992 && m.geom 
		AND ST_Intersects(l.geom_28992, m.geom)
)

SELECT * FROM landuse_municipality