DROP TABLE IF EXISTS nl_buurten; 

CREATE TABLE nl_buurten AS 

WITH neighborhoods AS (
	SELECT neighborhood_code, neighborhood, municipality_code, municipality_name, neighborhood_geom
	FROM nl_buurten_cbs
	WHERE aantal_huishoudens > -999
)

SELECT 
	n.*, 
	g.geom AS municipality_geom, 
	g.ligt_in_provincie_code AS province_code, 
	g.ligt_in_provincie_naam AS province
FROM neighborhoods n 
LEFT JOIN nl_gemeentegebied g 
ON n.municipality_code = g.identificatie;