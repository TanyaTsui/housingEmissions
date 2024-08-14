DROP TABLE IF EXISTS bag_pand_subset; 
CREATE TABLE bag_pand_subset AS SELECT * FROM bag_pand LIMIT 5000;
ALTER TABLE bag_pand_subset ADD COLUMN geom geometry(Linestring, 4326);

UPDATE bag_pand_subset
SET geom = ST_GeomFromText(
	'LINESTRING((' || REPLACE(trim(trailing ' 0.0' from geometry), ' 0.0 ', ',') || '))'
)

-- -- add new geometry column for bag_pand_subset
-- ALTER TABLE bag_pand_subset ADD COLUMN geom geometry(Polygon, 4326);
-- UPDATE bag_pand_subset
-- SET geom = ST_Transform(
--              ST_SetSRID(
--                ST_GeomFromText(
--                  'POLYGON((' || REPLACE(trim(trailing ' 0.0' from coordinates), ' 0.0 ', ',') || '))'
--                ), 28992
--              ), 4326
--            );