/*
OPTIMIZING THE BAG DATASET (PAND AND VBO)
- create indexes to id_pand (and potentially other columns)
- change column formats from strings to dates 
- rename columns 
*/ 

-- ROLLBACK; 
BEGIN; 

-- make test samples 
DROP TABLE IF EXISTS bag_pand; 
CREATE TABLE bag_pand AS SELECT * FROM bag_pand LIMIT 5000;
DROP TABLE IF EXISTS bag_vbo; 
CREATE TABLE bag_vbo AS SELECT * FROM bag_vbo LIMIT 5000;

-- alter bag_pand_subset
ALTER TABLE bag_pand
  ALTER COLUMN registration_start TYPE date USING registration_start::date,
  ALTER COLUMN registration_end TYPE date USING registration_end::date,
  ALTER COLUMN document_date TYPE date USING document_date::date, 
  ALTER COLUMN build_year TYPE date USING TO_DATE(build_year, 'YYYY');
ALTER TABLE bag_pand
  RENAME COLUMN geometry TO coordinates;
  
-- add new geometry column for bag_pand_subset
ALTER TABLE bag_pand ADD COLUMN geom geometry(Polygon, 4326);
UPDATE bag_pand
SET geom = ST_Transform(
             ST_SetSRID(
               ST_GeomFromText(
                 'POLYGON((' || RTRIM(REPLACE(coordinates || ' ', ' 0.0 ', ','), ',') || '))'
               ), 28992
             ), 4326
           );
		  
-- alter bag_vbo_subset 
ALTER TABLE bag_vbo
  ALTER COLUMN registration_start TYPE date USING registration_start::date,
  ALTER COLUMN registration_end TYPE date USING registration_end::date,
  ALTER COLUMN document_date TYPE date USING document_date::date, 
  ALTER COLUMN sqm TYPE integer USING sqm::integer; 
ALTER TABLE bag_vbo
  RENAME COLUMN geometry TO coordinates; 

-- add new geometry column for bag_vbo_subset
ALTER TABLE bag_vbo ADD COLUMN geom geometry(Point, 4326);
UPDATE bag_vbo
SET geom = ST_Transform(
             ST_SetSRID(
               ST_MakePoint(
                 (string_to_array(coordinates, ' '))[1]::float,
                 (string_to_array(coordinates, ' '))[2]::float
               ), 28992
             ), 4326
           );

COMMIT; 

-- BEGIN; 
-- -- make id_pand index for both pand and vbo 
-- CREATE INDEX idx_bag_pand_id_pand ON bag_pand(id_pand);
-- CREATE INDEX idx_bag_vbo_id_pand ON bag_vbo(id_pand);
-- COMMIT;

-- SELECT *
-- FROM bag_pand
-- LIMIT 100; 