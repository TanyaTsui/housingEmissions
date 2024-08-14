-- DROP TABLE IF EXISTS temp_bag_pand; 
-- CREATE TEMP TABLE temp_bag_pand AS
-- SELECT *
-- FROM bag_pand
-- LIMIT 10;

-- Add the new geometry column to the temporary table
ALTER TABLE bag_pand ADD COLUMN geom geometry(Polygon, 4326);

-- Update the geom column with transformed geometry data
UPDATE bag_pand
SET geom = ST_Transform(
             ST_SetSRID(
               ST_GeomFromText(
                 'POLYGON((' || RTRIM(REPLACE(geometry || ' ', ' 0.0 ', ','), ',') || '))'
               ), 28992
             ), 4326
           );

-- SELECT * FROM temp_bag_pand LIMIT 100;
