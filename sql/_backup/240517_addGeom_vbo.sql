-- Step 1: Add the geom column to the bag_vbo table
ALTER TABLE bag_vbo ADD COLUMN geom geometry(Geometry, 4326);

-- Step 2: Update the geom column with the transformed geometries
UPDATE bag_vbo
SET geom = ST_Transform(
             ST_SetSRID(
               ST_MakePoint(
                 SPLIT_PART(geometry, ' ', 1)::float,
                 SPLIT_PART(geometry, ' ', 2)::float
               ), 28992
             ), 4326
           );
