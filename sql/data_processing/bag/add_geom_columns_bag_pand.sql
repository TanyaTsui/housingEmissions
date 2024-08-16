-- Step 1: Add the Geometry Columns
ALTER TABLE bag_pand
ADD COLUMN IF NOT EXISTS geom geometry(Polygon, 4326),
ADD COLUMN IF NOT EXISTS geom_28992 geometry(Polygon, 28992);

-- Step 2: Convert Text Data to Geometry and Populate the geom_28992 Column
UPDATE bag_pand
SET geom_28992 = ST_GeomFromText(
                    'POLYGON((' || 
                    RTRIM(REPLACE(geometry, ' 0.0', ', '), ', ')
                    || '))', 28992)
WHERE geometry IS NOT NULL;

-- Step 3: Transform the 28992 Geometry to 4326 and Populate the geom Column
UPDATE bag_pand
SET geom = ST_Transform(geom_28992, 4326)
WHERE geom_28992 IS NOT NULL;

-- Step 4: Create Spatial Indexes for the New Geometry Columns
CREATE INDEX IF NOT EXISTS idx_bag_pand_geom ON bag_pand USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_bag_pand_geom_28992 ON bag_pand USING GIST (geom_28992);
