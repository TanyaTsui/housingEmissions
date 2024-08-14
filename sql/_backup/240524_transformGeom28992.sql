-- Add transformed geometry columns
ALTER TABLE bag_pand ADD COLUMN geom_28992 geometry;
ALTER TABLE bag_vbo ADD COLUMN geom_28992 geometry;
ALTER TABLE cdwaste ADD COLUMN geom_28992 geometry;

-- Update the transformed geometry columns
UPDATE bag_pand SET geom_28992 = ST_Transform(geom, 28992);
UPDATE bag_vbo SET geom_28992 = ST_Transform(geom, 28992);
UPDATE cdwaste SET geom_28992 = ST_Transform(geom, 28992);

-- Create indexes on the transformed geometry columns
CREATE INDEX IF NOT EXISTS idx_bag_pand_geom_28992 ON bag_pand USING GIST (geom_28992);
CREATE INDEX IF NOT EXISTS idx_bag_vbo_geom_28992 ON bag_vbo USING GIST (geom_28992);
CREATE INDEX IF NOT EXISTS idx_cdwaste_geom_28992 ON cdwaste USING GIST (geom_28992);

