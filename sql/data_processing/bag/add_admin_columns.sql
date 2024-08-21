ALTER TABLE bag_pand DROP COLUMN IF EXISTS neighborhood_code;
ALTER TABLE bag_pand DROP COLUMN IF EXISTS neighborhood;
ALTER TABLE bag_pand DROP COLUMN IF EXISTS municipality;

ALTER TABLE bag_pand ADD COLUMN neighborhood_code VARCHAR;
ALTER TABLE bag_pand ADD COLUMN neighborhood VARCHAR;
ALTER TABLE bag_pand ADD COLUMN municipality VARCHAR;

ALTER TABLE bag_vbo DROP COLUMN IF EXISTS neighborhood_code;
ALTER TABLE bag_vbo DROP COLUMN IF EXISTS neighborhood;
ALTER TABLE bag_vbo DROP COLUMN IF EXISTS municipality;

ALTER TABLE bag_vbo ADD COLUMN neighborhood_code VARCHAR;
ALTER TABLE bag_vbo ADD COLUMN neighborhood VARCHAR;
ALTER TABLE bag_vbo ADD COLUMN municipality VARCHAR;
