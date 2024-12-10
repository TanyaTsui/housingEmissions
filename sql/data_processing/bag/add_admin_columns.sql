-- 1. Remove the specified columns
ALTER TABLE bag_vbo
    DROP COLUMN IF EXISTS geometry,
    DROP COLUMN IF EXISTS geom,
    DROP COLUMN IF EXISTS province,
    DROP COLUMN IF EXISTS municipality,
    DROP COLUMN IF EXISTS wk_code,
    DROP COLUMN IF EXISTS neighborhood_code,
    DROP COLUMN IF EXISTS neighborhood;

-- 2. Change the column name from geom_28992 to pand_geom
ALTER TABLE bag_vbo
    RENAME COLUMN pand_geom TO vbo_geom;

-- 3. Add the new columns with character varying data types, and bu_geom as geometry
ALTER TABLE bag_vbo
    ADD COLUMN municipality character varying,
    ADD COLUMN wk_code character varying,
    ADD COLUMN bu_code character varying,
	ADD COLUMN year integer, 
    ADD COLUMN bu_geom geometry;




-- 1. Remove the specified columns
ALTER TABLE bag_pand
    DROP COLUMN IF EXISTS geom,
    DROP COLUMN IF EXISTS geometry,
    DROP COLUMN IF EXISTS province,
    DROP COLUMN IF EXISTS municipality,
    DROP COLUMN IF EXISTS wk_code,
    DROP COLUMN IF EXISTS neighborhood_code,
    DROP COLUMN IF EXISTS neighborhood;

-- 2. Rename geom_28992 to pand_geom
ALTER TABLE bag_pand
    RENAME COLUMN geom_28992 TO pand_geom;

-- 3. Add the new columns with character varying for text fields and geometry for bu_geom
ALTER TABLE bag_pand
    ADD COLUMN municipality character varying,
    ADD COLUMN wk_code character varying,
    ADD COLUMN bu_code character varying,
	ADD COLUMN year integer, 
    ADD COLUMN bu_geom geometry;



