-- Create spatial indexes if they do not exist
CREATE INDEX IF NOT EXISTS idx_bag_pand_geom ON bag_pand USING GIST (geom_28992);
CREATE INDEX IF NOT EXISTS idx_nl_gemeentegebied_geom ON nl_gemeentegebied USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_bag_vbo_geom ON bag_vbo USING GIST (geom_28992);


ALTER TABLE bag_pand ADD COLUMN municipality VARCHAR;
ALTER TABLE bag_pand ADD COLUMN province VARCHAR;

UPDATE bag_pand
SET 
    municipality = g.naam,
    province = g.ligt_in_provincie_naam
FROM nl_gemeentegebied g
WHERE ST_Within(bag_pand.geom_28992, g.geom);

ALTER TABLE bag_vbo ADD COLUMN municipality VARCHAR;
ALTER TABLE bag_vbo ADD COLUMN province VARCHAR;

UPDATE bag_vbo
SET 
    municipality = g.naam,
    province = g.ligt_in_provincie_naam
FROM nl_gemeentegebied g
WHERE ST_Within(bag_vbo.geom_28992, g.geom);