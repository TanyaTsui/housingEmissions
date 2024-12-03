CREATE TABLE IF NOT EXISTS emissions_all_wijk_s2 (
    year INTEGER,
    municipality TEXT,
    wk_code VARCHAR(8),
    wk_geom GEOMETRY,
    construction NUMERIC,
	demolition NUMERIC,
    transformation NUMERIC,
    renovation NUMERIC,
	embodied_kg_s2 NUMERIC, 
    operational_kg_s2 NUMERIC
);

