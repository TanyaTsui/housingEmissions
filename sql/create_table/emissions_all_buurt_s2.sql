CREATE TABLE IF NOT EXISTS emissions_all_buurt_s2 (
    year INTEGER,
    municipality CHARACTER VARYING,
    wk_code CHARACTER VARYING,
    bu_code CHARACTER VARYING,
    bu_geom GEOMETRY,
    construction NUMERIC,
    demolition NUMERIC,
    transformation NUMERIC,
    renovation NUMERIC,
    operational_kg_s0 NUMERIC,
    operational_kg_s1 NUMERIC,
    operational_kg_s2 NUMERIC,
    embodied_kg_s0 NUMERIC,
    embodied_kg_s1 NUMERIC,
    embodied_kg_s2 NUMERIC
);
