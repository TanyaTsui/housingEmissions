CREATE TABLE IF NOT EXISTS emissions_all_buurt_s1 (
    year INTEGER,
    municipality TEXT,
    wk_code TEXT,
    bu_code TEXT,
    bu_geom GEOMETRY,
    population NUMERIC,
    woz NUMERIC,
    n_homes NUMERIC,
    inuse NUMERIC,
    construction NUMERIC,
    transformation NUMERIC,
    renovation NUMERIC,
    demolition NUMERIC,
    gas_m3_s0 NUMERIC,
    gas_m3_s1 NUMERIC,
    electricity_kwh_s0 NUMERIC,
    electricity_kwh_s1 NUMERIC,
    operational_kg_s0 NUMERIC,
    operational_kg_s1 NUMERIC,
    embodied_kg_s0 NUMERIC,
    embodied_kg_s1 NUMERIC
);