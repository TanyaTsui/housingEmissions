CREATE TABLE IF NOT EXISTS emissions_all_buurt_s3 (
    year INTEGER,
    municipality VARCHAR,
    wk_code VARCHAR,
    bu_code VARCHAR,
    bu_geom GEOMETRY,
    embodied_kg_s0 NUMERIC,
    embodied_kg_s1 NUMERIC,
    embodied_kg_s2 NUMERIC,
    embodied_kg_s3 NUMERIC,
    operational_kg_s0 NUMERIC,
    operational_kg_s1 NUMERIC,
    operational_kg_s2 NUMERIC,
    operational_kg_s3 NUMERIC,
    construction NUMERIC,
    construction_s3 NUMERIC,
    transformation NUMERIC,
    transformation_s3 NUMERIC,
    renovation NUMERIC,
    demolition NUMERIC,
    inuse NUMERIC,
    inuse_s3 NUMERIC,
    tot_gas_m3 NUMERIC,
    tot_gas_m3_s3 NUMERIC,
    tot_elec_kwh NUMERIC,
    tot_elec_kwh_s3 NUMERIC,
    population NUMERIC,
    population_change NUMERIC,
    woz NUMERIC,
    n_homes NUMERIC
);


