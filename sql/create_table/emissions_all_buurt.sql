CREATE TABLE IF NOT EXISTS emissions_all_buurt (
    municipality CHARACTER VARYING,
    wk_code CHARACTER VARYING(8),
    bu_code CHARACTER VARYING,
    bu_geom GEOMETRY,
    year INTEGER,
    construction NUMERIC,
    renovation NUMERIC,
    transformation NUMERIC,
    demolition NUMERIC,
    population NUMERIC,
    n_homes NUMERIC,
    tot_gas_m3 NUMERIC,
    tot_elec_kwh NUMERIC,
    woz NUMERIC,
    embodied_kg NUMERIC,
    operational_kg NUMERIC
);
