CREATE TABLE IF NOT EXISTS cbs_map_all_buurt (
    municipality TEXT,
    wk_code TEXT,
    bu_code TEXT,
    bu_geom GEOMETRY,
    year INTEGER,
    n_units NUMERIC,
    population NUMERIC,
    woz NUMERIC,
    tot_gas_m3 NUMERIC,
    tot_elec_kwh NUMERIC
);
