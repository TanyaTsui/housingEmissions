CREATE TABLE IF NOT EXISTS emissions_all_wijk (
    municipality VARCHAR,
    wk_code VARCHAR(8),
    year INTEGER,
    construction NUMERIC,
    renovation NUMERIC,
    transformation NUMERIC,
    demolition NUMERIC,
    population NUMERIC,
    n_households NUMERIC,
    n_homes NUMERIC,
    gas_m3 NUMERIC,
    elec_kwh NUMERIC,
    av_p_stadverw NUMERIC,
    av_woz NUMERIC,
    embodied_kg NUMERIC,
    operational_kg NUMERIC,
    geom GEOMETRY
);
