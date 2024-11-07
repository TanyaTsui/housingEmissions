CREATE TABLE IF NOT EXISTS cbs_map_all_wijk (
    year INTEGER,
    municipality VARCHAR(60),
    wk_code VARCHAR(8),
    wk_geom GEOMETRY,
    population NUMERIC,
    n_households NUMERIC,
    n_homes NUMERIC,
    gas_m3 NUMERIC,
    elec_kwh NUMERIC,
    av_p_stadverw NUMERIC,
    av_woz NUMERIC
);
