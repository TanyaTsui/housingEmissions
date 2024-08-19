DROP TABLE IF EXISTS cbs_map_all; 
CREATE TABLE IF NOT EXISTS cbs_map_all (
    year INTEGER, 
    bu_code VARCHAR,
    bu_naam VARCHAR,
    gm_code VARCHAR,
    gm_naam VARCHAR,
    aant_inw INTEGER,
    aantal_hh INTEGER,
    woz NUMERIC,
    g_gas_tot NUMERIC,
    p_stadverw NUMERIC,
    g_elek_tot NUMERIC,
    geometry geometry, 
    geom_4326 geometry 
);