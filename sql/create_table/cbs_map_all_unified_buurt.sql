DROP TABLE IF EXISTS cbs_map_all_unified_buurt; 
CREATE TABLE cbs_map_all_unified_buurt (
    year INTEGER,
    aant_inw INTEGER,
    aantal_hh INTEGER,
    woz NUMERIC,
    g_gas_tot NUMERIC,
    p_stadverw NUMERIC,
    g_elek_tot NUMERIC,
    emissions_kg_gas NUMERIC,
    emissions_kg_electricity NUMERIC,
    emissions_kg_total NUMERIC,
    emissions_kg_pp NUMERIC,
    neighborhood_geom GEOMETRY, 
    neighborhood_code VARCHAR,
    neighborhood VARCHAR,
    municipality VARCHAR,
    province VARCHAR
);