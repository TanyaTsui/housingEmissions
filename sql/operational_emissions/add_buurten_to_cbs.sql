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

INSERT INTO cbs_map_all_unified_buurt (
    year, aant_inw, aantal_hh, woz, 
    g_gas_tot, p_stadverw, g_elek_tot, 
    emissions_kg_gas, emissions_kg_electricity, emissions_kg_total, 
    emissions_kg_pp, neighborhood_geom, neighborhood_code, neighborhood, 
    municipality, province
)

SELECT 
    c.year, c.aant_inw, c.aantal_hh, c.woz, 
	c.g_gas_tot, c.p_stadverw, c.g_elek_tot, 
	c.emissions_kg_gas, c.emissions_kg_electricity, c.emissions_kg_total, 
	c.emissions_kg_pp, 
	b.*
FROM 
    cbs_map_all c 
LEFT JOIN 
    nl_buurten b 
ON 
	c.geometry && b.neighborhood_geom
    AND ST_Within(ST_Centroid(c.geometry), b.neighborhood_geom)
