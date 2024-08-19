DELETE FROM cbs_map_all WHERE year = %s;

INSERT INTO cbs_map_all (
    year, bu_code, bu_naam, gm_code, gm_naam, 
    aant_inw, aantal_hh, woz, g_gas_tot, p_stadverw, g_elek_tot, 
    geometry, geom_4326
)
SELECT 
    %s AS year, "BU_CODE", "BU_NAAM", "GM_CODE", "GM_NAAM", 
    "AANT_INW", "AANTAL_HH", "WOZ", "G_GAS_TOT", "P_STADVERW", "G_ELEK_TOT", 
    geometry, ST_Transform(geometry, 4326) AS geom_4326
FROM cbs_map_%s
WHERE "OAD" > 0;