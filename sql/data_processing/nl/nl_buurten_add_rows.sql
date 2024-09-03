INSERT INTO nl_buurten (
    neighborhood_geom, neighborhood_code, neighborhood, 
    municipality, province
) 

SELECT 
    geometry AS neighborhood_geom, 
    "BU_CODE" AS neighborhood_code, 
    "BU_NAAM" AS neighborhood, 
    "GM_NAAM" AS municipality, 
    NULL AS province 
FROM cbs_map_2022
WHERE "H2O" = 'NEE'
