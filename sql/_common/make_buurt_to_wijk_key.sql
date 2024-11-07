DELETE FROM key_buurt2022_to_wijk2012
WHERE municipality = 'Amsterdam';

INSERT INTO key_buurt2022_to_wijk2012 (neighborhood_code, neighborhood_geom, wk_code, municipality, wk_geom)

WITH wijken AS (
    SELECT wk_code, gm_naam AS municipality, ST_Buffer(geom, 500) AS wk_geom_buffer, geom AS wk_geom
    FROM cbs_wijk_2012
    WHERE gm_naam = 'Amsterdam'
),     
municipality_bbox AS (
    SELECT ST_Extent(wk_geom) AS bbox
    FROM wijken
), 
buurten AS (
    SELECT "BU_CODE" AS neighborhood_code, geometry AS neighborhood_geom
    FROM cbs_map_2022, municipality_bbox
    WHERE "WATER" = 'NEE'
        AND geometry && bbox
), 
buurten_with_wijk_overlap AS (
    SELECT 
		b.neighborhood_code, b.neighborhood_geom, w.wk_code, w.municipality, w.wk_geom,
        ST_Area(ST_Intersection(b.neighborhood_geom, w.wk_geom)) AS overlap_area
    FROM buurten b  
    JOIN wijken w 
    ON b.neighborhood_geom && w.wk_geom
        AND ST_Within(b.neighborhood_geom, w.wk_geom_buffer)
),
buurten_with_wijk AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY neighborhood_code ORDER BY overlap_area DESC) AS rank
    FROM buurten_with_wijk_overlap
)

SELECT neighborhood_code, neighborhood_geom, wk_code, municipality, wk_geom
FROM buurten_with_wijk
WHERE rank = 1;

