WITH overlapped_neighborhoods AS (
    SELECT 
        neighborhood_code,
        wk_code,
        municipality,
        neighborhood_geom,
        wk_geom,
        ST_Area(ST_Intersection(neighborhood_geom, wk_geom)) AS overlap_area
    FROM key_buurt2022_to_wijk2012
),
ranked_overlaps AS (
    SELECT 
        neighborhood_code,
        wk_code,
        municipality,
        neighborhood_geom,
        wk_geom,
        overlap_area,
        ROW_NUMBER() OVER (PARTITION BY neighborhood_code ORDER BY overlap_area DESC) AS rank
    FROM overlapped_neighborhoods
)
	
DELETE FROM key_buurt2022_to_wijk2012
WHERE (neighborhood_code, wk_code) IN (
    SELECT neighborhood_code, wk_code
    FROM ranked_overlaps
    WHERE rank > 1
)