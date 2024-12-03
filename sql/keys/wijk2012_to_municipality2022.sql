CREATE TABLE key_wijk2012_to_municipality2022 AS

WITH municipalities AS (
    SELECT "GM_NAAM" AS municipality, geometry
    FROM nl_gemeenten
    WHERE "H2O" = 'NEE'
), 
wijken AS (
    SELECT wk_code, geom AS geometry 
    FROM cbs_wijk_2012 
    WHERE water = 'NEE'
),
intersections AS (
    SELECT 
        a.wk_code,
        b.municipality,
        ST_Area(ST_Intersection(a.geometry, b.geometry)) AS intersect_area
    FROM wijken a 
    JOIN municipalities b 
    ON ST_Intersects(a.geometry, b.geometry)
),
ranked_intersections AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY wk_code ORDER BY intersect_area DESC) AS rn
    FROM intersections
)

SELECT wk_code, municipality
FROM ranked_intersections
WHERE rn = 1;
