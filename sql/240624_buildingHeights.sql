-- Calculate average elevation for each building footprint
WITH buildings_intersecting_raster AS (
  SELECT b.id_pand, b.geom_28992, b.status
  FROM housing_emissions_nl b, ahn2_buildingheight r
  WHERE ST_Intersects(r.rast, b.geom_28992)
), 
clipped_rasters AS (
  SELECT
    bir.id_pand,
    ST_Clip(r.rast, bir.geom_28992) AS clipped_raster
  FROM
    buildings_intersecting_raster bir, public.ahn2_88 r
  WHERE
    ST_Intersects(r.rast, bir.geom_28992)
),
average_elevations AS (
  SELECT
    cr.id_pand,
    (ST_SummaryStats(ST_Union(cr.clipped_raster))).mean AS avg_elevation
  FROM
    clipped_rasters cr
  GROUP BY
    cr.id_pand
)
	
SELECT * -- , id_pand, ST_SummaryStats(clipped_raster) AS av_elevation
FROM clipped_rasters
LIMIT 100