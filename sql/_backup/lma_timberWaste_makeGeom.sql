-- add geometry column
ALTER TABLE public."timberWaste"
ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326); -- 4326 is the SRID for WGS 84, commonly used for lat/lon
UPDATE public."timberWaste"
SET geom = ST_SetSRID(ST_MakePoint(lat, lon), 4326);

SELECT * 
FROM public."timberWaste";
-- WHERE registration_end IS NULL or total_sqm IS NULL
-- LIMIT 1000;

SELECT * FROM 