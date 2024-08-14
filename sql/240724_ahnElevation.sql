CREATE TABLE ahn2_elevation (
    rid SERIAL PRIMARY KEY,
    rast RASTER
);

INSERT INTO ahn2_elevation (rast)
SELECT ST_MapAlgebra(dsm.rast, dtm.rast, '([rast1] - [rast2])'::text)
FROM ahn2_dsm dsm, ahn2_dtm dtm
WHERE dsm.rid = dtm.rid;

-- SELECT ST_Transform(ST_Envelope(rast), 4326) AS geom, rid
-- FROM ahn2_dsm