-- create indexes
CREATE INDEX IF NOT EXISTS idx_housing_nl_status_registration_start ON housing_nl (status, registration_start);
CREATE INDEX IF NOT EXISTS idx_buildings_geom_28992 ON housing_nl USING gist (geom_28992);
CREATE INDEX IF NOT EXISTS idx_ahn_elevation_rast_geom ON ahn_elevation USING gist (rast_geom);

ALTER TABLE housing_nl 
ADD COLUMN IF NOT EXISTS ahn_version VARCHAR; 

INSERT INTO housing_nl (
    sqm, ahn_version, function, id_pand, build_year, status, 
    document_date, registration_start, registration_end, 
    geom, geom_28992, 
    neighborhood_code, neighborhood, municipality, province
)

WITH buildings AS (
    SELECT 
        sqm, function, id_pand, build_year, status, 
        document_date, registration_start, registration_end, 
        geom, geom_28992, neighborhood_code, neighborhood, municipality, province
    FROM housing_nl
    WHERE 
        sqm IS NULL AND 
        ST_Area(geom_28992) < 100000 AND 
        municipality = %s
), 	

-- assign rows to ahn version
buildings_ahnversion AS (
    SELECT 
        CASE 		
            -- cases for bouw gestart 
            WHEN status = 'Bouw gestart' AND registration_start BETWEEN '2011-01-01' AND '2014-01-01' THEN 'ahn3'
            WHEN status = 'Bouw gestart' AND registration_start BETWEEN '2014-01-01' AND '2020-01-01' THEN 'ahn4'
            -- must be NULL, definitely don't have height data for buildings built after 2020 
            WHEN status = 'Bouw gestart' AND registration_start > '2020-01-01' THEN 'no ahn available' 
    
            -- cases for verbouwing pand 
            WHEN status = 'Verbouwing pand' AND registration_start BETWEEN '2011-01-01' AND '2013-06-30' THEN 'ahn2'
            WHEN status = 'Verbouwing pand' AND registration_start BETWEEN '2013-07-01' AND '2019-12-31' THEN 'ahn3' 
            WHEN status = 'Verbouwing pand' AND registration_start > '2020-01-01' THEN 'ahn4'
    
            -- cases for pand gesloopt 
            -- buildings demolished pre-2013 could use ahn2, but there is a risk that the elevation data was recorded during / after demolition. 
            -- for 100 percent accuracy, AHN1 data is needed, but (easily) available. (online has only DSM at 5m resolution) 
            WHEN status = 'Pand gesloopt' AND registration_start BETWEEN '2011-01-01' AND '2012-12-31' THEN 'ahn2' -- not 100 percent accurate 
            WHEN status = 'Pand gesloopt' AND registration_start BETWEEN '2013-01-01' AND '2019-12-31' THEN 'ahn2'
            WHEN status = 'Pand gesloopt' AND registration_start BETWEEN '2020-01-01' AND '2022-12-31' THEN 'ahn3'
            WHEN status = 'Pand gesloopt' AND registration_start >= '2023-01-01' THEN 'ahn4'
    
            -- other null cases 
            ELSE 'unforseen case'
        END AS ahn_version, 
        * 
    FROM buildings
), 

-- clip rasters by building footprint 
elevation_municipality AS (
    SELECT *
    FROM ahn_elevation 
    WHERE municipality = %s
), 
clipped_rasters AS (
    SELECT 
        b.ahn_version, b.function, b.id_pand, b.build_year, 
        b.status, b.document_date, b.registration_start, b.registration_end, 
        b.geom, b.geom_28992, b.neighborhood_code, b.neighborhood, b.municipality, b.province, 
        ST_Union(ST_Clip(r.rast, 1, b.geom_28992, -9999)) AS raster
    FROM buildings_ahnversion b
    LEFT JOIN elevation_municipality r
    ON ST_Intersects(r.rast_geom, b.geom_28992) AND b.ahn_version = r.ahn_version 
    GROUP BY
        b.ahn_version, b.function, b.id_pand, b.build_year, 
        b.status, b.document_date, b.registration_start, b.registration_end, 
        b.geom, b.geom_28992, b.neighborhood_code, b.neighborhood, b.municipality, b.province
), 

-- estimate n floors, sqm 
building_heights AS (
    SELECT
        *, ST_Area(geom_28992) AS footprint_sqm, 
        (ST_SummaryStats(raster)).count AS n_pixels, (ST_SummaryStats(raster)).max AS height
    FROM clipped_rasters
), 
building_nfloors AS (
    SELECT 
        CASE
            WHEN height < 4 THEN 1 
            WHEN height > 4 THEN ROUND(height / 3)
            ELSE 1 
        END AS n_floors, 
        * 
    FROM building_heights
), 
building_sqm_ahn AS (
    SELECT
        (n_floors * footprint_sqm)::INTEGER AS sqm, * 
    FROM building_nfloors
), 
to_insert AS (
    SELECT 
        sqm::INTEGER AS sqm, ahn_version, function, id_pand, build_year, 
        status, document_date, registration_start, registration_end, geom, 
        geom_28992, neighborhood_code, neighborhood, municipality, province
    FROM building_sqm_ahn
)
SELECT * FROM to_insert; 

-- delete row that didn't have sqm data 
DELETE FROM housing_nl
WHERE sqm IS NULL AND municipality = %s; 
