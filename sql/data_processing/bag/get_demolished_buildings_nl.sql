DELETE FROM demolished_buildings_nl
WHERE municipality = 'Amsterdam';

INSERT INTO demolished_buildings_nl(
	id_pand, geometry, build_year, status, document_date, document_number, 
    registration_start, registration_end, geom, geom_28992, province, 
    neighborhood_code, neighborhood, municipality, function, sqm
)

-- get demolished buildings 
WITH demolished_buildings AS (
	SELECT * FROM bag_pand 
	WHERE municipality = 'Amsterdam' 
		AND status = 'Pand gesloopt'
), 

-- find sqm and function from vbo 
bag_vbo_sample AS (
    SELECT *
    FROM bag_vbo
    WHERE municipality = 'Amsterdam'
), 
demolished_units AS (
    SELECT DISTINCT ON (id_vbo) * 
    FROM bag_vbo_sample
    WHERE 
		-- TODO: decide which status to use 
		-- status = 'Verblijfsobject in gebruik'
		status = 'Verblijfsobject buiten gebruik' 
		OR status = 'Verblijfsobject ingetrokken'
		AND sqm::INTEGER < 9999
    ORDER BY id_vbo, registration_start DESC
), 
demolished_buildings_info AS (
	SELECT id_pand, 
		ARRAY_AGG(DISTINCT function) AS function, 
		SUM(sqm::INTEGER) AS sqm, 
		MIN(neighborhood_code) AS neighborhood_code
	FROM demolished_units
	GROUP BY id_pand
), 
demolished_buildings_vbo AS (
	SELECT a.*, b.function, b.sqm
	FROM demolished_buildings a 
	LEFT JOIN demolished_buildings_info b 
	ON a.id_pand = b.id_pand
), 
buildings_known AS (
	SELECT * FROM demolished_buildings_vbo 
	WHERE sqm IS NOT NULL
), 
	
-- guess sqm if not found in vbo
buildings_unknown AS (
	SELECT * FROM demolished_buildings_vbo 
	WHERE sqm IS NULL
), 
buildings_ahnversion AS (
	    SELECT 
        CASE 		    
            WHEN registration_start BETWEEN '2011-01-01' AND '2012-12-31' THEN 'ahn2' -- not 100 percent accurate 
            WHEN registration_start BETWEEN '2013-01-01' AND '2019-12-31' THEN 'ahn2'
            WHEN registration_start BETWEEN '2020-01-01' AND '2022-12-31' THEN 'ahn3'
            WHEN registration_start >= '2023-01-01' THEN 'ahn4'
            ELSE 'unforseen case'
        END AS ahn_version, 
        * 
    FROM buildings_unknown 
), 

-- clip rasters by building footprint 
elevation_municipality AS (
    SELECT *
    FROM ahn_elevation 
    WHERE municipality = 'Amsterdam'
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
buildings_sqm_ahn AS (
    SELECT
		(n_floors * footprint_sqm)::BIGINT AS sqm, *
    FROM building_nfloors
), 
buildings_sqm_guesses AS (
	SELECT 
		a.id_pand, a.geometry, a.build_year, a.status, a.document_date, a.document_number, 
		a.registration_start, a.registration_end, a.geom, a.geom_28992, 
		a.province, a.neighborhood_code, a.neighborhood, a.municipality, a.function, b.sqm
	FROM buildings_unknown a 
	LEFT JOIN buildings_sqm_ahn b 
	ON a.id_pand = b.id_pand 
)

-- concatenate both tables - buildings_known and buildings_sqm_guesses 
SELECT * FROM buildings_sqm_guesses
UNION
SELECT * FROM buildings_known
