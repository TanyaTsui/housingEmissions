-- get demolitions and constructions in area 
WITH demolitions AS (
	SELECT * FROM demolished_buildings_nl 
	WHERE municipality = 'Amsterdam' 
), 
constructions AS (
	SELECT * FROM housing_nl 
	WHERE municipality = 'Amsterdam'
		AND status = 'Bouw gestart'
), 

-- get replacement constructions (constructions that did intersect with demolitions)
constructions_replacement_raw AS (
	SELECT 
		c.sqm AS c_sqm, d.sqm AS d_sqm, 
		d.geom AS d_geom, d.geom_28992 AS d_geom_28992, 
		ST_Area(d.geom_28992) AS d_footprint, ST_Area(c.geom_28992) AS c_footprint, 
		ST_Area(ST_Intersection(c.geom_28992, d.geom_28992)) / ST_Area(d.geom_28992) * d.sqm AS replaced_sqm,
		ST_Area(ST_Intersection(c.geom_28992, d.geom_28992)) / ST_Area(c.geom_28992) * c.sqm AS new_sqm,
		ST_Transform(ST_Intersection(c.geom_28992, d.geom_28992), 4326) AS intersection, 
		c.*
	FROM constructions c 
	JOIN demolitions d 
	ON c.neighborhood_code = d.neighborhood_code
		AND ST_Intersects(c.geom_28992, d.geom_28992)
), 
constructions_replacement AS (
	SELECT id_pand, 
		SUM(replaced_sqm) AS replaced_sqm, SUM(new_sqm) AS new_sqm, 
		MIN(c_sqm) AS c_sqm, SUM(d_sqm) AS d_sqm, 
		MIN(c_footprint) AS c_footprint, SUM(d_footprint) AS d_footprint, 
		COUNT(d_sqm) AS count, 
		ST_SetSRID(ST_GeomFromText(MIN(ST_AsText(geom))), 4326) AS geom
	FROM constructions_replacement_raw
	GROUP BY id_pand 
)

SELECT * FROM constructions_replacement_raw LIMIT 5

-- -- make housing_nl_s1 with new statuses 
-- renovations AS (
-- 	SELECT id_pand, 'renovation - s1' AS status
-- 	FROM constructions_replacement
-- 	WHERE replaced_sqm != 0 AND new_sqm <= replaced_sqm AND c_sqm <= d_sqm
-- ), 
-- housing_reality AS (
-- 	SELECT * 
-- 	FROM housing_nl 
-- 	WHERE municipality = 'Amsterdam' 
-- ), 

-- -- replace constructions with renovations (when applicable)
-- housing_s1_renovations AS (
-- 	SELECT 
-- 	    COALESCE(hs.status, hr.status) AS status, -- Use status from housing_s1 if exists, else keep original status
-- 		hr.function, hr.sqm, hr.id_pand, hr.build_year, 
-- 		hr.document_date, hr.document_number, hr.registration_start, hr.registration_end, hr.geom, hr.geom_28992, 
-- 		hr.neighborhood_code, hr.neighborhood, hr.municipality, hr.province
-- 	FROM housing_reality hr
-- 	LEFT JOIN renovations hs
-- 	ON hr.id_pand = hs.id_pand
-- )