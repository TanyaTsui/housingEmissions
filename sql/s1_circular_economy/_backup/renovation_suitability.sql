DELETE FROM housing_nl_s1
WHERE municipality = 'Amsterdam';

INSERT INTO housing_nl_s1 (
	status, function, sqm, id_pand, build_year, document_date, document_number, 
	registration_start, registration_end, geom, geom_28992, 
	neighborhood_code, neighborhood, municipality, province
)

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

-- get new constructions (constructions that didn't replace demolitions)
constructions_new_raw AS (
	SELECT 
		c.*
	FROM constructions c 
	LEFT JOIN demolitions d 
	ON c.neighborhood_code = d.neighborhood_code
		AND ST_Intersects(c.geom_28992, d.geom_28992)
	WHERE d.neighborhood_code IS NULL
), 
constructions_new AS (
	SELECT id_pand, SUM(sqm) AS c_sqm, 
		ST_SetSRID(ST_GeomFromText(MIN(ST_AsText(geom))), 4326) AS geom
	FROM constructions_new_raw
	GROUP BY id_pand 
), 

-- get replacement constructions (constructions that did replace demolitions)
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
), 

-- make housing_nl_s1 with new statuses 
housing_s1_construction AS (
	SELECT 
		id_pand, 
		CASE 
	        WHEN replaced_sqm != 0 AND new_sqm <= replaced_sqm AND c_sqm <= d_sqm THEN 'renovation - s1'
	        ELSE 'Bouw gestart'
	    END AS status
	FROM constructions_replacement
	UNION ALL 
	SELECT id_pand, 'Bouw gestart' AS status FROM constructions_new
), 
housing_reality AS (
	SELECT * 
	FROM housing_nl 
	WHERE municipality = 'Amsterdam' 
), 

-- replace constructions with renovations (when applicable)
housing_s1 AS (
	SELECT 
	    COALESCE(hs.status, hr.status) AS status, -- Use status from housing_s1 if exists, else keep original status
		hr.function, hr.sqm, hr.id_pand, hr.build_year, 
		hr.document_date, hr.document_number, hr.registration_start, hr.registration_end, hr.geom, hr.geom_28992, 
		hr.neighborhood_code, hr.neighborhood, hr.municipality, hr.province
	FROM housing_reality hr
	LEFT JOIN housing_s1_construction hs
	ON hr.id_pand = hs.id_pand
), 

-- delete demolitions that were replaced by renovation - s1 
renovations_s1 AS (
	SELECT * 
	FROM housing_s1
	WHERE status = 'renovation - s1'
), 
demolitions_to_delete AS (
	SELECT h.*
	FROM housing_s1 h 
	JOIN renovations_s1 r 
	ON h.neighborhood_code = r.neighborhood_code 
		AND ST_intersects(h.geom_28992, r.geom_28992)
	WHERE h.status = 'Pand gesloopt'
)

SELECT * 
FROM housing_s1
WHERE NOT (
        status = 'Pand gesloopt' 
        AND id_pand IN (SELECT id_pand FROM demolitions_to_delete)
    )