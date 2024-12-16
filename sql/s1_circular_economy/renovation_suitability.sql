DELETE FROM housing_nl_s2
WHERE municipality = 'Delft';

INSERT INTO housing_nl_s2 (
	municipality, wk_code, bu_code, id_pand, pand_geom, 
	status, function, sqm, build_year, document_date, document_number, 
	registration_start, registration_end
)

-- get demolitions and constructions in area 
WITH demolitions AS (
	SELECT * FROM housing_nl 
	WHERE municipality = 'Delft' 
		AND status = 'Pand gesloopt'
), 
constructions AS (
	SELECT * FROM housing_nl 
	WHERE municipality = 'Delft'
		AND status = 'Bouw gestart'
), 

-- get replacement constructions (constructions that did intersect with demolitions)
constructions_replacement_raw AS (
	SELECT 
		c.sqm AS c_sqm, d.sqm AS d_sqm, d.pand_geom AS d_geom, 
		ST_Area(d.pand_geom) AS d_footprint, ST_Area(c.pand_geom) AS c_footprint, 
		ST_Area(ST_Intersection(c.pand_geom, d.pand_geom)) / ST_Area(d.pand_geom) * d.sqm AS replaced_sqm,
		ST_Area(ST_Intersection(c.pand_geom, d.pand_geom)) / ST_Area(c.pand_geom) * c.sqm AS new_sqm,
		ST_Transform(ST_Intersection(c.pand_geom, d.pand_geom), 4326) AS intersection, 
		c.*
	FROM constructions c 
	JOIN demolitions d 
	ON c.bu_code = d.bu_code
		AND ST_Intersects(c.pand_geom, d.pand_geom)
), 
constructions_replacement AS (
	SELECT id_pand, 
		SUM(replaced_sqm) AS replaced_sqm, SUM(new_sqm) AS new_sqm, 
		MIN(c_sqm) AS c_sqm, SUM(d_sqm) AS d_sqm, 
		MIN(c_footprint) AS c_footprint, SUM(d_footprint) AS d_footprint, 
		COUNT(d_sqm) AS count, 
		ST_SetSRID(ST_GeomFromText(MIN(ST_AsText(pand_geom))), 4326) AS pand_geom
	FROM constructions_replacement_raw
	GROUP BY id_pand 
), 

-- make housing_nl_s1 with new statuses 
renovations AS (
	SELECT id_pand, 'renovation - s1' AS status
	FROM constructions_replacement
	WHERE replaced_sqm != 0 AND new_sqm <= replaced_sqm AND c_sqm <= d_sqm
), 
housing_reality AS (
	SELECT * 
	FROM housing_nl 
	WHERE municipality = 'Delft' 
), 

-- replace constructions with renovations (when applicable)
housing_s1_renovations AS (
	SELECT 
		hr.municipality, hr.wk_code, hr.bu_code, hr.id_pand, hr.pand_geom, 
	    COALESCE(hs.status, hr.status) AS status, -- Use status from housing_s1 if exists, else keep original status
		hr.function, hr.sqm, hr.build_year, 
		hr.document_date, hr.document_number, hr.registration_start, hr.registration_end
	FROM housing_reality hr
	LEFT JOIN renovations hs
	ON hr.id_pand = hs.id_pand
), 

-- delete demolitions that were replaced by renovation - s1 
renovations_s1 AS (
	SELECT * FROM housing_s1_renovations WHERE status = 'renovation - s1'
), 
demolitions_to_delete AS (
	SELECT h.*
	FROM housing_s1_renovations h 
	JOIN renovations_s1 r 
	ON h.bu_code = r.bu_code 
		AND ST_intersects(h.pand_geom, r.pand_geom)
	WHERE h.status = 'Pand gesloopt'
), 
housing_s1 AS (
	SELECT * 
	FROM housing_s1_renovations
	WHERE NOT (
	        status = 'Pand gesloopt' 
	        AND id_pand IN (SELECT id_pand FROM demolitions_to_delete)
	    )
)
SELECT * FROM housing_s1