DELETE FROM housing_nl_s2
WHERE municipality = 'Delft';

INSERT INTO housing_nl_s2 (
	year, municipality, wk_code, bu_code, id_pand, pand_geom, 
	status, function, sqm, build_year, document_date, document_number, 
	registration_start, registration_end
)

-- get demolitions and constructions in area 
WITH demolitions AS (
	SELECT 
		CASE 
			WHEN registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
			ELSE LEFT(registration_start, 4)::INTEGER 
		END AS year, * 
	FROM housing_nl 
	WHERE municipality = 'Delft' 
		AND status = 'Pand gesloopt'
), 
constructions AS (
	SELECT 
		CASE 
			WHEN registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
			ELSE LEFT(registration_start, 4)::INTEGER 
		END AS year, * 
	FROM housing_nl 
	WHERE municipality = 'Delft'
		AND status = 'Bouw gestart'
), 

-- get replacement constructions (constructions that did intersect with demolitions)
constructions_replacement_raw AS (
	SELECT 
		c.id_pand AS c_id_pand, d.id_pand AS d_id_pand, 
		c.sqm AS c_sqm, d.sqm AS d_sqm, d.pand_geom AS d_geom, 
		ST_Area(d.pand_geom) AS d_footprint, ST_Area(c.pand_geom) AS c_footprint, 
		ST_Area(ST_Intersection(c.pand_geom, d.pand_geom)) / ST_Area(d.pand_geom) * d.sqm AS replaced_sqm,
		ST_Area(ST_Intersection(c.pand_geom, d.pand_geom)) / ST_Area(c.pand_geom) * c.sqm AS new_sqm,
		ST_Transform(ST_Intersection(c.pand_geom, d.pand_geom), 4326) AS intersection, 
		c.*
	FROM constructions c 
	JOIN demolitions d 
	ON c.bu_code = d.bu_code
		AND c.year BETWEEN d.year - 5 AND d.year + 5
		AND ST_Intersects(c.pand_geom, d.pand_geom)
), 
-- sqm of new replacement construction per demolished building 
constructions_replacement AS ( 
	SELECT d_id_pand AS id_pand, 
		SUM(replaced_sqm) AS replaced_sqm, SUM(new_sqm) AS new_sqm, 
		SUM(c_sqm) AS c_sqm, MAX(d_sqm) AS d_sqm, -- SUM(d_sqm) AS d_sqm 
		SUM(c_footprint) AS c_footprint, SUM(d_footprint) AS d_footprint, 
		COUNT(d_sqm) AS count, 
		ST_SetSRID(ST_GeomFromText(MIN(ST_AsText(pand_geom))), 28992) AS pand_geom
	FROM constructions_replacement_raw
	GROUP BY d_id_pand 
), 

-- make housing_nl_s1 with new statuses 
renovations AS (
	SELECT 
		id_pand, -- id_pand of demolished building, which is now being renovated instead 
		'renovation - s1' AS status
	FROM constructions_replacement
	WHERE replaced_sqm != 0 
		-- AND new_sqm <= replaced_sqm 
		AND c_sqm BETWEEN d_sqm * 0.8 AND d_sqm * 1.2
		-- AND c_sqm <= d_sqm 
), 
housing_nl AS (
	SELECT * 
	FROM housing_nl 
	WHERE municipality = 'Delft' 
), 

-- replace demolitions with renovations (when applicable)
housing_s1_with_renovations AS (
	SELECT 
		hr.municipality, hr.wk_code, hr.bu_code, hr.id_pand, hr.pand_geom, 
	    COALESCE(hs.status, hr.status) AS status, -- Use status 'renovation - s1' if exists, else keep original status
		hr.function, hr.sqm, hr.build_year, 
		hr.document_date, hr.document_number, hr.registration_start, hr.registration_end
	FROM housing_nl hr
	LEFT JOIN renovations hs
	ON hr.id_pand = hs.id_pand
), 

-- update status for constructions that were replaced
new_renovations AS (
	SELECT * FROM housing_s1_with_renovations WHERE status = 'renovation - s1'
), 
constructions_replaced AS (
	SELECT DISTINCT c_id_pand AS id_pand, 'construction - invalid' AS status 
	FROM constructions_replacement_raw a 
	JOIN new_renovations b 
	ON a.d_id_pand = b.id_pand 
), 
housing_s1_with_constructions AS (
	SELECT 
	    a.municipality, a.wk_code, a.bu_code, a.id_pand, a.pand_geom, 
	    COALESCE(b.status, a.status) AS status, -- Use status 'construction - invalid' if exists, else keep original status
	    a.function, a.sqm, a.build_year, 
	    a.document_date, a.document_number, a.registration_start, a.registration_end
	FROM housing_s1_with_renovations a
	LEFT JOIN constructions_replaced b
	ON a.id_pand = b.id_pand
)

SELECT 
	CASE 
		WHEN registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
		ELSE LEFT(registration_start, 4)::INTEGER 
	END AS year, 
	* 
FROM housing_s1_with_constructions 
-- WHERE status = 'construction - invalid'