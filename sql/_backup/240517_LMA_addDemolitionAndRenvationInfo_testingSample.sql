/* 

ADDING RENOVATION AND DEMOLITION INFO TO CDWASTE 

This query takes ~1 hour to run. It does the following: 
- filters out demolished buildings from bag_pand
- filters out renovated buildings from bag_vbo 
- adds 'demolition' and 'renovation' columns in cdwaste

The resulting table is saved in the database as 'cdwaste'. 
It contains the following information: 
- cdwaste info from LMA 
- whether waste flow comes from demolition, and associated building info if TRUE 
- whether waste flow comes from renovation, and associated building info if TRUE 

*/

DROP TABLE IF EXISTS cdwaste_demorenoinfo 
CREATE TABLE cdwaste_demorenoinfo AS 

-- make demolished_buildings from bag_pand 
WITH demolished_buildings AS (
	SELECT 
		id_pand, build_year, status, document_date, registration_start, registration_end,
		ST_Centroid(geom) AS geom
	FROM bag_pand
	WHERE status = 'Pand gesloopt'
), 

-- add demolition column to cdwaste table 
ranked_demolitions AS (
    SELECT 
        w.*, 
        b.id_pand AS demolition_id_pand, b.geom AS demolition_geom, b.document_date AS demolition_date,
        ST_Distance(ST_Transform(w.geom, 28992), ST_Transform(b.geom, 28992)) AS demolition_distance,
        ROW_NUMBER() OVER (PARTITION BY w.address, w."wasteCode", w."wasteName", w."wasteDesc", w.kg, w.year, w.geom ORDER BY ST_Distance(ST_Transform(w.geom, 28992), ST_Transform(b.geom, 28992))) AS rn
    FROM 
        cdwaste w
    LEFT JOIN 
        demolished_buildings b
    ON 
        ST_DWithin(
            ST_Transform(w.geom, 28992), 
            ST_Transform(b.geom, 28992), 
            100
        )
), 
cdwaste_demolitions AS (
	SELECT
	    address, "wasteCode", "wasteName", "wasteDesc", kg, year, geom,
	    demolition_id_pand, demolition_geom, demolition_date,
	    CASE WHEN demolition_id_pand IS NOT NULL THEN TRUE ELSE FALSE END AS demolition
	FROM 
	    ranked_demolitions
	WHERE
	    rn = 1
), 

-- make renovated_buildings from bag_vbo 
renovated_buildings AS (
	SELECT 
		MAX(id_vbo) AS renovation_id_vbo, 
		id_pand AS renovation_id_pand, 
		MAX(status) AS renovation_status, 
		MAX(document_date) AS renovation_document_date, 
		MAX(registration_start) AS renovation_registration_start, 
		MAX(registration_end) AS renovation_registration_end, 
		ST_Union(geom) AS renovation_geom 
	FROM bag_vbo
	WHERE 
		CAST(sqm AS INTEGER) < 999999 AND 
		status = 'Verbouwing verblijfsobject'
	GROUP BY id_pand 
), 

-- add "renovation" column to cdwaste table 
ranked_renovations AS (
    SELECT 
        w.address, w."wasteCode", w."wasteName", w."wasteDesc", w.kg, w.year, w.geom, 
        w.demolition, w.demolition_id_pand, w.demolition_geom, w.demolition_date,
        b.renovation_id_pand, b.renovation_geom, b.renovation_document_date,
		b.renovation_registration_start, b.renovation_registration_end,
        ST_Distance(ST_Transform(w.geom, 28992), 
		ST_Transform(b.renovation_geom, 28992)) AS renovation_distance,
        ROW_NUMBER() OVER (PARTITION BY w.address, w."wasteCode", w."wasteName", w."wasteDesc", w.kg, w.year, w.geom ORDER BY ST_Distance(ST_Transform(w.geom, 28992), ST_Transform(b.renovation_geom, 28992))) AS rn
    FROM 
        cdwaste_demolitions w
    LEFT JOIN 
        renovated_buildings b
    ON 
        ST_DWithin(
            ST_Transform(w.geom, 28992), 
            ST_Transform(b.renovation_geom, 28992), 
            100
        )
), 
cdwaste_final AS (
	SELECT
	    address, "wasteCode", "wasteName", "wasteDesc", kg, year, geom AS w_geom,
	    demolition, demolition_id_pand, demolition_geom, demolition_date,
	    CASE WHEN renovation_id_pand IS NOT NULL THEN TRUE ELSE FALSE END AS renovation,
	    renovation_id_pand, renovation_geom, renovation_document_date, 
		renovation_registration_start, renovation_registration_end 
	FROM 
	    ranked_renovations
	WHERE
	    rn = 1
)

-- match demolition / renovation dates with LMA date 
WITH cdwaste_final_filtered AS (
    SELECT 
		*, 
		CASE
			WHEN 
				demolition = TRUE AND 
				(CAST(SUBSTRING(demolition_date, 1, 4) AS INTEGER) <= year + 1 AND 
				CAST(SUBSTRING(demolition_date, 1, 4) AS INTEGER) >= year - 1) 
			THEN TRUE ELSE FALSE
			END AS demolition_real, 
		CASE 
			WHEN renovation = TRUE AND 
				(CAST(SUBSTRING(renovation_registration_start, 1, 4) AS INTEGER) <= year + 1 AND 
				CAST(SUBSTRING(renovation_registration_end, 1, 4) AS INTEGER) > year - 1) THEN TRUE
			ELSE FALSE 
			END AS renovation_real
    FROM cdwaste_final
)
	
SELECT * 
FROM cdwaste_final_filtered; 