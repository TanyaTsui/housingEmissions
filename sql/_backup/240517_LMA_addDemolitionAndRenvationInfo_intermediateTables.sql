/* 

ADDING RENOVATION AND DEMOLITION INFO TO CDWASTE 

This query should take ~20 hours to run. It does the following: 
- filters out demolished buildings from bag_pand
- filters out renovated buildings from bag_vbo 
- adds 'demolition' and 'renovation' columns in cdwaste

The resulting table is saved in the database as 'cdwaste'. 
It contains the following information: 
- cdwaste info from LMA 
- whether waste flow comes from demolition, and associated building info if TRUE 
- whether waste flow comes from renovation, and associated building info if TRUE 

*/

DROP TABLE IF EXISTS cdwaste_demolitions;
CREATE TABLE cdwaste_demolitions AS 
-- make demolished_buildings from bag_pand 
WITH demolished_buildings AS (
	SELECT 
		id_pand, build_year, status, document_date, registration_start, registration_end,
		ST_Centroid(geom_28992) AS geom_28992
	FROM bag_pand
	WHERE 
		status = 'Pand gesloopt' AND 
		CAST(SUBSTRING(document_date, 1, 4) AS INTEGER) >= 2018 AND 
		CAST(SUBSTRING(document_date, 1, 4) AS INTEGER) <= 2021 
), 
-- add demolition column to cdwaste table 
ranked_demolitions AS (
    SELECT 
        w.*, 
        b.id_pand AS demolition_id_pand, b.geom_28992 AS demolition_geom, b.document_date AS demolition_date,
        ROW_NUMBER() OVER (
			PARTITION BY w.address, w."wasteCode", w."wasteName", w."wasteDesc", w.kg, w.year, w.geom 
			ORDER BY ST_Distance(w.geom_28992, b.geom_28992)) AS rn
    FROM cdwaste w
    LEFT JOIN demolished_buildings b
    ON ST_DWithin(w.geom_28992, b.geom_28992, 100)
), 
cdwaste_demolitions AS (
	SELECT
	    address, "wasteCode", "wasteName", "wasteDesc", kg, year, geom, geom_28992, 
	    demolition_id_pand, demolition_geom, demolition_date,
	    CASE WHEN demolition_id_pand IS NOT NULL THEN TRUE ELSE FALSE END AS demolition
	FROM 
	    ranked_demolitions
	WHERE
	    rn = 1
)
SELECT * FROM cdwaste_demolitions; 

-- add renovation info to cdwaste 
DROP TABLE IF EXISTS cdwaste_final;
CREATE TABLE cdwaste_final AS 
	
-- make renovated_buildings from bag_vbo 
WITH renovated_buildings AS (
	SELECT 
		MAX(id_vbo) AS renovation_id_vbo, 
		id_pand AS renovation_id_pand, 
		MAX(status) AS renovation_status, 
		MAX(document_date) AS renovation_document_date, 
		MAX(registration_start) AS renovation_registration_start, 
		MAX(registration_end) AS renovation_registration_end, 
		ST_Union(geom_28992) AS renovation_geom_28992
	FROM bag_vbo
	WHERE 
		CAST(sqm AS INTEGER) < 999999 AND 
		status = 'Verbouwing verblijfsobject' AND 
		CAST(SUBSTRING(document_date, 1, 4) AS INTEGER) >= 2018 AND 
		CAST(SUBSTRING(document_date, 1, 4) AS INTEGER) <= 2021 
	GROUP BY id_pand 
), 

-- add "renovation" column to cdwaste table 
ranked_renovations AS (
    SELECT 
        w.address, w."wasteCode", w."wasteName", w."wasteDesc", w.kg, w.year, w.geom, w.geom_28992, 
        w.demolition, w.demolition_id_pand, w.demolition_geom, w.demolition_date,
        b.renovation_id_pand, b.renovation_geom_28992, b.renovation_document_date,
		b.renovation_registration_start, b.renovation_registration_end,
        ROW_NUMBER() OVER (
			PARTITION BY w.address, w."wasteCode", w."wasteName", w."wasteDesc", w.kg, w.year, w.geom 
			ORDER BY ST_Distance(w.geom_28992, b.renovation_geom_28992)) AS rn
    FROM cdwaste_demolitions w
    LEFT JOIN renovated_buildings b
    ON ST_DWithin(w.geom_28992, b.renovation_geom_28992, 100)
), 
cdwaste_final AS (
	SELECT
	    address, "wasteCode", "wasteName", "wasteDesc", kg, year, 
		geom AS w_geom, geom_28992 AS w_geom_28992, 
	    demolition, demolition_id_pand, demolition_geom, demolition_date,
	    CASE WHEN renovation_id_pand IS NOT NULL THEN TRUE ELSE FALSE END AS renovation,
	    renovation_id_pand, renovation_geom_28992, renovation_document_date, 
		renovation_registration_start, renovation_registration_end 
	FROM 
	    ranked_renovations
	WHERE
	    rn = 1
)

SELECT * FROM cdwaste_final; 

-- match demolition / renovation dates with LMA date 
DROP TABLE IF EXISTS cdwaste_final_filtered;
CREATE TABLE cdwaste_final_filtered AS 
	
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