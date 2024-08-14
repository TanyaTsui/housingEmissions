-- TODO: change date range to dynamic, update with new LMA data 
WITH buildings AS (
	SELECT *
	FROM bag_pand
	WHERE 
		CAST(document_date AS DATE) BETWEEN '2018-01-01' AND '2021-12-31' AND 
		status NOT IN ('Niet gerealiseerd pand', 'Pand ten onrechte opgevoerd')
), 
redeveloped_buildings AS (
	SELECT *
	FROM buildings
	WHERE status IN ('Pand gesloopt', 'Verbouwing pand')
), 
units AS (
	SELECT *,
           ST_Transform(ST_SetSRID(ST_MakePoint(SPLIT_PART(geometry, ' ', 1)::float,
                                               SPLIT_PART(geometry, ' ', 2)::float), 28992), 4326) AS geom
	FROM bag_vbo
		WHERE 
			CAST(document_date AS DATE) BETWEEN '2018-01-01' AND '2021-12-31' AND 
			status NOT IN ('Niet gerealiseerd verblijfsobject', 'Verblijfsobject ten onrechte opgevoerd')
), 
redeveloped_units AS (
	SELECT *
	FROM units
	WHERE status = 'Verbouwing verblijfsobject'
), 

-- Join demolished buildings with bag_vbo, and renovated units with bag_pand
redeveloped_all AS (
    SELECT 
        rb.id_pand AS id_pand, 
        rb.build_year AS build_year, 
        rb.status AS status, 
        rb.document_date AS document_date, 
        rb.registration_start AS registration_start, 
        rb.registration_end AS registration_end,
        units.id_vbo AS id_vbo, 
        units.id_num AS id_num, 
        units.geom AS geom, 
        units.function AS function, 
        units.sqm AS sqm, 
        units.status AS unit_status, 
        units.document_date AS unit_document_date, 
        units.registration_start AS unit_registration_start, 
        units.registration_end AS unit_registration_end
    FROM redeveloped_buildings rb
    JOIN units ON rb.id_pand = units.id_pand
    UNION ALL
    SELECT 
        buildings.id_pand AS id_pand, 
        buildings.build_year AS build_year, 
        buildings.status AS status, 
        buildings.document_date AS document_date, 
        buildings.registration_start AS registration_start, 
        buildings.registration_end AS registration_end,
        ru.id_vbo AS id_vbo, 
        ru.id_num AS id_num, 
        ru.geom AS geom, 
        ru.function AS function, 
        ru.sqm AS sqm, 
        ru.status AS unit_status, 
        ru.document_date AS unit_document_date, 
        ru.registration_start AS unit_registration_start, 
        ru.registration_end AS unit_registration_end
    FROM redeveloped_units ru
    JOIN buildings ON ru.id_pand = buildings.id_pand
)

SELECT *
FROM redeveloped_all 
WHERE 
	status = 'Pand gesloopt' AND
	unit_status = 'Niet gerealiseerd verblijfsobject';
	
-- find rows where there's "verbouwing verblijfsobject" but not "verbouwing pand"

-- -- find LMA data that intersects with demolished buildings 
