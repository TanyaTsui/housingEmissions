-- add 2022 admin boundaries using spatial join
WITH buurt2022_municipality AS (
	SELECT "GM_NAAM" AS municipality, "WK_CODE" AS wk_code, 
		"BU_CODE" AS bu_code, geometry AS bu_geom
	FROM cbs_map_2022 
	WHERE "WATER" = 'NEE' AND "GM_NAAM" = 'Delft' 
), 
bag_vbo_with_admin_boundaries AS (
	SELECT a.id_vbo, a.id_num, a.id_pand, a.function, a.sqm, a.status, 
		a.document_date, a.document_number, a.registration_start, a.registration_end, 
		a.vbo_geom, b.*
	FROM bag_vbo a 
	JOIN buurt2022_municipality b 
	ON a.vbo_geom && b.bu_geom
		AND ST_Within(a.vbo_geom, b.bu_geom)
), 
bag_vbo_final AS (
	SELECT 
		CASE 
			WHEN registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
			ELSE LEFT(registration_start, 4)::INTEGER
		END AS year, *
	FROM bag_vbo_with_admin_boundaries
)

UPDATE bag_vbo t
SET 
    year = f.year,
    municipality = f.municipality,
    wk_code = f.wk_code,
    bu_code = f.bu_code,
    bu_geom = f.bu_geom
FROM bag_vbo_final f
WHERE t.id_vbo = f.id_vbo
	AND t.document_date = f.document_date; 