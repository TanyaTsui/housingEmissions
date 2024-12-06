WITH year_count AS (
	SELECT municipality, bu_code, COUNT(*) AS n_years 
	FROM cbs_map_all_buurt -- WHERE municipality = 'Venlo'
	GROUP BY municipality, bu_code
	ORDER BY municipality, bu_code
)

SELECT ST_Transform(bu_geom, 4326), * 
FROM year_count a
JOIN (SELECT DISTINCT ON (bu_code) * FROM cbs_map_all_buurt) b 
ON a.bu_code = b.bu_code
WHERE a.n_years < 10


