-- calculate new_construction and rebuilt
WITH construction_stats AS (
	SELECT *, 
		CASE 
			WHEN construction = 0 THEN 0
			WHEN construction >= demolition THEN construction - demolition
			WHEN construction < demolition THEN 0
		END AS new_construction, 
		CASE 
			WHEN construction = 0 THEN 0
			WHEN construction >= demolition THEN demolition
			WHEN construction < demolition THEN construction
		END AS rebuilt
	FROM housing_bybuurt
), 

-- calculate percentage for new_construction, rebuilt, and renovated 
percentages AS (
	SELECT neighborhood_code, year, in_use, 
		ROUND(new_construction/in_use*100, 3) AS new_construction, 
		ROUND(rebuilt/in_use*100, 3) AS rebuilt,
		ROUND(renovation/in_use*100, 3) AS renovated
	FROM construction_stats
), 

-- add energy_use
gas AS (
	SELECT neighborhood_code, year, g_gas_tot AS gas
	FROM cbs_map_all_unified_buurt
	WHERE g_gas_tot IS NOT NULL
)

SELECT 
	g.neighborhood_code, g.year, 
	ROUND(g.gas/p.in_use,3) AS gas_per_sqm, 
	p.new_construction, p.rebuilt, p.renovated
FROM gas g 
JOIN percentages p 
ON g.neighborhood_code = p.neighborhood_code
	AND g.year = p.year


