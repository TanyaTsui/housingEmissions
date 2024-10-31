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
		new_construction, rebuilt, renovation AS renovated, 
		ROUND(new_construction/in_use*100, 3) AS new_construction_p, 
		ROUND(rebuilt/in_use*100, 3) AS rebuilt_p,
		ROUND(renovation/in_use*100, 3) AS renovated_p
	FROM construction_stats
), 

-- add lagged variables
lagged_data AS (
	SELECT 
		neighborhood_code, year, in_use, 
		new_construction, rebuilt, renovated,
		LAG(new_construction) OVER (PARTITION BY neighborhood_code ORDER BY year) AS lag_new_construction,
		LAG(rebuilt) OVER (PARTITION BY neighborhood_code ORDER BY year) AS lag_rebuilt,
		LAG(renovated) OVER (PARTITION BY neighborhood_code ORDER BY year) AS lag_renovated
	FROM percentages
),

-- add energy_use
gas AS (
	SELECT neighborhood_code, year, g_gas_tot * aantal_hh AS gas, p_stadverw
	FROM cbs_map_all_unified_buurt
	WHERE g_gas_tot IS NOT NULL
		-- AND p_stadverw > 0
)

SELECT 
	g.neighborhood_code, g.year, g.gas, 
	ROUND(g.gas/p.in_use,3) AS gas_per_sqm, g.p_stadverw, 
	p.new_construction_p, p.rebuilt_p, p.renovated_p, p.in_use, 
	p.new_construction, p.rebuilt, p.renovated
FROM gas g 
JOIN percentages p 
ON g.neighborhood_code = p.neighborhood_code
	AND g.year = p.year
WHERE p.new_construction_p > 100

-- SELECT 
-- 	g.neighborhood_code, g.year, g.gas, 
-- 	ROUND(g.gas/l.in_use,3) AS gas_per_sqm, g.p_stadverw, 
-- 	l.in_use, l.new_construction, l.rebuilt, l.renovated,
-- 	l.lag_new_construction, l.lag_rebuilt, l.lag_renovated
-- FROM gas g 
-- JOIN lagged_data l 
-- ON g.neighborhood_code = l.neighborhood_code
-- 	AND g.year = l.year
-- WHERE l.lag_new_construction IS NOT NULL 
-- 	AND l.lag_rebuilt IS NOT NULL 
-- 	AND l.lag_renovated IS NOT NULL
-- 	AND l.new_construction > l.in_use


