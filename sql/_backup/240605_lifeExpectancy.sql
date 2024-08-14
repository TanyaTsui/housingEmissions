-- DROP TABLE IF EXISTS life_expectancy; 
-- CREATE TABLE life_expectancy AS 

-- WITH life_expectancy_all AS (
-- 	SELECT demolition_year, AVG(age) AS life_expectancy, COUNT(*) AS row_count
-- 	FROM demolished_buildings_ams
-- 	WHERE demolition_year > 2010
-- 	GROUP BY demolition_year
-- 	ORDER BY demolition_year 
-- ), 
-- -- life_expectancy_function AS (
	
-- -- ), 
-- -- life_expectancy_buildYear AS (
	
-- -- ), 
-- life_expectancy_age AS (
-- 	SELECT age, 
-- )

SELECT * FROM demolished_buildings_ams 