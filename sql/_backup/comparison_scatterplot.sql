/*
MAKE COMPARISON SCATTERPLOT
Create scatterplot that compares waste predictions from LMA waste data vs archetype method
- create geom from text for bag_20190101 
- match buildings from bag that overlap with LMA waste data (spatial join)
- estimate timber waste from selected buildings 
- compare these values with LMA data 
*/ 

-- -- create test sample
-- DROP TABLE IF EXISTS bag_20190101_sample; 
-- CREATE TABLE bag_20190101_sample AS SELECT * FROM bag_20190101 LIMIT 10000;
-- DROP TABLE IF EXISTS timberWaste_sample; 
-- CREATE TABLE timberWaste_sample AS SELECT * FROM public."timberWaste" LIMIT 10000;

-- create new table for scatterplot comparison
DROP TABLE IF EXISTS comparison_lma_bag;
CREATE TABLE comparison_lma_bag AS

-- estimate timber waste using archetype method, by combining bag snapshot with archetypes 
WITH vbo_timber_archetypes AS (
	SELECT 
		vbo.*, archetypes.*, 
		(archetypes."wood_kgPerM2"::numeric * vbo.sqm::numeric) AS kg_archetype
	FROM bag_20190101 AS vbo 
	LEFT JOIN archetypes_timber AS archetypes  
	ON vbo.function = archetypes.kadaster_type 
		AND vbo.build_year::integer >= archetypes."buildYear_cohort_start"
		AND vbo.build_year::integer <= archetypes."buildYear_cohort_end"
), 

pand_timber_archetypes AS(
	SELECT 
		id_pand, 
		(array_agg(geom))[1] AS geom,
		(array_agg(status))[1] AS status,
		(array_agg(build_year))[1] AS build_year,
		(array_agg("buildYear_cohort_start"))[1] AS buildYear_cohort_start,
		(array_agg("buildYear_cohort_end"))[1] AS buildYear_cohort_end,
		AVG("wood_kgPerM2") AS wood_kgPerM2_average,
		(string_agg(DISTINCT function, ', ')) AS function, 
		SUM(sqm::numeric) AS sqm, 
		SUM(kg_archetype) AS kg_archetype, 
		COUNT(id_vbo) AS num_vbos
	FROM vbo_timber_archetypes
	GROUP BY id_pand 
), 

-- group LMA data by address 
lma AS (
	SELECT 
		address, 
		SUM(kg) AS kg, 
		(array_agg("wasteName"))[1] AS wastename,
		(array_agg(geom))[1] AS geom
	FROM public."timberWaste"
	GROUP BY address 
)

-- - match buildings that overlap with LMA waste data (spatial join)
SELECT 
	lma.address, lma.wastename, lma.geom AS geom_lma, lma.kg AS kg_lma, 
	b.kg_archetype, b.sqm, b.num_vbos, b.function, b.build_year, 
	b.id_pand, b.geom AS geom_pand, b.status
FROM lma
JOIN pand_timber_archetypes AS b
ON ST_Within(lma.geom, b.geom)
