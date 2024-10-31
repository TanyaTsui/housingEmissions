WITH emissions_per_neighborhood AS (
	SELECT municipality, neighborhood_code, 
		SUM(emissions_operational_kg) AS emissions_operational_kg, 
		SUM(emissions_embodied_kg) AS emissions_embodied_kg
	FROM new_housing_emissions 
	GROUP BY municipality, neighborhood_code
), 
new_sqm AS (
	SELECT municipality, neighborhood_code, id_pand, MIN(sqm) AS sqm
	FROM new_housing_emissions 
	GROUP BY municipality, neighborhood_code, id_pand
), 
new_sqm_per_neighborhood AS (
	SELECT municipality, neighborhood_code, SUM(sqm) AS sqm 
	FROM new_sqm 
	GROUP BY municipality, neighborhood_code
), 

neighborhood_stats AS (
	SELECT b.*, a.sqm AS sqm_new_housing, 
		ROUND(b.emissions_operational_kg / a.sqm) AS operational_per_sqm, 
		ROUND(b.emissions_embodied_kg / a.sqm) AS embodied_per_sqm
	FROM new_sqm_per_neighborhood a 
	LEFT JOIN emissions_per_neighborhood b 
	ON a.neighborhood_code = b.neighborhood_code
), 
municipality_stats AS (
	SELECT municipality, SUM(sqm_new_housing) AS sqm_new_housing, 
		SUM(emissions_operational_kg) AS emissions_operational_kg, 
		SUM(emissions_embodied_kg) AS emissions_embodied_kg
	FROM neighborhood_stats 
	GROUP BY municipality
)

SELECT *, 
	ROUND(emissions_operational_kg / sqm_new_housing) AS operational_per_sqm, 
	ROUND(emissions_embodied_kg / sqm_new_housing) AS embodied_per_sqm
FROM municipality_stats





-- -- change in n_households from 2012-2021 
-- hh_2012 AS (
--     SELECT neighborhood_code, n_households AS hh_2012, municipality
--     FROM cbs_map_all
--     WHERE year = 2012
-- ),
-- hh_2021 AS (
--     SELECT neighborhood_code, n_households AS hh_2021, municipality
--     FROM cbs_map_all
--     WHERE year = 2021
-- ), 
-- hh_change AS (
-- 	SELECT
-- 	    COALESCE(hh_2021.municipality, hh_2012.municipality) AS municipality,
-- 	    COALESCE(hh_2021.neighborhood_code, hh_2012.neighborhood_code) AS neighborhood_code,
-- 	    (hh_2021.hh_2021 - hh_2012.hh_2012) AS change_hh, 
-- 		hh_2012.hh_2012, hh_2021.hh_2021
-- 	FROM hh_2012
-- 	FULL OUTER JOIN hh_2021
-- 	    ON hh_2012.neighborhood_code = hh_2021.neighborhood_code
-- 	-- WHERE hh_2012.hh_2012 IS NOT NULL AND hh_2021.hh_2021 IS NOT NULL
-- ), 

-- neighborhood_stats AS (
-- 	SELECT e.*, h.change_hh, h.hh_2012, h.hh_2021, 
-- 		ROUND(e.emissions_operational_kg / h.change_hh) AS operational_perhome, 
-- 		ROUND(e.emissions_embodied_kg / h.change_hh) AS embodied_perhome
-- 	FROM emissions_per_neighborhood e 
-- 	LEFT JOIN hh_change h
-- 	ON 
-- 		h.municipality = e.municipality 
-- 		AND h.neighborhood_code = e.neighborhood_code 
-- 	WHERE h.change_hh IS NOT NULL
-- 		AND h.change_hh != 0
-- 	-- TODO: check why 17% of rows have NULL values 
-- ), 

-- municipality_stats AS (
-- 	SELECT municipality, 
-- 		SUM(change_hh) AS change_hh, 
-- 		SUM(emissions_operational_kg) AS operational_kg_total, 
-- 		SUM(emissions_embodied_kg) AS embodied_kg_total
-- 	FROM neighborhood_stats 
-- 	GROUP BY municipality
-- )

-- SELECT *, 
-- 	ROUND(operational_kg_total / change_hh) AS operational_perhome, 
-- 	ROUND(embodied_kg_total / change_hh) AS embodied_perhome
-- FROM municipality_stats
