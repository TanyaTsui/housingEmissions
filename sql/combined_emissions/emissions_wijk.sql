INSERT INTO emissions_all_wijk (
    municipality, wk_code, year, construction, renovation, transformation, demolition, 
    population, n_households, n_homes, gas_m3, elec_kwh, av_p_stadverw, av_woz, 
    embodied_kg, operational_kg, geom
)

-- wijk level construction events (input for embodied emissions) 
WITH construction_events_raw AS (
	SELECT 
		CASE
	        WHEN status = 'Bouw gestart' THEN LEFT(registration_end, 4)::INTEGER
	        ELSE LEFT(registration_start, 4)::INTEGER
	    END AS year, 
		CASE
			WHEN status = 'Bouw gestart' THEN 'construction'
			WHEN status = 'Pand gesloopt' THEN 'demolition'
			WHEN status IN ('renovation - post2020', 'renovation - pre2020') THEN 'renovation'
			WHEN status IN ('transformation - adding units', 'transformation - function change') THEN 'transformation'
		END AS status, 
		id_pand, n_units, sqm, neighborhood_code, wk_code, municipality 
	FROM housing_nl
	WHERE municipality = 'Delft'
), 
construction_events_wijk AS (
	SELECT 
	    municipality, wk_code, year,
	    SUM(CASE WHEN status = 'construction' THEN sqm ELSE 0 END) AS construction,
	    SUM(CASE WHEN status = 'renovation' THEN sqm ELSE 0 END) AS renovation,
		SUM(CASE WHEN status = 'transformation' THEN sqm ELSE 0 END) AS transformation,
	    SUM(CASE WHEN status = 'demolition' THEN sqm ELSE 0 END) AS demolition
	FROM construction_events_raw
	WHERE year >= 2012
		AND year <= 2021
	GROUP BY municipality, wk_code, year
), 

-- wijk level energy use (input for operational emissions) 
energy_wijk AS (
	SELECT * FROM cbs_map_all_wijk WHERE municipality = 'Delft'
), 

-- calculate emissions 
wijk_stats AS (
	SELECT a.*, b.population, b.n_households, b.n_homes, b.gas_m3, b.elec_kwh, b.av_p_stadverw, b.av_woz
	FROM construction_events_wijk a 
	JOIN energy_wijk b 
	ON a.year = b.year AND a.wk_code = b.wk_code 
), 
emissions_wijk AS (
	SELECT a.*, 
		a.construction * 316 + a.renovation * 126 + a.transformation * 126 + a.demolition * 126 AS embodied_kg, 
		a.gas_m3 * 1.9 + elec_kwh * 0.45 AS operational_kg, 
		b.geom
	FROM wijk_stats a 
	LEFT JOIN cbs_wijk_2012 b 
	ON a.wk_code = b.wk_code
)

SELECT * FROM emissions_wijk 




