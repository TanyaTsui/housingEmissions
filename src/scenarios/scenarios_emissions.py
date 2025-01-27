import sys
from src.data_processing._common.query_runner import QueryRunner
from src.data_processing._common.database_manager import DatabaseManager
from src.data_processing._common.query_term_replacer import QueryTermReplacer

class s1EnergyEfficiency(): 
    def __init__(self, start_year, end_year):
        self.db_manager = DatabaseManager()
        self.conn = self.db_manager.connect()
        self.cursor = self.conn.cursor()
        self.municipalities = self.db_manager.get_municipalities_list()
        self.start_year = start_year
        self.end_year = end_year

    def run(self): 
        # create table if it doesn't already exist
        QueryRunner('sql/create_table/emissions_all_buurt_s1.sql').run_query() 

        print(f'\n\nCalculating emissions for scenario 1 - energy efficiency (period: {self.start_year} t/m {self.end_year}) ...')
        for year in range(self.start_year, self.end_year + 1):
            self.year = year
            for i, municipality in enumerate(self.municipalities): 
                query = self.make_query(self.year) 
                n_placeholders = QueryTermReplacer().counter(query)
                output = f"\rYear ({self.year - self.start_year + 1}/{self.end_year - self.start_year + 1}): {self.year} | Municipality ({i+1}/{len(self.municipalities)}): {municipality}                         "
                sys.stdout.write(output)
                sys.stdout.flush()
                self.cursor.execute(query, (municipality,) * n_placeholders)
                self.conn.commit()

    def make_query(self, year):
        return f''' 
            DELETE FROM emissions_all_buurt_s1 WHERE year = {self.year} AND municipality = %s;
            INSERT INTO emissions_all_buurt_s1 (
                year, municipality, wk_code, bu_code, bu_geom, population, woz, n_homes, inuse, 
                construction, transformation, renovation, demolition,
                gas_m3_s0, gas_m3_s1, electricity_kwh_s0, electricity_kwh_s1, 
                operational_kg_s0, operational_kg_s1, embodied_kg_s0, embodied_kg_s1
            )

            -- get buurt_stats: buurt level energy use and in-use sqm data 
            WITH cbs_stats_buurt AS (
                SELECT * FROM cbs_map_all_buurt WHERE municipality = %s AND year = {self.year}
            ), 
            housing_inuse AS (
                SELECT * FROM housing_inuse_2012_2021 WHERE municipality = %s AND year = {self.year}
            ), 
            housing_inuse_buurt AS (
                SELECT municipality, bu_code, year, SUM(sqm) AS sqm, SUM(n_units) AS n_units
                FROM housing_inuse
                GROUP BY municipality, bu_code, year
            ), 
            buurt_stats AS (
                SELECT b.*, a.sqm
                FROM housing_inuse_buurt a 
                JOIN cbs_stats_buurt b 
                ON a.municipality = b.municipality 
                    AND a.year = b.year
                    AND a.bu_code = b.bu_code 
            ), 

            -- get all constructions and renovations that happened before year  
            construction_municipality AS ( -- all construction activity in year
                SELECT id_pand, 
                    CASE 
                        WHEN registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
                        ELSE LEFT(registration_start, 4)::INTEGER
                    END AS year, 
                    status, sqm, n_units, pand_geom, bu_code, wk_code, municipality
                FROM housing_nl
                WHERE municipality = %s
                    AND ahn_version IS NULL
            ), 
            construction_sample AS (
                SELECT * FROM construction_municipality 
                WHERE year <= {self.year}
            ), 

            -- identify in-use buildings that were previously constructed or renovated (low-energy)
            inuse_lowenergy AS (
                SELECT 
                    b.municipality, b.bu_code, b.id_pand, b.year, 
                    'Pand in gebruik - low energy' AS status, 
                    b.tot_gas_m3, b.tot_elec_kwh, 
                    b.sqm, b.n_units
                FROM (SELECT DISTINCT ON (id_pand) id_pand, status FROM construction_sample) a 
                LEFT JOIN housing_inuse b 
                ON a.id_pand = b.id_pand
                WHERE a.status != 'Pand gesloopt'
                    AND b.id_pand IS NOT NULL
            ), 
            inuse_normalenergy AS (
                SELECT 
                    b.municipality, b.bu_code, b.id_pand, 
                    b.year, 'Pand in gebruik' AS status, 
                    b.tot_gas_m3, b.tot_elec_kwh, 
                    b.sqm, b.n_units
                FROM construction_sample a 
                RIGHT JOIN housing_inuse b 
                ON a.id_pand = b.id_pand
                WHERE a.id_pand IS NULL 
            ), 
            buildings_all AS (
                -- all construction / renovation / transformation / demolition activity in year
                SELECT municipality, bu_code, id_pand, year, status, 
                    0 AS tot_gas_m3, 0 AS tot_elec_kwh, 
                    sqm, n_units
                FROM construction_sample 
                WHERE year = {self.year}
                
                UNION ALL 
                
                -- low energy in use buildings in year
                SELECT * FROM inuse_lowenergy
                
                UNION ALL 
                
                -- non-low energy in use buildings in year
                SELECT * FROM inuse_normalenergy
            ), 

            -- calculate energy use per building according to low or normal energy use status
            energy_use_per_building AS (
                SELECT 
                    COALESCE(a.municipality, b.municipality) AS municipality, b.wk_code, 
                    COALESCE(a.bu_code, b.bu_code) AS bu_code, b.bu_geom, 
                    a.id_pand, a.year, a.status, a.sqm, b.n_homes, b.population, b.woz, 
                
                    -- a.tot_gas_m3 AS gas_m3_s0, -- not needed, this is already in emissions_all_buurt
                    CASE
                        WHEN status = 'Pand in gebruik' THEN a.tot_gas_m3 
                        WHEN status = 'Pand in gebruik - low energy' AND a.sqm * 5 < a.tot_gas_m3  THEN a.sqm * 5
                        WHEN status = 'Pand in gebruik - low energy' AND a.sqm * 5 >= a.tot_gas_m3  THEN a.tot_gas_m3 
                        ELSE 0 
                    END AS gas_m3_s1,
                
                    -- a.tot_elec_kwh AS electricity_kwh_s0, 
                    CASE 
                        WHEN status IN ('Pand in gebruik', 'Pand in gebruik - low energy') THEN a.tot_elec_kwh 
                        ELSE 0 
                    END AS electricity_kwh_s1
                FROM buildings_all a 
                FULL JOIN buurt_stats b 
                ON a.bu_code = b.bu_code
            ), 
            emissions_per_building AS (
                SELECT 
                    municipality, wk_code, bu_code, bu_geom, id_pand, 
                    year, status, sqm, n_homes, population, woz,
                    -- gas_m3_s0, electricity_kwh_s0, 
                    -- ROUND(gas_m3_s0 * 1.9 + electricity_kwh_s0 * 0.45) AS operational_kg_s0, 	
                    gas_m3_s1, electricity_kwh_s1, 
                    ROUND(gas_m3_s1 * 1.9 + electricity_kwh_s1 * 0.45) AS operational_kg_s1, 
                    
                    CASE 
                        WHEN status IN ('transformation - adding units', 'transformation - function change', 
                                        'renovation - pre2020', 'renovation - post2020') THEN sqm * 126
                        WHEN status = 'Bouw gestart' THEN sqm * 325
                        WHEN status = 'Pand gesloopt' THEN sqm * 77
                        ELSE 0 
                    END AS embodied_kg_s0, 
                    CASE 
                        WHEN status IN ('transformation - adding units', 'transformation - function change', 
                                        'renovation - pre2020', 'renovation - post2020') THEN sqm * 200
                        WHEN status = 'Bouw gestart' THEN sqm * 550
                        WHEN status = 'Pand gesloopt' THEN sqm * 77
                        ELSE 0 
                    END AS embodied_kg_s1
                    
                FROM energy_use_per_building 
            ), 
            stats_per_building AS (
                SELECT 
                    CASE 
                        WHEN status IN ('Pand in gebruik', 'Pand in gebruik - low energy') THEN sqm ELSE 0
                    END AS inuse, 
                    CASE 
                        WHEN status = 'Bouw gestart' THEN sqm ELSE 0 
                    END AS construction, 
                    CASE 
                        WHEN status IN ('transformation - adding units', 'transformation - function change') THEN sqm ELSE 0
                    END AS transformation, 
                    CASE 
                        WHEN status IN ('renovation - pre2020', 'renovation - post2020') THEN sqm ELSE 0 
                    END AS renovation, 
                    CASE 
                        WHEN status = 'Pand gesloopt' THEN sqm ELSE 0 
                    END AS demolition, *
                FROM emissions_per_building 
            ), 
            s1_results AS (
                SELECT {self.year} AS year, municipality, wk_code, bu_code, bu_geom,  
                    ROUND(AVG(population)) AS population, ROUND(AVG(woz)) AS woz, 
                    
                    SUM(n_homes) AS n_homes, SUM(inuse) AS inuse, SUM(construction) AS construction, SUM(transformation) AS transformation, 
                    SUM(renovation) AS renovation, SUM(demolition) AS demolition, 
                    
                    -- ROUND(SUM(gas_m3_s0)) AS gas_m3_s0, 
                    ROUND(SUM(gas_m3_s1)) AS gas_m3_s1, 
                    -- ROUND(SUM(electricity_kwh_s0)) AS electricity_kwh_s0, 
                    ROUND(SUM(electricity_kwh_s1)) AS electricity_kwh_s1, 
                
                    -- SUM(operational_kg_s0) AS operational_kg_s0, 
                    SUM(operational_kg_s1) AS operational_kg_s1, 
                    SUM(embodied_kg_s0) AS embodied_kg_s0, SUM(embodied_kg_s1) AS embodied_kg_s1
                    
                FROM stats_per_building
                GROUP BY municipality, wk_code, bu_code, bu_geom
            ), 
            s0_results AS (
                SELECT bu_code, 
                    tot_gas_m3 AS gas_m3_s0, 
                    tot_elec_kwh AS electricity_kwh_s0, 
                    ROUND(embodied_kg) AS embodied_kg_s0, 
                    ROUND(operational_kg) AS operational_kg_s0 
                FROM emissions_all_buurt 
                WHERE municipality = %s AND year = {self.year}
            )

            SELECT 
                a.year, a.municipality, a.wk_code, a.bu_code, a.bu_geom, 
                a.population, a.woz, a.n_homes, a.inuse, 
                a.construction, a.transformation, a.renovation, a.demolition, 
                b.gas_m3_s0, a.gas_m3_s1, b.electricity_kwh_s0, a.electricity_kwh_s1, 
                b.operational_kg_s0, a.operational_kg_s1, b.embodied_kg_s0, a.embodied_kg_s1	
            FROM s1_results a 
            LEFT JOIN s0_results b
            ON a.bu_code = b.bu_code 
            '''

class s2CircularEconomy(): 
    def __init__(self, start_year, end_year):
        self.db_manager = DatabaseManager()
        self.conn = self.db_manager.connect()
        self.cursor = self.conn.cursor()
        self.municipalities = self.db_manager.get_municipalities_list()
        self.start_year = start_year
        self.end_year = end_year

    def run(self):         
        print('\n\nCalculating emissions for scenario 2 - circular economy ...')

        # create and fill housing_nl_s2 table
        QueryRunner('sql/create_table/housing_nl_s2.sql').run_query()
        QueryRunner('sql/s2_circular_economy/renovation_suitability.sql').run_query_for_each_municipality('Adding to housing_nl_s2...')

        # create and fill emissions_all_buurt_s2 table
        QueryRunner('sql/create_table/emissions_all_buurt_s2.sql').run_query()
        # for year in [2012]: 
        for year in range(self.start_year, self.end_year + 1):
            self.year = year     
            for i, municipality in enumerate(self.municipalities): 
                query = self.make_query() 
                n_placeholders = QueryTermReplacer().counter(query)
                output = f"\rYear ({self.year - self.start_year + 1}/{self.end_year - self.start_year + 1}): {self.year} | Municipality ({i+1}/{len(self.municipalities)}): {municipality}                         "
                sys.stdout.write(output)
                sys.stdout.flush()
                self.cursor.execute(query, (municipality,) * n_placeholders)
                self.conn.commit()

    def make_query(self):
        return f'''
            DELETE FROM emissions_all_buurt_s2
            WHERE municipality = %s AND year = {self.year};

            INSERT INTO emissions_all_buurt_s2 (
                year, municipality, wk_code, bu_code, bu_geom,
                construction, construction_s2, demolition, demolition_s2, 
                transformation, transformation_s2, renovation, renovation_s2, 
                operational_kg_s0, operational_kg_s1, operational_kg_s2,
                embodied_kg_s0, embodied_kg_s1, embodied_kg_s2,
                inuse, gas_m3_s0, gas_m3_s1, gas_m3_s2,
                electricity_kwh_s0, electricity_kwh_s1, electricity_kwh_s2,
                n_homes, population, woz
            )

            -- 1. Make record of in use buildings 
            -- add construction activity from housing_nl_s2 
            WITH housing_nl AS (
                SELECT bu_code, id_pand, status, function, sqm, pand_geom AS pd_geom, 
                    CASE 
                        WHEN registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
                        ELSE LEFT(registration_start, 4)::INTEGER
                    END AS year
                FROM housing_nl_s2
                WHERE municipality = %s 
            ), 
            inuse AS (
                SELECT * FROM housing_inuse_2012_2021 
                WHERE municipality = %s AND year = {self.year}
            ),  
            -- remove invalid in-use buildings (buildings where status = 'construction - invalid') check years before as well
            invalid_constructions AS (
                SELECT DISTINCT id_pand FROM housing_nl 
                WHERE status = 'construction - invalid' AND year <= {self.year}
            ), 
            inuse_without_invalid_constructions AS (
                SELECT * FROM inuse
                WHERE id_pand NOT IN (
                    SELECT id_pand FROM invalid_constructions 
                )
            ), 

            -- add in-use buildings where status = 'renovation - s1' (be sure to check years before as well)
            new_renovations AS (
                SELECT DISTINCT ON (id_pand) 
                    id_pand, sqm
                FROM housing_nl 
                WHERE status = 'renovation - s1' AND year <= {self.year}
            ), 
            inuse_with_new_renovations AS (
                SELECT 
                    {self.year} AS year, id_pand, 'in use - s' AS status, sqm, 
                    NULL AS tot_gas_m3, NULL AS tot_elec_kwh, 
                    bu_code, %s AS municipality 
                FROM housing_nl 
                WHERE status = 'renovation - s1'

                UNION ALL 

                SELECT 
                    {self.year} AS year, id_pand, 'in use' AS status, sqm, 
                    tot_gas_m3, tot_elec_kwh, 
                    bu_code, %s AS municipality 
                FROM inuse_without_invalid_constructions
            ), 

            -- 2. Calculate operational emissions using in-use buildings 
            -- find av_gas_per_sqm and av_elec_per_sqm for inuse buildings that don't have energy use data 
            energy_data_buurt AS (
                SELECT * FROM cbs_map_all_buurt
                WHERE municipality = %s AND year = {self.year}
            ), 
            inuse_sqm_per_buurt AS (
                SELECT bu_code, SUM(sqm) AS inuse_sqm FROM inuse GROUP BY bu_code
            ), 
            per_sqm_energy_use_for_buurt AS (
                SELECT a.bu_code, a.year, b.inuse_sqm, 
                    tot_gas_m3 / inuse_sqm AS gas_m3_per_sqm, 
                    tot_elec_kwh / inuse_sqm AS elec_kwh_per_sqm, 
                    a.tot_gas_m3, a.tot_elec_kwh
                FROM energy_data_buurt a 
                LEFT JOIN inuse_sqm_per_buurt b 
                ON a.bu_code = b.bu_code
            ), 
            inuse_with_per_sqm_energy_use AS (
                SELECT a.*, 
                    b.gas_m3_per_sqm, b.elec_kwh_per_sqm 
                FROM inuse_with_new_renovations a 
                LEFT JOIN per_sqm_energy_use_for_buurt b 
                ON a.bu_code = b.bu_code 
            ), 
            -- estimate energy use using av_gas_per_sqm and av_elec_per_sqm 
            inuse_with_energy_use AS (
                SELECT year, id_pand, status, sqm, 
                    CASE 
                        WHEN tot_gas_m3 IS NULL THEN gas_m3_per_sqm * sqm 
                        ELSE tot_gas_m3 
                    END AS tot_gas_m3, 
                    CASE 
                        WHEN tot_elec_kwh IS NULL THEN elec_kwh_per_sqm * sqm 
                        ELSE tot_elec_kwh 
                    END AS tot_elec_kwh, 
                    bu_code, municipality
                FROM inuse_with_per_sqm_energy_use
            ), 
            -- calculate operational emissions using energy use data 
            inuse_with_operational_emissions AS (
                SELECT *, 
                    tot_gas_m3 * 1.9 + tot_elec_kwh * 0.45 AS operational_kg 
                FROM inuse_with_energy_use
            ), 

            -- 3. Calculate embodied emissions 
            -- get building activity data 
            building_activity AS (
                SELECT 
                    year, id_pand, 
                    CASE 
                        WHEN status = 'Pand gesloopt' THEN 'demolition' 
                        WHEN status IN ('renovation - pre2020', 'renovation - post2020', 'renovation - s1') THEN 'renovation' 
                        WHEN status IN ('transformation - function change', 'transformation - adding units') THEN 'transformation' 
                        WHEN status = 'Bouw gestart' THEN 'construction' 
                        ELSE NULL 
                    END AS status, 
                    function, sqm, bu_code, %s AS municipality
                FROM housing_nl
                WHERE status != 'construction - invalid' AND year = {self.year}
            ), 
            -- calculate embodied emissions according to status 
            building_activity_with_embodied_emissions AS (
                SELECT *, 
                    CASE 
                        WHEN status = 'construction' THEN sqm * 316 
                        WHEN status IN ('renovation', 'transformation') THEN sqm * 126 
                        WHEN status = 'demolition' THEN sqm * 77 
                        ELSE NULL 
                    END AS embodied_kg 
                FROM building_activity
            ), 

            -- 4. Aggregate numbers to emissions_all_buurt_s2 
            emissions_all_per_pand AS (
                SELECT year, id_pand, status, embodied_kg, 0 AS operational_kg, sqm, 
                    NULL AS tot_gas_m3, NULL AS tot_elec_kwh, bu_code
                FROM building_activity_with_embodied_emissions

                UNION ALL 

                SELECT year, id_pand, status, 0 AS embodied_kg, ROUND(operational_kg) AS operational_kg, 
                    sqm, tot_gas_m3, tot_elec_kwh, bu_code 
                FROM inuse_with_operational_emissions
            ), 
            emissions_per_pand_with_status_columns AS (
                SELECT *, 
                    CASE WHEN status = 'construction' THEN sqm ELSE 0 END AS construction, 
                    CASE WHEN status = 'renovation' THEN sqm ELSE 0 END AS renovation, 
                    CASE WHEN status = 'transformation' THEN sqm ELSE 0 END AS transformation, 
                    CASE WHEN status = 'demolition' THEN sqm ELSE 0 END AS demolition, 
                    CASE WHEN status IN ('in use - s', 'in use') THEN sqm ELSE 0 END AS inuse
                FROM emissions_all_per_pand
            ), 
            emissions_per_buurt AS (
                SELECT {self.year} AS year, bu_code, 
                    SUM(construction) AS construction, SUM(renovation) AS renovation, 
                    SUM(transformation) AS transformation, SUM(demolition) AS demolition, 
                    SUM(inuse) AS inuse, 
                    SUM(tot_gas_m3) AS tot_gas_m3, SUM(tot_elec_kwh) AS tot_elec_kwh, 
                    SUM(embodied_kg) AS embodied_kg_s2, SUM(operational_kg) AS operational_kg_s2
                FROM emissions_per_pand_with_status_columns
                GROUP BY bu_code 
            ), 
            emissions_other_scenarios AS (
                SELECT * FROM emissions_all_buurt_s1
                WHERE municipality = %s AND year = {self.year}
            ), 
            final_table AS (
                SELECT a.year, b.municipality, b.wk_code, b.bu_code, b.bu_geom, 
                    b.construction, a.construction AS construction_s2, 
                    b.demolition, a.demolition AS demolition_s2, 
                    b.transformation, a.transformation AS transformation_s2, 
                    b.renovation, a.renovation AS renovation_s2, 
                    b.operational_kg_s0, b.operational_kg_s1, a.operational_kg_s2, 
                    b.embodied_kg_s0, b.embodied_kg_s1, a.embodied_kg_s2, 
                    a.inuse, 
                    b.gas_m3_s0, b.gas_m3_s1, ROUND(a.tot_gas_m3) AS gas_m3_s2, 
                    b.electricity_kwh_s0, b.electricity_kwh_s1, ROUND(a.tot_elec_kwh) AS electricity_kwh_s2, 
                    b.n_homes, b.population, b.woz 
                FROM emissions_per_buurt a 
                FULL JOIN emissions_other_scenarios b 
                ON a.bu_code = b.bu_code 
            )

            SELECT * FROM final_table 
            '''

class s3SpaceEfficiency():
    def __init__(self, start_year, end_year):
            self.db_manager = DatabaseManager()
            self.conn = self.db_manager.connect()
            self.cursor = self.conn.cursor()
            self.municipalities = self.db_manager.get_municipalities_list()
            self.start_year = start_year
            self.end_year = end_year

    def run(self): 
        print('\n\nCalculating emissions for scenario 3 - space efficiency ...')
        QueryRunner('sql/create_table/emissions_all_buurt_s3.sql').run_query()
        for year in range(self.start_year + 1, self.end_year + 1):
            self.year = year
            for i, municipality in enumerate(self.municipalities): 
                query = self.make_query() 
                n_placeholders = QueryTermReplacer().counter(query)
                output = f"\rYear ({self.year - self.start_year + 1}/{self.end_year - self.start_year + 1}): {self.year} | Municipality ({i+1}/{len(self.municipalities)}): {municipality}                         "
                sys.stdout.write(output)
                sys.stdout.flush()
                self.cursor.execute(query, (municipality,) * n_placeholders)
                self.conn.commit()

    def make_query(self): 
        return f''' 
            DELETE FROM emissions_all_buurt_s3 WHERE municipality = %s and year = {self.year};
            INSERT INTO emissions_all_buurt_s3 (
                year, municipality, wk_code, bu_code, bu_geom,
                embodied_kg_s0, embodied_kg_s1, embodied_kg_s2, embodied_kg_s3,
                operational_kg_s0, operational_kg_s1, operational_kg_s2, operational_kg_s3,
                construction, construction_s2, construction_s3, transformation, transformation_s2, transformation_s3,
                renovation, renovation_s2, demolition, demolition_s2, inuse, inuse_s3,
                tot_gas_m3, tot_gas_m3_s3, tot_elec_kwh, tot_elec_kwh_s3,
                population, population_change, woz, n_homes
            )

            WITH emissions_all_buurt AS (
                SELECT  
                    CASE 
                        WHEN construction = 0 AND transformation = 0 THEN 0 
                        ELSE ROUND(construction / (construction + transformation), 3) 
                    END AS construction_perc, 
                    CASE 
                        WHEN construction = 0 AND transformation = 0 THEN 0 
                        ELSE ROUND(transformation / (construction + transformation), 3)
                    END AS transformation_perc, 
                    * 
                FROM emissions_all_buurt_s2
                WHERE municipality = %s AND year = {self.year}
            ), 
            emissions_all_buurt_nextyear AS (
                SELECT * 
                FROM emissions_all_buurt_s2
                WHERE municipality = %s AND year = {self.year} + 1
            ), 

            -- population increase 
            population_change AS (
                SELECT b.population - a.population AS population_change,
                    a.* 
                FROM emissions_all_buurt a 
                LEFT JOIN emissions_all_buurt_nextyear b 
                ON a.bu_code = b.bu_code 
            ), 


            -- sqm of construction and transformation in scenario 3 
            sqm AS (
                SELECT 
                    CASE
                        WHEN construction = 0 THEN 0 
                        WHEN population_change IS NULL THEN construction
                        WHEN population_change <= 0 THEN construction 
                        WHEN population_change > 0 AND (population_change * 250 * construction_perc) < construction 
                            THEN ROUND(population_change * 250 * construction_perc)
                        WHEN population_change > 0 AND (population_change * 250 * construction_perc) >= construction 
                            THEN construction 
                    END AS construction_s3, 
                    CASE
                        WHEN transformation = 0 THEN 0 
                        WHEN population_change IS NULL THEN transformation
                        WHEN population_change <= 0 THEN transformation 
                        WHEN population_change > 0 AND (population_change * 250 * transformation_perc) < transformation 
                            THEN ROUND(population_change * 250 * transformation_perc)
                        WHEN population_change > 0 AND (population_change * 250 * transformation_perc) >= transformation 
                            THEN transformation 
                    END AS transformation_s3, 
                    * 
                FROM population_change
            ), 

            -- calculate embodied emissions
            embodied_emissions AS (
                SELECT 
                    embodied_kg_s0, embodied_kg_s1, embodied_kg_s2,  
                    operational_kg_s0, operational_kg_s1, operational_kg_s2, 
                    construction_s3*316 + transformation_s3*126 + renovation*126 + demolition*77 AS embodied_kg_s3, 
                    construction, construction_s2, construction_s3, 
                    transformation, transformation_s2, transformation_s3, 
                    renovation, renovation_s2, demolition, demolition_s2, 
                    population, population_change, n_homes, 
                    gas_m3_s0 AS tot_gas_m3, electricity_kwh_s0 AS tot_elec_kwh, 
                    woz, year, municipality, wk_code, bu_code, bu_geom 
                FROM sqm
            ), 
            emissions_all_buurt_lastyear AS (
                SELECT * 
                FROM emissions_all_buurt_s3
                WHERE municipality = %s AND year = {self.year} - 1
            ), 
            inuse_lastyear_s3 AS (
                SELECT bu_code, SUM(inuse) AS inuse 
                FROM emissions_all_buurt_lastyear
                GROUP BY bu_code
            ), 
            embodied_emissions_with_inuse_lastyear AS (
                SELECT b.inuse AS inuse_lastyear, a.*
                FROM embodied_emissions a 
                LEFT JOIN inuse_lastyear_s3 b 
                ON a.bu_code = b.bu_code
            ), 
            building_activity_lastyear_s3 AS (
                SELECT year, bu_code, construction_s3, transformation_s3
                FROM emissions_all_buurt_lastyear
            ), 
            embodied_emissions_with_values_lastyear AS (
                SELECT 
                    b.construction_s3 AS construction_lastyear, 
                    b.transformation_s3 AS transformation_lastyear, 
                    a.*
                FROM embodied_emissions_with_inuse_lastyear a 
                LEFT JOIN building_activity_lastyear_s3 b 
                ON a.bu_code = b.bu_code
            ), 
            inuse_s0 AS (
                SELECT bu_code, SUM(sqm) AS inuse
                FROM housing_inuse_2012_2021
                WHERE municipality = %s AND year = {self.year}
                GROUP BY bu_code
            ), 
            embodied_emissions_with_inuse_s0 AS (
                SELECT
                    b.inuse AS inuse_s0, a.*
                FROM embodied_emissions_with_values_lastyear a 
                LEFT JOIN inuse_s0 b 
                ON a.bu_code = b.bu_code
            ), 

            -- calculate in-use sqm for scenario 3 
            s3_inuse AS (
                SELECT 
                    CASE 
                        WHEN year = 2012 THEN inuse_s0
                        -- WHEN construction = construction_s3 AND transformation = transformation_s3 THEN inuse_s0
                        ELSE inuse_lastyear + construction_lastyear + transformation_lastyear
                    END AS inuse_s3,
                    * 
                FROM embodied_emissions_with_inuse_s0
            ), 
            s3_inuse_adjusted AS (
                SELECT 
                    CASE 
                        WHEN inuse_s3 > inuse_s0 THEN inuse_s0
                        ELSE inuse_s3
                    END AS inuse_s3_adjusted, 
                *
                FROM s3_inuse
            ), 

            -- calculate energy usage (gas and electricity) for s3 
            s3_energy AS (
                SELECT 
                    ROUND(tot_gas_m3 / inuse_s0 * inuse_s3_adjusted) AS tot_gas_m3_s3, 
                    ROUND(tot_elec_kwh / inuse_s0 * inuse_s3_adjusted) AS tot_elec_kwh_s3, 
                    * 
                FROM s3_inuse_adjusted
            ), 
            -- calculate operational emissions for s3 
            s3_operational_emissions AS (
                SELECT 
                    ROUND(tot_gas_m3_s3 * 1.9 + tot_elec_kwh_s3 * 0.45) AS operational_kg_s3, 
                    * 
                FROM s3_energy
            ), 
            final_table AS (
                SELECT 
                    year, municipality, wk_code, bu_code, bu_geom, 
                    embodied_kg_s0, embodied_kg_s1, embodied_kg_s2, embodied_kg_s3, 
                    ROUND(operational_kg_s0) AS operational_kg_s0, 
                    operational_kg_s1, operational_kg_s2, operational_kg_s3, 
                    construction, construction_s2, construction_s3, transformation, transformation_s2, transformation_s3, 
                    renovation, renovation_s2, demolition, demolition_s2, inuse_s0, inuse_s3_adjusted, 
                    tot_gas_m3, tot_gas_m3_s3, tot_elec_kwh, tot_elec_kwh_s3, 
                    population, population_change, woz, n_homes 
                FROM s3_operational_emissions
            )

            SELECT * FROM final_table 
            ''' 