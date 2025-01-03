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
                SELECT * FROM housing_inuse_{self.start_year}_{self.end_year} WHERE municipality = %s AND year = {self.year}
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
                
                    a.tot_gas_m3 AS gas_m3_s0,
                    CASE
                        WHEN status = 'Pand in gebruik' THEN a.tot_gas_m3 
                        WHEN status = 'Pand in gebruik - low energy' AND a.sqm * 5 < a.tot_gas_m3  THEN a.sqm * 5
                        WHEN status = 'Pand in gebruik - low energy' AND a.sqm * 5 >= a.tot_gas_m3  THEN a.tot_gas_m3 
                        ELSE 0 
                    END AS gas_m3_s1,
                
                    a.tot_elec_kwh AS electricity_kwh_s0, 
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
                
                    gas_m3_s0, gas_m3_s1, electricity_kwh_s0, electricity_kwh_s1, 
                    ROUND(gas_m3_s0 * 1.9 + electricity_kwh_s0 * 0.45) AS operational_kg_s0, 
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
            final_selection AS (
                SELECT {self.year} AS year, municipality, wk_code, bu_code, bu_geom,  
                    ROUND(AVG(population)) AS population, ROUND(AVG(woz)) AS woz, 
                    
                    SUM(n_homes) AS n_homes, SUM(inuse) AS inuse, SUM(construction) AS construction, SUM(transformation) AS transformation, 
                    SUM(renovation) AS renovation, SUM(demolition) AS demolition, 
                    
                    ROUND(SUM(gas_m3_s0)) AS gas_m3_s0, ROUND(SUM(gas_m3_s1)) AS gas_m3_s1, 
                    ROUND(SUM(electricity_kwh_s0)) AS electricity_kwh_s0, ROUND(SUM(electricity_kwh_s1)) AS electricity_kwh_s1, 
                    
                    SUM(operational_kg_s0) AS operational_kg_s0, SUM(operational_kg_s1) AS operational_kg_s1, 
                    SUM(embodied_kg_s0) AS embodied_kg_s0, SUM(embodied_kg_s1) AS embodied_kg_s1
                    
                FROM stats_per_building
                GROUP BY municipality, wk_code, bu_code, bu_geom
            )

            SELECT * FROM final_selection
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
        # QueryRunner('sql/create_table/housing_nl_s2.sql').run_query()
        # QueryRunner('sql/s1_circular_economy/renovation_suitability.sql').run_query_for_each_municipality('Adding to housing_nl_s2...')

        # create and fill emissions_all_buurt_s2 table
        QueryRunner('sql/create_table/emissions_all_buurt_s2.sql').run_query()
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
                construction, demolition, transformation, renovation,
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
            )
            SELECT a.year, b.municipality, b.wk_code, b.bu_code, b.bu_geom, 
                a.construction, a.demolition, a.transformation, a.renovation, a.inuse, 
                b.gas_m3_s0, b.gas_m3_s1, ROUND(a.tot_gas_m3) AS gas_m3_s2, 
                b.electricity_kwh_s0, b.electricity_kwh_s1, ROUND(a.tot_elec_kwh) AS electricity_kwh_s2, 
                b.operational_kg_s0, b.operational_kg_s1, a.operational_kg_s2, 
                b.embodied_kg_s0, b.embodied_kg_s1, a.embodied_kg_s2, 
                b.n_homes, b.population, b.woz 
            FROM emissions_per_buurt a 
            FULL JOIN emissions_other_scenarios b 
            ON a.bu_code = b.bu_code 
            '''

class s3SpaceEfficiency():
    def run(self): 
        print('\n\nCalculating emissions for scenario 3 - space efficiency ...')
        QueryRunner('sql/create_table/emissions_all_buurt_s3.sql').run_query()
        QueryRunner('sql/s3_space_efficiency/space_efficiency.sql').run_query_for_each_municipality('Adding to emissions_all_buurt_s3...') 