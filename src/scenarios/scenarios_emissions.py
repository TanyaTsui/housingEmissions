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
        QueryRunner('sql/create_table/emissions_all_wijk_s1.sql').run_query() 

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
            DELETE FROM emissions_all_wijk_s1 WHERE year = {year} AND municipality = %s;
            INSERT INTO emissions_all_wijk_s1 (
                year, municipality, wk_code, wk_geom, population, av_woz, n_units, inuse, 
                construction, transformation, renovation, demolition,
                gas_m3_s0, gas_m3_s1, electricity_kwh_s0, electricity_kwh_s1, 
                operational_kg_s0, operational_kg_s1, embodied_kg_s0, embodied_kg_s1
            )

            -- get wijk_stats: wijk level energy use and in-use sqm data 
            WITH cbs_stats_wijk AS (
                SELECT * FROM cbs_map_all_wijk WHERE municipality = %s AND year = {year}
            ), 
            housing_inuse AS (
                SELECT * FROM housing_inuse_{self.start_year}_{self.end_year} WHERE municipality = %s AND year = {year}
            ), 
            housing_inuse_wijk AS (
                SELECT municipality, wk_code, year, SUM(sqm) AS sqm, SUM(n_units) AS n_units
                FROM housing_inuse
                GROUP BY municipality, wk_code, year
            ), 
            wijk_stats AS (
                SELECT b.*, a.sqm, a.n_units 
                FROM housing_inuse_wijk a 
                JOIN cbs_stats_wijk b 
                ON a.municipality = b.municipality 
                    AND a.year = b.year
                    AND a.wk_code = b.wk_code 
            ), 

            -- get all constructions and renovations that happened before year  
            construction_municipality AS ( -- all construction activity in year
                SELECT id_pand, 
                    CASE 
                        WHEN status = 'Pand gesloopt' THEN LEFT(registration_start, 4)::INTEGER
                        WHEN status != 'Pand gesloopt' AND registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
                        ELSE LEFT(registration_start, 4)::INTEGER
                    END AS year, 
                    status, sqm, n_units, geom, geom_28992, wk_code, municipality
                FROM housing_nl
                WHERE municipality = %s
                    AND ahn_version IS NULL
            ), 
            construction_sample AS (
                SELECT * FROM construction_municipality 
                WHERE year <= {year}
            ), 

            -- identify in-use buildings that were previously constructed or renovated (low-energy)
            inuse_lowenergy AS (
                SELECT 
                    b.id_pand, b.year, 'Pand in gebruik - low energy' AS status, b.sqm, b.n_units, b.wk_code
                FROM (SELECT DISTINCT ON (id_pand) id_pand, status FROM construction_sample) a 
                LEFT JOIN housing_inuse b 
                ON a.id_pand = b.id_pand
                WHERE a.status != 'Pand gesloopt'
                    AND b.id_pand IS NOT NULL
            ), 
            inuse_normalenergy AS (
                SELECT b.id_pand, b.year, 'Pand in gebruik' AS status, b.sqm, b.n_units, b.wk_code
                FROM construction_sample a 
                RIGHT JOIN housing_inuse b 
                ON a.id_pand = b.id_pand
                WHERE a.id_pand IS NULL 
            ), 
            buildings_all AS (
                -- all construction / renovation / transformation / demolition activity in year
                SELECT id_pand, year, status, sqm, n_units, wk_code 
                FROM construction_sample 
                WHERE year = {year}
                
                UNION ALL 
                
                -- low energy in use buildings in year
                SELECT * FROM inuse_lowenergy
                
                UNION ALL 
                
                -- non-low energy in use buildings in year
                SELECT * FROM inuse_normalenergy
            ), 

            -- calculate energy use per building according to low or normal energy use status
            energy_use_per_building AS (
                SELECT a.id_pand, a.year, a.status, a.sqm, a.n_units, 
                    CASE 
                        WHEN status IN ('Pand in gebruik', 'Pand in gebruik - low energy') THEN ROUND(a.sqm / b.sqm * b.gas_m3)
                        ELSE 0
                    END AS gas_m3_s0,
                    CASE
                        WHEN status = 'Pand in gebruik' THEN ROUND(a.sqm / b.sqm * b.gas_m3)
                        WHEN status = 'Pand in gebruik - low energy' AND a.sqm * 5 < a.sqm / b.sqm * b.gas_m3 THEN a.sqm * 5
                        WHEN status = 'Pand in gebruik - low energy' AND a.sqm * 5 >= a.sqm / b.sqm * b.gas_m3 THEN ROUND(a.sqm / b.sqm * b.gas_m3)
                        ELSE 0 
                    END AS gas_m3_s1,
                    CASE 
                        WHEN status IN ('Pand in gebruik', 'Pand in gebruik - low energy') THEN ROUND(a.sqm / b.sqm * b.elec_kwh) 
                        ELSE 0 
                    END AS electricity_kwh_s0, 
                    CASE 
                        WHEN status IN ('Pand in gebruik', 'Pand in gebruik - low energy') THEN ROUND(a.sqm / b.sqm * b.elec_kwh) 
                        ELSE 0 
                    END AS electricity_kwh_s1, 
                    b.population, b.av_woz, 
                    b.wk_code, b.wk_geom
                FROM buildings_all a 
                JOIN wijk_stats b 
                ON a.wk_code = b.wk_code
            ), 
            emissions_per_building AS (
                SELECT id_pand, year, status, sqm, n_units, gas_m3_s0, gas_m3_s1, electricity_kwh_s0, electricity_kwh_s1, 
                    ROUND(gas_m3_s0 * 1.9 + electricity_kwh_s0 * 0.45) AS operational_kg_s0, 
                    CASE 
                        WHEN status IN ('transformation - adding units', 'transformation - function change', 
                                        'renovation - pre2020', 'renovation - post2020') THEN sqm * 126
                        WHEN status = 'Bouw gestart' THEN sqm * 325
                        WHEN status = 'Pand gesloopt' THEN sqm * 77
                        ELSE 0 
                    END AS embodied_kg_s0, 
                    
                    ROUND(gas_m3_s1 * 1.9 + electricity_kwh_s1 * 0.45) AS operational_kg_s1, 
                    CASE 
                        WHEN status IN ('transformation - adding units', 'transformation - function change', 
                                        'renovation - pre2020', 'renovation - post2020') THEN sqm * 200
                        WHEN status = 'Bouw gestart' THEN sqm * 550
                        WHEN status = 'Pand gesloopt' THEN sqm * 77
                        ELSE 0 
                    END AS embodied_kg_s1, 
                    population, av_woz, wk_code, wk_geom
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
            )
                
            SELECT {year} AS year, %s AS municipality, wk_code, wk_geom, 
                MIN(population) AS population, MIN(av_woz) AS av_woz, -- MIN because these values are the same for all buildings in a wijk
                
                SUM(n_units) AS n_units, SUM(inuse) AS inuse, SUM(construction) AS construction, SUM(transformation) AS transformation, 
                SUM(renovation) AS renovation, SUM(demolition) AS demolition, 
                
                SUM(gas_m3_s0) AS gas_m3_s0, SUM(gas_m3_s1) AS gas_m3_s1, 
                SUM(electricity_kwh_s0) AS electricity_kwh_s0, SUM(electricity_kwh_s1) AS electricity_kwh_s1, 
                
                SUM(operational_kg_s0) AS operational_kg_s0, SUM(operational_kg_s1) AS operational_kg_s1, 
                SUM(embodied_kg_s0) AS embodied_kg_s0, SUM(embodied_kg_s1) AS embodied_kg_s1
                
            FROM stats_per_building
            GROUP BY wk_code, wk_geom
            '''

class s2CircularEconomy(): 
    def run(self): 
        # QueryRunner('sql/create_table/demolished_buildings_nl.sql').run_query('Creating demolished_buildings_nl table...')  
        # QueryRunner('sql/data_processing/bag/get_demolished_buildings_nl.sql').run_query_for_each_municipality('Getting demolished buildings...')
        
        # create and fill housing_nl_s2 table
        # QueryRunner('sql/create_table/housing_nl_s2.sql').run_query('Creating housing_nl_s2 table...')
        # QueryRunner('sql/s1_circular_economy/renovation_suitability.sql').run_query_for_each_municipality('Adding to housing_nl_s2...')

        # create and fill emissions_all_wijk_s2 table
        QueryRunner('sql/create_table/emissions_all_wijk_s2.sql').run_query('Creating emissions_all_wijk_s2 table...')
        QueryRunner('sql/s1_circular_economy/emissions_all_wijk_s2.sql').run_query_for_each_municipality('Adding to emissions_all_wijk_s2...')