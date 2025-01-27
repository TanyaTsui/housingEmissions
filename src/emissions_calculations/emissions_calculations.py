from src.data_processing._common.query_runner import QueryRunner
from src.data_processing._common.database_manager import DatabaseManager
import sys

class EmissionsCalculator(): 
    def __init__(self, start_year, end_year):
        self.db_manager = DatabaseManager()
        self.conn = self.db_manager.connect()
        self.cursor = self.conn.cursor()
        self.municipalities = self.db_manager.get_municipalities_list()
        self.start_year = start_year
        self.end_year = end_year

    def run(self): 
        print(f'\nCalculating housing emissions for years {self.start_year} t/m {self.end_year} ...')
        
        # create table if it doesn't already exist
        QueryRunner('sql/create_table/emissions_all_buurt.sql').run_query() 
        
        self.query = self.make_query()
        self.n_placeholders = self.query.count('%s')
        
        for i, municipality in enumerate(self.municipalities):
            output = f"\rProcessing municipality ({i+1}/{len(self.municipalities)}): {municipality}                         "
            sys.stdout.write(output)
            sys.stdout.flush()
            self.cursor.execute(self.query, (municipality,) * self.n_placeholders)
            self.conn.commit()

    def make_query(self): 
        return f''' 
            DELETE FROM emissions_all_buurt WHERE municipality = %s;
            INSERT INTO emissions_all_buurt (
                municipality, wk_code, bu_code, bu_geom, year, 
                construction, renovation, transformation, demolition, 
                population, n_homes, tot_gas_m3, tot_elec_kwh, woz, 
                embodied_kg, operational_kg
            )

            -- buurt level construction events (input for embodied emissions) 
            WITH construction_events_raw AS (
                SELECT 
                    CASE
                        WHEN registration_end IS NOT NULL THEN LEFT(registration_end, 4)::INTEGER
                        ELSE LEFT(registration_start, 4)::INTEGER
                    END AS year, 
                    CASE
                        WHEN status = 'Bouw gestart' THEN 'construction'
                        WHEN status = 'Pand gesloopt' THEN 'demolition'
                        WHEN status IN ('renovation - post2020', 'renovation - pre2020') THEN 'renovation'
                        WHEN status IN ('transformation - adding units', 'transformation - function change') THEN 'transformation'
                    END AS status, 
                    id_pand, n_units, sqm, bu_code, wk_code, municipality 
                FROM housing_nl
                WHERE municipality = %s
            ), 
            construction_events_buurt AS (
                SELECT 
                    municipality, wk_code, bu_code, year,
                    SUM(CASE WHEN status = 'construction' THEN sqm ELSE 0 END) AS construction,
                    SUM(CASE WHEN status = 'renovation' THEN sqm ELSE 0 END) AS renovation,
                    SUM(CASE WHEN status = 'transformation' THEN sqm ELSE 0 END) AS transformation,
                    SUM(CASE WHEN status = 'demolition' THEN sqm ELSE 0 END) AS demolition
                FROM construction_events_raw
                WHERE year >= {self.start_year}
                    AND year <= {self.end_year}
                GROUP BY municipality, wk_code, bu_code, year
            ), 

            -- buurt level energy use (input for operational emissions) 
            energy_buurt AS (
                SELECT * FROM cbs_map_all_buurt WHERE municipality = %s
            ), 
            inuse_buurt AS (
                SELECT year, bu_code, SUM(sqm) AS inuse
                FROM housing_inuse_2012_2021 
                WHERE municipality = %s
                GROUP BY year, bu_code 
            )

            -- calculate emissions 
            buurt_stats AS (
                SELECT 
                    COALESCE(a.municipality, b.municipality) AS municipality,
                    COALESCE(a.wk_code, b.wk_code) AS wk_code,
                    COALESCE(a.bu_code, b.bu_code) AS bu_code,
                    COALESCE(a.year, b.year) AS year, 
                    COALESCE(a.construction, 0) AS construction, 
                    COALESCE(a.renovation, 0) AS renovation, 
                    COALESCE(a.transformation, 0) AS transformation, 
                    COALESCE(a.demolition, 0) AS demolition, 
                    b.bu_geom, b.population, b.n_homes, b.tot_gas_m3, b.tot_elec_kwh, b.woz
                FROM construction_events_buurt a 
                FULL JOIN energy_buurt b 
                ON a.year = b.year AND a.bu_code = b.bu_code 
            ), 
            emissions_buurt AS (
                SELECT 
                    municipality, wk_code, bu_code, bu_geom, year, 
                    construction, renovation, transformation, demolition, 
                    population, n_homes, tot_gas_m3, tot_elec_kwh, woz, 
                    construction * 316 + renovation * 126 + transformation * 126 + demolition * 77 AS embodied_kg, 
                    tot_gas_m3 * 1.9 + tot_elec_kwh * 0.45 AS operational_kg
                FROM buurt_stats 
            )

            SELECT * FROM emissions_buurt 
            ''' 