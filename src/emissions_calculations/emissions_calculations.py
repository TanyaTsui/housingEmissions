from src.data_processing._common.query_runner import QueryRunner

class EmissionsCalculator(): 
    def run(self): 
        QueryRunner('sql/create_table/emissions_all_wijk.sql').run_query('Creating emissions_all_wijk table...')
        QueryRunner('sql/combined_emissions/emissions_wijk.sql').run_query_for_each_municipality('Calculating wijk-level emissions results...')