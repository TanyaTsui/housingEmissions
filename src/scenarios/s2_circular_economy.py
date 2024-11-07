from src.data_processing._common.query_runner import QueryRunner

class s2CircularEconomy(): 
    def run(self): 
        QueryRunner('sql/create_table/demolished_buildings_nl.sql').run_query('Creating demolished_buildings_nl table...')  
        QueryRunner('sql/data_processing/bag/get_demolished_buildings_nl.sql').run_query_for_each_municipality('Getting demolished buildings...')
        QueryRunner('sql/create_table/housing_nl_s1.sql').run_query('Creating housing_nl_s1 table...')
        QueryRunner('sql/s1_circular_economy/renovation_suitability.sql').run_query_for_each_municipality('Calculating renovation suitability...')
        QueryRunner('sql/create_table/emissions_embodied_housing_nl_s1.sql').run_query('Creating emissions_embodied_housing_nl_s1 table...')
        QueryRunner('sql/embodied_emissions/calculate_emissions/calculate_embodied_emissions_s1.sql').run_query_for_each_municipality('Calculating embodied emissions for strategy one...')
