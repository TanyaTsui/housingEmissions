from data_processing._common.query_runner import QueryRunner

class EmbodiedEmissionsCalculator(): 
    def __init__(self): 
        None
    
    def run(self): 
        QueryRunner('sql/create_table/emissions_embodied_housing_nl.sql').run_query()
        QueryRunner('sql/embodied_emissions/calculate_emissions/calculate_embodied_emissions.sql').run_query_for_each_municipality('calculating embodied emissions...')