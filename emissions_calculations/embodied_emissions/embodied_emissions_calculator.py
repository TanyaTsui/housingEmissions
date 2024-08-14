from ...data_processing._common.query_runner import QueryRunner
from ...data_processing._common.query_manager import QueryManager

class EmbodiedEmissionsCalculator(): 
    def __init__(self): 
        None
    
    def run(self): 
        query_runner = QueryRunner()
        query_manager = QueryManager()

        query_runner.run_query(query_manager.query_create_emissions_embodied_housing_nl_table())
        query_runner.run_query_for_each_municipality(query_manager.query_calculate_embodied_emissions(), 1, '\ncalculating embodied emissions...')