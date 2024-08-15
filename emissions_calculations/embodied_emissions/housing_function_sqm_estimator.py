from data_processing._common.query_runner import QueryRunner
from data_processing._common.query_manager import QueryManager

class HousingFunctionSqmEstimator(): 
    def __init__(self): 
        None

    def run(self):
        query_runner = QueryRunner()
        query_manager = QueryManager()

        
        query_runner.run_query_for_each_municipality(query_manager.query_estimate_housing_function(), 2, '\nestimating housing function...')
        query_runner.run_query_for_each_municipality(query_manager.query_estimate_housing_sqm(), 3, '\nestimating housing sqm...')
        query_runner.run_query(query_manager.query_add_landuse_column())
        query_runner.run_query_for_each_municipality(query_manager.query_filter_out_non_residential(), 3, '\nfiltering out non-residential buildings using landuse data...')