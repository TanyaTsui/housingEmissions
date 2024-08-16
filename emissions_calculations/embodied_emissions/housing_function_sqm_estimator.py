from data_processing._common.query_runner import QueryRunner

class HousingFunctionSqmEstimator(): 
    def __init__(self): 
        None

    def run(self):
        QueryRunner('sql/embodied_emissions/function_sqm/estimate_housing_function.sql').run_query_for_each_municipality('Estimating housing function...')
        QueryRunner('sql/embodied_emissions/function_sqm/estimate_housing_sqm.sql').run_query_for_each_municipality('Estimating housing sqm...')
        QueryRunner('sql/embodied_emissions/function_sqm/add_landuse_column.sql').run_query('Adding landuse column to housing_nl...')
        QueryRunner('sql/embodied_emissions/function_sqm/filter_out_non_residential.sql').run_query_for_each_municipality('Filtering out non-residential buildings using landuse data...')