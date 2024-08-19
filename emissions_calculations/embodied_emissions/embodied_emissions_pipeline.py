from data_processing._common.query_runner import QueryRunner

class AdminBoundaryAdder(): 
    def run(self):
        QueryRunner('sql/data_processing/bag/add_admin_columns.sql').run_query('Adding admin boundary columns to bag...')
        QueryRunner('sql/data_processing/bag/match_admin_boundaries_bag.sql').run_query_for_each_municipality('Matching bag to admin boundaries...')
        QueryRunner('sql/data_processing/ahn/match_admin_boundaries_ahn.sql').run_query_for_each_municipality('Matching ahn to admin boundaries...')
        QueryRunner('sql/create_table/landuse_nl.sql').run_query('Creating landuse_nl table...')
        QueryRunner('sql/data_processing/bag/match_admin_boundaries_landuse.sql').run_query_for_each_municipality('Matching landuse_nl to admin boundaries...')

class RenovationInfoAdder(): 
    def run(self):
        QueryRunner('sql/create_table/housing_nl.sql').run_query('Creating housing_nl table...')
        QueryRunner('sql/embodied_emissions/renovation/addPre2020Renovation_toHousingNL.sql').run_query_for_each_municipality('Adding pre-2020 renovations to housing_nl...')
        QueryRunner('sql/embodied_emissions/renovation/addPost2020Renovation_toHousingNL.sql').run_query_for_each_municipality('Adding post-2020 renovations to housing_nl...')
        QueryRunner('sql/embodied_emissions/renovation/transformation_functionChange.sql').run_query_for_each_municipality('Adding transformations from function change to housing_nl...')
        QueryRunner('sql/embodied_emissions/renovation/transformation_addUnits.sql').run_query_for_each_municipality('Adding transformations from adding units to housing_nl...')
        QueryRunner('sql/embodied_emissions/renovation/remove_renovation_duplicates.sql').run_query_for_each_municipality('Removing renovation duplicates from housing_nl...')

class HousingFunctionSqmEstimator(): 
    def run(self):
        QueryRunner('sql/embodied_emissions/function_sqm/estimate_housing_function.sql').run_query_for_each_municipality('Estimating housing function...')
        QueryRunner('sql/embodied_emissions/function_sqm/estimate_housing_sqm.sql').run_query_for_each_municipality('Estimating housing sqm...')
        QueryRunner('sql/embodied_emissions/function_sqm/add_landuse_column.sql').run_query('Adding landuse column to housing_nl...')
        QueryRunner('sql/embodied_emissions/function_sqm/filter_out_non_residential.sql').run_query_for_each_municipality('Filtering out non-residential buildings using landuse data...')

class EmbodiedEmissionsCalculator(): 
    def run(self): 
        QueryRunner('sql/create_table/emissions_embodied_housing_nl.sql').run_query('Creating emissions_embodied_housing_nl table...')
        QueryRunner('sql/embodied_emissions/calculate_emissions/calculate_embodied_emissions.sql').run_query_for_each_municipality('calculating embodied emissions...')