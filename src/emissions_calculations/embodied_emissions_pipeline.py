from src.data_processing._common.query_runner import QueryRunner

class AdminBoundaryAdder(): 
    def run(self):
        # QueryRunner('sql/data_processing/bag/add_admin_columns.sql').run_query('Adding admin boundary columns to bag...')
        # QueryRunner('sql/data_processing/bag/create_index_bag.sql').run_query('Creating index for bag...')
        QueryRunner('sql/data_processing/bag/match_admin_boundaries_bagvbo_buurt.sql').run_query_for_each_municipality('Adding 2022 admin boundaries to bag_vbo (from cbs_map_2022)...')
        QueryRunner('sql/data_processing/bag/match_admin_boundaries_bagpand_buurt.sql').run_query_for_each_municipality('Adding 2022 admin boundaries to bag_pand (from cbs_map_2022)...')
        None 

class ConstructionActivityInfoAdder(): 
    def run(self):
        # QueryRunner('sql/create_table/housing_nl.sql').run_query('Creating housing_nl table...') # creates table if it doesn't already exist

        # renovation activity
        QueryRunner('sql/embodied_emissions/renovation/addPre2020Renovation_toHousingNL.sql').run_query_for_each_municipality('Adding pre-2020 renovations to housing_nl...') 
        QueryRunner('sql/embodied_emissions/renovation/addPost2020Renovation_toHousingNL.sql').run_query_for_each_municipality('Adding post-2020 renovations to housing_nl...') 
        
        # transformation activity
        QueryRunner('sql/embodied_emissions/renovation/transformation_functionChange.sql').run_query_for_each_municipality('Adding transformations from function change to housing_nl...') 
        QueryRunner('sql/embodied_emissions/renovation/transformation_addUnits.sql').run_query_for_each_municipality('Adding transformations from adding units to housing_nl...')
        QueryRunner('sql/embodied_emissions/renovation/remove_renovation_duplicates.sql').run_query_for_each_municipality('Removing renovation duplicates from housing_nl...')      
        
        # construction and demolition activity
        QueryRunner('sql/embodied_emissions/construction_and_demolition/add_construction_and_demolition.sql').run_query_for_each_municipality('Adding construction and demolition data to housing_nl...') 

class DataHarmoniserWijk(): 
    def run(self): 
        # make buurt to wijk key
        # QueryRunner('sql/_common/make_buurt_to_wijk_key.sql').run_query_for_each_municipality('Making buurt to wijk key...')
        # QueryRunner('sql/_common/buurt_and_wijk_key_delete_duplicates.sql').run_query() 
        
        # # add wk_code to tables 
        # QueryRunner('sql/data_processing/bag/match_admin_boundaries_bagvbo_wijk.sql').run_query_for_each_municipality('Matching bag vbo to admin boundaries (from cbs_wijk_2012) ...') # finished running
        # QueryRunner('sql/data_processing/bag/match_admin_boundaries_bagpand_wijk.sql').run_query_for_each_municipality('Matching bag pand to admin boundaries (from cbs_wijk_2012) ...')
        # QueryRunner('sql/data_processing/bag/match_admin_boundaries_housinginuse_wijk.sql').run_query_for_each_municipality('Matching housinginuse to admin boundaries (from cbs_wijk_2012) ...')
        # QueryRunner('sql/data_processing/bag/match_admin_boundaries_housingnl_wijk.sql').run_query_for_each_municipality('Matching housingnl to admin boundaries (from cbs_wijk_2012) ...')

        None 







# class EmbodiedEmissionsCalculator_byBuurt():
#     def run(self):
#         QueryRunner('sql/create_table/emissions_embodied_bybuurt.sql').run_query('Creating emissions_embodied_bybuurt table...')
#         QueryRunner('sql/embodied_emissions/calculate_emissions/calculate_embodied_emissions_bybuurt.sql').run_query_for_each_municipality('Aggregating housing data by buurt...')

# class EmbodiedEmissionsCalculator(): 
#     def run(self):
#         QueryRunner('sql/create_table/emissions_embodied_housing_nl.sql').run_query('Creating emissions_embodied_housing_nl table...')
#         QueryRunner('sql/embodied_emissions/calculate_emissions/calculate_embodied_emissions.sql').run_query_for_each_municipality('calculating embodied emissions...')

# class NlBuurtenMaker(): # not needed
#     def run(self): 
#         QueryRunner('sql/create_table/nl_buurten.sql').run_query('Creating nl_buurten table...')
#         QueryRunner('sql/data_processing/nl/nl_buurten_add_rows.sql').run_query('Adding rows from cbs_map_2022 to nl_buurten...')

# class HousingFunctionSqmEstimator(): # not needed
#     def run(self):
#         QueryRunner('sql/embodied_emissions/function_sqm/estimate_housing_function.sql').run_query_for_each_municipality('Estimating housing function...') # not needed 
#         QueryRunner('sql/embodied_emissions/function_sqm/estimate_housing_sqm.sql').run_query_for_each_municipality('Estimating housing sqm...') # not needed 
#         QueryRunner('sql/embodied_emissions/function_sqm/add_landuse_column.sql').run_query('Adding landuse column to housing_nl...') # not needed 
#         QueryRunner('sql/embodied_emissions/function_sqm/filter_out_non_residential.sql').run_query_for_each_municipality('Filtering out non-residential buildings using landuse data...') # not needed 

