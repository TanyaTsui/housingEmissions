from data_processing._common.query_runner import QueryRunner

class AdminBoundaryAdder(): 
    def __init__(self): 
        None

    def run(self):
        QueryRunner('sql/data_processing/bag/add_admin_columns.sql').run_query('Adding admin boundary columns to bag...')
        QueryRunner('sql/data_processing/bag/match_admin_boundaries_bag.sql').run_query_for_each_municipality('Matching bag to admin boundaries...')
        QueryRunner('sql/data_processing/ahn/match_admin_boundaries_ahn.sql').run_query_for_each_municipality('Matching ahn to admin boundaries...')
        QueryRunner('sql/create_table/landuse_nl.sql').run_query('Creating landuse_nl table...')
        QueryRunner('sql/data_processing/bag/match_admin_boundaries_landuse.sql').run_query_for_each_municipality('Matching landuse_nl to admin boundaries...')