from ...data_processing._common.query_runner import QueryRunner
from ...data_processing._common.query_manager import QueryManager

class AdminBoundaryAdder(): 
    def __init__(self): 
        None

    def run(self):
        query_runner = QueryRunner()
        query_manager = QueryManager()
        query_runner.run_query_for_each_municipality(query_manager.query_match_bag_to_admin_boundaries(), 2, 'matching bag to admin boundaries...')
        # query_runner.run_query_for_each_municipality(query_manager.query_add_municipality_ahn(), 1, 'adding municipality and province columns to ahn_elevation...')
        # query_runner.run_query(query_manager.query_create_landuse_nl_table(), 'creating landuse_nl table...')
        # query_runner.run_query_for_each_municipality(query_manager.query_add_buurtInfo_to_landuse_nl(), 1, 'adding buurt info to landuse...')
