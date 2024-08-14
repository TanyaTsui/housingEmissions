from ...data_processing._common.query_runner import QueryRunner
from ...data_processing._common.query_manager import QueryManager

class RenovationInfoAdder(): 
    def __init__(self): 
        None

    def run(self):
        query_runner = QueryRunner()
        query_manager = QueryManager()

        query_runner.run_query(query_manager.query_create_housing_nl_table())
        query_runner.run_query_for_each_municipality(query_manager.query_add_pre2020_renovations(), 4, 
                                                     '\nadding pre-2020 renovations to housing_nl table...')
        query_runner.run_query_for_each_municipality(query_manager.query_add_post2020_renovations(), 4, 
                                                     '\nadding post-2020 renovations to housing_nl table...')
        # TODO: add transformations from function change to housing_nl, with status as 'transformation - function change'
        # TODO: add transformations from adding units to housing_nl, with status as 'transformation - new units'
        # TODO: remove rows from housing_nl where id_pand and registration year are duplicated, 
        #       keeping the row(s) about transformation 

        # TODO: remove queries below
        query_runner.run_query_for_each_municipality(query_manager.query_add_transformation_function_change(), 4, '\nadding transformations from function change...') 
