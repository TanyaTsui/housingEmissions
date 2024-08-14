from data_processing._common.query_manager import QueryManager
from data_processing._common.query_runner import QueryRunner

class GeomColumnAdder(): 
    def run(self, data_types): 
        query_manager = QueryManager()
        query_runner = QueryRunner()

        if 'pand' in data_types:
            query_runner.run_query(query_manager.query_add_geom_columns_bag_pand())
        if 'vbo' in data_types:
            None # add geom columns to bag_vbo
