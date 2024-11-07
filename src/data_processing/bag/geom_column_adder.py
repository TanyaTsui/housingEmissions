from src.data_processing._common.query_runner import QueryRunner

class GeomColumnAdder(): 
    def run(self, data_types): 
        if 'pand' in data_types:
            QueryRunner('sql/data_processing/bag/geom_columns/add_geom_columns_bag_pand.sql').run_query('Adding geom columns to bag_pand...')
        if 'vbo' in data_types:
            None # add geom columns to bag_vbo
