from src.data_processing._common.query_runner import QueryRunner

class IndexAdder(): 
    def __init__(self): 
        self.query_runner = QueryRunner()
        
    def add_index(self, table_name:str, column_names:list): 
        for column_name in column_names: 
            if isinstance(column_name, list): 
                column_name_forIndex = '_'.join(column_name)
                column_name_forQuery = ', '.join(column_name)
                query = f'CREATE INDEX IF NOT EXISTS idx_{table_name}_{column_name_forIndex} ON {table_name} ({column_name_forQuery});'
                self.query_runner.run_query(query)
            else: 
                query = f'CREATE INDEX IF NOT EXISTS idx_{table_name}_{column_name} ON {table_name} ({column_name});'
                self.query_runner.run_query(query)