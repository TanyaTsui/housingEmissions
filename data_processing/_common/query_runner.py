import sys
from data_processing._common.database_manager import DatabaseManager

class QueryRunner(): 
    def __init__(self): 
        self.db_manager = DatabaseManager()
        self.conn = self.db_manager.connect()
        self.cursor = self.conn.cursor()
    
    def run_query(self, query, message=''): 
        if message != '': 
            print(message)
        self.conn.rollback()
        self.cursor.execute(query)
        self.conn.commit()

    def run_query_for_each_municipality(self, query, len_tuple, message=''):
        if message != '': 
            print(message)
        municipalities = self.db_manager.get_municipalities_list()
        for i, municipality in enumerate(municipalities):
            try:
                output = f"\rProcessing municipality ({i+1}/{len(municipalities)}): {municipality}                         "
                sys.stdout.write(output)
                sys.stdout.flush()
                self.cursor.execute(query, (municipality,) * len_tuple)
                self.conn.commit()
            except Exception as e:
                print(f"Error processing {municipality}: {e}")
                self.conn.rollback()