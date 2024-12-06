import psycopg2
import subprocess
from sqlalchemy import create_engine
from .params_manager import ParamsManager

class DatabaseManager:    
    def __init__(self):
        self.params_manager = ParamsManager()

    def connect(self):
        db_params = self.params_manager.get_database_params()
        return psycopg2.connect(**db_params)
    
    def get_sqlalchemy_engine(self):
        db_params = self.params_manager.get_database_params()
        connection_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        engine = create_engine(connection_string)
        return engine
    
    def check_if_table_exists(self, data_type):
        table_name = self.params_manager.get_table_params(data_type)['table_name']
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(f'''
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = '{table_name}'
            );
        ''')
        exists = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return exists

    def create_table(self, data_type):
        conn = self.connect()
        cursor = conn.cursor()
        
        table_params = self.params_manager.get_table_params(data_type)
        table_name = table_params['table_name']
        columns_sql = table_params['columns_sql']
                
        print(f'Creating table {table_name} ...')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});
        ''')
        
        conn.commit()
        cursor.close()
        conn.close()
    
    def get_municipalities_list(self): 
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute('''SELECT DISTINCT "GM_NAAM" FROM cbs_map_2022 WHERE "WATER" = 'NEE';''')
        municipalities_list = cursor.fetchall()
        return [municipality[0] for municipality in municipalities_list]
        # return [''''s-Gravenhage'''] # for testing
    
    def add_file_to_db(self, gpkg_file_path):
        db_params = self.params_manager.get_database_params()
        ogr2ogr_command = [
            "ogr2ogr",
            "-f", "PostgreSQL",
            f"PG:host={db_params['host']} dbname={db_params['dbname']} user={db_params['user']} password={db_params['password']} port={db_params['port']}",
            gpkg_file_path
        ]
        subprocess.run(ogr2ogr_command, check=True)