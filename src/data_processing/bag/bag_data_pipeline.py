from src.data_processing._common.database_manager import DatabaseManager
from src.data_processing._common.params_manager import ParamsManager
from src.data_processing.bag.xml_importer import XMLImporter
from src.data_processing.bag.geom_column_adder import GeomColumnAdder

class BagDataPipeline(): 
    def __init__(self, data_types=['pand', 'vbo']):
        self.params_manager = ParamsManager()  
        self.database_manager = DatabaseManager()
        self.XML_importer = XMLImporter()
        self.data_types = data_types
        
    def run(self): 
        for data_type in self.data_types: 
            if not self.database_manager.check_if_table_exists(data_type):
                self.database_manager.create_table(data_type)
                self.XML_importer.process_and_insert_xml(data_type)
            else: 
                table_name = self.params_manager.get_table_params(data_type)['table_name']
                print(f'Table {table_name} already exists. Skipping import ...')
        GeomColumnAdder().run()