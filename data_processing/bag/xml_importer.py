import zipfile
import io
import xml.etree.ElementTree as ET
from .._common.database_manager import DatabaseManager
from .._common.params_manager import ParamsManager
from .data_extractor import DataExtractor

class XMLImporter:
    def __init__(self, batch_size=1000):
        self.db_manager = DatabaseManager()
        self.params_manager = ParamsManager()
        self.batch_size = batch_size

    def create_params(self, data_type): 
        self.table_params = self.params_manager.get_table_params(data_type)
        self.namespace_params = self.params_manager.get_namespace_params(data_type)
        self.xml_params = self.params_manager.get_xml_params(data_type)

    def insert_batch(self, cursor, insert_query_prefix, values_list, placeholders):
        if not values_list:
            return
        query = insert_query_prefix + ','.join(cursor.mogrify(f'({placeholders})', x).decode('utf-8') for x in values_list)
        cursor.execute(query)
        values_list.clear() 

    def process_and_insert_xml(self, data_type):
        db_manager = self.db_manager
        self.create_params(data_type)

        zip_file_name = self.xml_params['zip_file_name']
        extract_function_name = self.xml_params['extract_function']
        table_name = self.table_params['table_name']
        column_names = self.table_params['column_names']
        
        insert_query_prefix = f'INSERT INTO {table_name} {column_names} VALUES '
        columns_list = column_names.strip('()').replace(" ", "").split(',')
        num_columns = len(columns_list)
        placeholders = ', '.join(['%s'] * num_columns)

        values_list = []

        with zipfile.ZipFile('data/raw/bag/lvbag-extract-nl.zip', 'r') as outer_zip:
            with outer_zip.open(zip_file_name) as inner_zip:
                with zipfile.ZipFile(io.BytesIO(inner_zip.read())) as zfile:
                    xml_names = zfile.namelist()
                    print(f'Found {len(xml_names)} XML files in {zip_file_name} ...')

                    for xml_name in xml_names:
                        print(xml_name)
                        with zfile.open(xml_name) as xml:
                            tree = ET.parse(xml)
                            root = tree.getroot()
                            data_extractor = DataExtractor(root, self.namespace_params)
                            extract_function = getattr(data_extractor, extract_function_name)
                            data_batch = extract_function()
                            values_list.extend(data_batch)

                            if len(values_list) >= self.batch_size:
                                # Insert batch into the database
                                with db_manager.connect() as conn:
                                    with conn.cursor() as cursor:
                                        self.insert_batch(cursor, insert_query_prefix, values_list, placeholders)

                    # Insert any remaining data
                    if values_list:
                        with db_manager.connect() as conn:
                            with conn.cursor() as cursor:
                                self.insert_batch(cursor, insert_query_prefix, values_list, placeholders)
