class ParamsManager:
    def __init__(self):
        self.database_params = {
            'dbname': 'urbanmining', 
            'user': 'postgres', 
            'host': 'localhost', 
            'password': 'Tunacompany5694!', 
            'port': '5432'
        } 
        self.table_params = {
            'vbo': {
                'table_name': 'bag_vbo',
                'columns_sql': '''
                    id_vbo VARCHAR, id_num VARCHAR, id_pand VARCHAR, 
                    geometry VARCHAR, function VARCHAR, sqm VARCHAR, status VARCHAR, 
                    document_date VARCHAR, document_number VARCHAR, 
                    registration_start VARCHAR, registration_end VARCHAR
                    ''', 
                'column_names': '(id_vbo, id_num, id_pand, geometry, function, sqm, status, document_date, document_number, registration_start, registration_end)',   
            }, 
            'pand': {
                'table_name': 'bag_pand',
                'columns_sql': '''
                    id_pand VARCHAR, geometry VARCHAR, build_year VARCHAR, status VARCHAR, 
                    document_date VARCHAR, document_number VARCHAR, 
                    registration_start VARCHAR, registration_end VARCHAR''', 
                'column_names': '(id_pand, geometry, build_year, status, document_date, document_number, registration_start, registration_end)'
            },
            'num': {
                'table_name': 'bag_num',
                'columns_sql': '''
                    id_num VARCHAR, 
                    house_number VARCHAR, house_letter VARCHAR, post_code VARCHAR, 
                    document_date VARCHAR, document_number VARCHAR, 
                    registration_start VARCHAR, registration_end VARCHAR''', 
                'column_names': '(id_num, house_number, house_letter, post_code, document_date, document_number, registration_start, registration_end)',
            }, 
            'ahn_elevation': {
                'table_name': 'ahn_elevation',
            }, 
            'housing_nl': {
                'table_name': 'housing_nl',
            }
        } 
        self.namespace_params = {
            'vbo': {
                'ns2': 'www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601',
                'ns3': 'www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601',
                'ns4': 'www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601',
                'ns5': 'http://www.opengis.net/gml/3.2'
            },
            'pand': {
                'lvbag': 'www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601',
                'gml': 'http://www.opengis.net/gml/3.2',
                'hist': 'www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601'
            },
            'num': {
                'lvbag': 'www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601',
                'hist': 'www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601'
            }
        }
        self.xml_params = {
            # zip_file_name, extract_function 
            'vbo': {
                'zip_file_name': '9999VBO08082024.zip',
                'extract_function': 'extract_vbo_data'
            },
            'pand': {
                'zip_file_name': '9999PND08082024.zip',
                'extract_function': 'extract_pand_data'
            },
            'num': {
                'zip_file_name': '9999NUM08082024.zip',
                'extract_function': 'extract_num_data'
            }
        } 

    def get_namespace_params(self, data_type):
        return self.namespace_params[data_type]
    
    def get_database_params(self):
        return self.database_params
    
    def get_table_params(self, data_type):
        return self.table_params[data_type]
    
    def get_xml_params(self, data_type):
        return self.xml_params[data_type]
