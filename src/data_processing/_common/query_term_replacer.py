from src.data_processing._common.params_manager import ParamsManager
import re
import pyperclip

class QueryTermReplacer(): 
    def __init__(self): 
        self.params_manager = ParamsManager()
        self.bag_pand_table_name = self.params_manager.get_table_params('pand')['table_name']
        self.bag_vbo_table_name = self.params_manager.get_table_params('vbo')['table_name']
        self.ahn_table_name = self.params_manager.get_table_params('ahn_elevation')['table_name']
        self.housing_table_name = self.params_manager.get_table_params('housing_nl')['table_name']

    def replacer(self, query): 
        query = re.sub(r'\bbag_pand\b', self.bag_pand_table_name, query)
        query = re.sub(r'\bbag_vbo\b', self.bag_vbo_table_name, query)
        query = re.sub(r'\bahn_elevation\b', self.ahn_table_name, query)
        query = re.sub(r'\bhousing_nl\b', self.housing_table_name, query)
        query = query.replace("'Delft'", '%s')
        query = query.replace("'Amsterdam'", '%s')
        return query
    
    def counter(self, query): 
        n_placeholders = query.count('%s')
        return n_placeholders
    
    def run(self, query):
        query = self.replacer(query)
        n_placeholders = self.counter(query)
        print(f'n placeholders: {n_placeholders}')
        pyperclip.copy(query)
        print("Query copied to clipboard!")


if __name__ == '__main__':
    query = '''
            -- Delete rows where municipality = 'Delft' AND status = 'renovation - pre2020'
            DELETE FROM housing_nl
            WHERE 
                municipality = 'Delft' 
                AND status = 'renovation - post2020';

            -- Insert rows from the query into housing_nl
            WITH building_renovations AS (
                SELECT 
                    id_pand, geometry, build_year, 
                    CASE 
                        WHEN status = 'Verbouwing pand' THEN 'renovation - post2020'
                        ELSE status
                    END AS status, 
                    document_date, document_number, registration_start, registration_end, 
                    geom, geom_28992, neighborhood_code, neighborhood, municipality, province
                FROM bag_pand
                WHERE 
                    municipality = 'Delft' 
                    AND status = 'Verbouwing pand'
                    AND LEFT(registration_start, 4)::INTEGER >= 2020
            ), 
            housing_units_inuse AS (
                SELECT * 
                FROM bag_vbo 
                WHERE 
                    municipality = 'Delft' 
                    AND status = 'Verblijfsobject in gebruik'
                    AND function = 'woonfunctie'
                    AND sqm::INTEGER < 9999
            ), 
            housing_units_unique AS (
                SELECT DISTINCT ON (id_vbo) * 
                FROM housing_units_inuse
            ), 
            housing_sqm AS (
                SELECT id_pand, SUM(sqm::INTEGER) AS sqm
                FROM housing_units_unique
                GROUP BY id_pand
            ), 
            housing_sqm_function AS (
                SELECT 'woonfunctie' AS function, *
                FROM housing_sqm
            )
            INSERT INTO housing_nl (
                function, sqm, id_pand, geometry, build_year, status, 
                document_date, document_number, registration_start, registration_end, 
                geom, geom_28992, neighborhood_code, neighborhood, municipality, province
            )
            SELECT 
                h.function, h.sqm, 
                r.id_pand, r.geometry, r.build_year, r.status, 
                r.document_date, r.document_number, r.registration_start, r.registration_end, 
                r.geom, r.geom_28992, r.neighborhood_code, r.neighborhood, r.municipality, r.province
            FROM building_renovations r
            LEFT JOIN housing_sqm_function h
            ON r.id_pand = h.id_pand
            '''
    QueryTermReplacer().run(query)