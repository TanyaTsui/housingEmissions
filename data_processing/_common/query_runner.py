import sys
import psycopg2
import geopandas as gpd
import pandas as pd
from pathlib import Path
from data_processing._common.database_manager import DatabaseManager
from data_processing._common.query_term_replacer import QueryTermReplacer

class QueryRunner(): 
    def __init__(self, file_path): 
        self.db_manager = DatabaseManager()
        self.conn = self.db_manager.connect()
        self.cursor = self.conn.cursor()
        self.engine = self.db_manager.get_sqlalchemy_engine()
        self.query, self.n_placeholders = self.get_query_info(file_path)

    def get_query_info(self, file_path):
        query = self._read_query_from_file(file_path)
        query_modified = QueryTermReplacer().replacer(query)
        n_placeholders = QueryTermReplacer().counter(query_modified)
        return query_modified, n_placeholders

    def _read_query_from_file(self, file_path):
        sql_file = Path(file_path)
        if sql_file.exists():
            with open(sql_file, 'r') as file:
                query = file.read()
            return query
        else:
            raise FileNotFoundError(f"File not found: {file_path}")   
    
    def run_query(self, message=''): 
        if message != '': 
            print(message)
        self.conn.rollback()
        self.cursor.execute(self.query)
        self.conn.commit()
        print('Done!\n')

    def get_dataframe_from_query(self): 
        df = pd.read_sql(self.query, self.engine)
        return df
    
    def get_geodataframe_from_query(self, geom_col='geom'):
        gdf = gpd.read_postgis(self.query, self.engine, geom_col=geom_col)
        return gdf

    def run_query_for_one_municipality(self, municipality, message=''): 
        if message != '': 
            print(message)
        self.conn.rollback()
        self.cursor.execute(self.query, (municipality,) * self.n_placeholders)
        self.conn.commit()
        if message != '':
            print('Done!\n')

    def run_query_for_each_municipality(self, message=''):
        if message != '': 
            print(message)
        municipalities = self.db_manager.get_municipalities_list()
        # municipalities = ['Het Hogeland', 'Zaanstad', 'Stadskanaal']
        # municipalities = ['Amsterdam'] # get rid of this line, just for testing first 3 municipalities
        for i, municipality in enumerate(municipalities):
            try:
                output = f"\rProcessing municipality ({i+1}/{len(municipalities)}): {municipality}                         "
                sys.stdout.write(output)
                sys.stdout.flush()
                self.cursor.execute(self.query, (municipality,) * self.n_placeholders)
                self.conn.commit()
            except Exception as e:
                print(f"Error processing {municipality}: {e}")
                self.conn.rollback()
        print('\nDone!\n')

    def run_query_for_each_year(self, first_year, final_year, message=''):
        if message != '': 
            print(message)
        for year in range(first_year, final_year+1):
            output = f"\rProcessing year: {year}                         "
            sys.stdout.write(output)
            sys.stdout.flush()
            self.cursor.execute(self.query, (year,) * self.n_placeholders)
            self.conn.commit()
        print('\nDone!\n')

    def run_query_to_combine_emissions(self, message=''):
        if message != '': 
            print(message) 
        municipalities = self.db_manager.get_municipalities_list()
        # municipalities = ['Amsterdam'] # , "'s-Gravenhage"] # testing municipalitites 
        for i, municipality in enumerate(municipalities): 
            for year in range(2012, 2025): 
                output = f"\rCombining emissions for ({i+1}/{len(municipalities)}): {municipality} for year {year}                         "
                sys.stdout.write(output)
                sys.stdout.flush()
                self.cursor.execute(self.query, (year, municipality, year, municipality, year, municipality))
                self.conn.commit()