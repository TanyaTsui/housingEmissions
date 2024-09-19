import pandas as pd
import geopandas as gpd
import math
import numpy as np
import os
import re
import zipfile
import shutil
import psycopg2
from sqlalchemy import create_engine

from data_processing._common.params_manager import ParamsManager
from data_processing._common.query_runner import QueryRunner

class CBSCsvDownloader(): 
    # not really needed - data is already in shp files
    # see CBSShpDownloader

    def __init__(self):
        self.columns_of_interest = [
            'gwb_code_10', 'regio', 'gm_naam', 'recs', 
            'a_inw', 'a_hh', 'a_woning', 'g_ele', 'g_gas', 'p_stadsv'
        ]
        print('saving cbs data to csv...')
    
    def run(self): 
        self.read_data_2012()
        self.change_column_names_2012()
        self.add_missing_columns_2012()
        # TODO: self.add_woz_columns_pre2020()
        self.combine_data()
        self.format_data(columns_list=['g_ele', 'g_gas', 'p_stadsv'])
        self.save_data()

    def read_data_2012(self):
        dtype_dict = {
            'WK_CODE': str, 'BU_CODE': str, 
            'Code_10_pos12': str, 'GWB_CODE12': str, 
            'GM_CODE': str
        }
        df_file_2012 = pd.read_excel('data/raw/cbs/kwb-2012.xls', converters=dtype_dict)
        self.data_2012 = df_file_2012

    def change_column_names_2012(self): 
        column_conversion_dict = {
            'GWB_NAAM12_60POS': 'regio',
            'GEM_NAAM': 'gm_naam', 
            'AANT_INW': 'a_inw', 
            'AANTAL_HH': 'a_hh',
            'WONINGEN': 'a_woning', 
            'P_ELEK_TOT': 'g_ele',
            'P_GAS_TOT': 'g_gas', 
            'P_STADVERW': 'p_stadsv'
        }
        self.data_2012.rename(columns=column_conversion_dict, inplace=True)

    def add_missing_columns_2012(self): 
        self._add_recs()
        self._add_gwb_code_10()
    
    def _add_recs(self): 
        def _lambda_make_recs_code(row): 
            recs_dict = {
                'B': 'BU', 'W': 'WK', 'G': 'GM', 'N': 'NL'
            }
            return recs_dict[row.RECS]
        def _lambda_make_recs(row): 
            recs_dict = {
                'B': 'Buurt', 'W': 'Wijk', 'G': 'Gemeente', 'N': 'Land'
            }
            return recs_dict[row.RECS]
        self.data_2012['recs_code'] = self.data_2012.apply(lambda row: _lambda_make_recs_code(row), axis=1)
        self.data_2012['recs'] = self.data_2012.apply(lambda row: _lambda_make_recs(row), axis=1)

    def _add_gwb_code_10(self):
        def _lambda_make_gwb_code_10(row): 
            if pd.isna(row.WK_CODE): 
                row.WK_CODE = ''
            if pd.isna(row.BU_CODE):
                row.BU_CODE = ''
            return f'{row.recs_code}{row.GM_CODE}{row.WK_CODE}{row.BU_CODE}'
        self.data_2012['gwb_code_10'] = self.data_2012.apply(lambda row: _lambda_make_gwb_code_10(row), axis=1)

    def add_woz_columns_pre2020(self):
        None
        # add missing woz columns for pre-2020 data 
    
    def combine_data(self): 
        df_list = []
        self.data_2012 = self.data_2012[self.columns_of_interest]
        self.data_2012['year'] = 2012
        print('Adding data from 2012...')
        df_list.append(self.data_2012)

        for year in range(2013, 2023):
            print(f'Adding data from {year}...')
            extension = 'xlsx' if year > 2018 else 'xls'
            df = pd.read_excel(f'data/raw/cbs/kwb-{year}.{extension}') # remove nrows later
            df = df[self.columns_of_interest]
            df['year'] = year
            df_list.append(df)

        self.data_all = pd.concat(df_list)

    def format_data(self, columns_list):
        df = self.data_all
        for column in columns_list: 
            df[column] = df[column].str.strip()
            df[column] = df[column].str.replace(',', '.', regex=False)
            df[column] = pd.to_numeric(df[column], errors='coerce')
        self.data_all = df

    def save_data(self): 
        file_path = 'data/processed/cbs/kwb-all.csv'
        self.data_all.to_csv(file_path, index=False)
        print(f'Saved data to {file_path}')

class CBSShpDownloader(): 
    def run(self): 
        # self.rename_zip_files()
        self.save_buurt_data()

    def rename_zip_files(self): 
        directory = 'data/raw/cbs/wijkEnBuurtKaart'
        year_pattern = re.compile(r'(201[2-9]|202[0-3])')
        for filename in os.listdir(directory):
            if filename.endswith('.zip'):
                match = year_pattern.search(filename)
                if match:
                    year = match.group(0)
                    new_filename = f"wijk_en_buurt_kaart_{year}.zip"
                    old_file = os.path.join(directory, filename)
                    new_file = os.path.join(directory, new_filename)
                    os.rename(old_file, new_file)
    
    def save_buurt_data(self): 
        buurt_filePaths_inZip = {}
        for year in range(2012, 2023):
            buurt_filePaths_inZip[year] = self._get_buurt_file_paths(year) 
            self._save_buurt_data(year, buurt_filePaths_inZip[year])

    def _get_buurt_file_paths(self, year): 
        zip_file_path = f'data/raw/cbs/wijkEnBuurtKaart/wijk_en_buurt_kaart_{year}.zip'
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_contents = zip_ref.namelist()
            buurt_files = [filename for filename in zip_contents if 'buurt' in filename]
            return buurt_files
        
    def _save_buurt_data(self, year, file_paths):
        shp_extensions = {'.shp', '.shx', '.dbf', '.prj', '.cpg', '.sbn', '.sbx', '.xml'}
        zip_file_path = f'data/raw/cbs/wijkEnBuurtKaart/wijk_en_buurt_kaart_{year}.zip'
        target_folder = f'data/raw/cbs/wijkEnBuurtKaart/shp'
        with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
            for file_path in file_paths: 
                ext = os.path.splitext(file_path)[-1].lower()
                if ext in shp_extensions:
                    source = zip_file.open(file_path)
                    new_file_name = f'buurt_{year}{ext}'
                    target_path = os.path.join(target_folder, new_file_name)
                    with open(target_path, 'wb') as target_file:
                        shutil.copyfileobj(source, target_file)

class CBSDataImporter(): 
    def __init__(self): 
        self.params_manager = ParamsManager()
        self.db_name = self.params_manager.database_params['dbname']
        self.db_user = self.params_manager.database_params['user']
        self.db_password = self.params_manager.database_params['password']
        self.db_host = self.params_manager.database_params['host']
        self.db_port = self.params_manager.database_params['port']
        self.engine = create_engine(f'postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')

    def run(self): 
        # self.import_csv_to_db() # not really needed - data is already in shp files
        # self.import_shps_to_db()
        self.import_gemeenten_to_db('data/raw/nl/nl_adminBoundaries/gemeenten_2022_v2.shp', 'nl_gemeenten')
    
    def import_csv_to_db(self):
        csv_file_path = 'data/processed/cbs/kwb-all.csv'
        table_name = 'cbs_stats_all'
        df = pd.read_csv(csv_file_path)
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)
        print(f'Imported {csv_file_path} to {table_name} in database.')

    def import_shps_to_db(self):
        for year in range(2012, 2023): 
            shp_file_path = f'data/raw/cbs/wijkEnBuurtKaart/shp/buurt_{year}.shp'
            table_name = f'cbs_map_{year}' 
            gdf = gpd.read_file(shp_file_path)
            gdf.to_postgis(table_name, self.engine, if_exists='replace')
            print(f'Imported {shp_file_path} to {table_name} in database.')

    def import_gemeenten_to_db(self, shp_file_path, table_name): 
        gdf = gpd.read_file(shp_file_path)
        gdf.to_postgis(table_name, self.engine, if_exists='replace')
        print(f'Imported {shp_file_path} to {table_name} in database.')

class CBSDataCombiner(): 
    def run(self): 
        QueryRunner('sql/create_table/cbs_map_all.sql').run_query('creating cbs_map_all table...')
        QueryRunner('sql/data_processing/cbs/make_cbs_map_all.sql').run_query_for_each_year(first_year=2012, final_year=2021, message='making cbs_map_all...')
