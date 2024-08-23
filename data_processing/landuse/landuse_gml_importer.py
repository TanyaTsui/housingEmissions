import geopandas as gpd
from sqlalchemy import create_engine
import psycopg2

from data_processing._common.params_manager import ParamsManager
from data_processing._common.query_runner import QueryRunner

class LanduseGmlImporter():
    def __init__(self): 
        self.landuse_gml_path = 'data/raw/landuse/landuse.gml'
        self.table_name_raw = 'landuse_nl_raw'
        
        self.dbname = ParamsManager().get_database_params()['dbname']
        self.user = ParamsManager().get_database_params()['user']
        self.host = ParamsManager().get_database_params()['host']
        self.password = ParamsManager().get_database_params()['password']
        self.port = ParamsManager().get_database_params()['port']

    def run(self):
        self.import_raw_gml()
        self.process_gml()

    def import_raw_gml(self): 
        engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}')
        print('reading landuse gml file ...')
        gdf = gpd.read_file(self.landuse_gml_path)
        gdf = gdf.to_crs(epsg=28992)
        print(f'writing landuse gml file to db as {self.table_name_raw}...')
        gdf.to_postgis(self.table_name_raw, engine, if_exists='replace')

    def process_gml(self):
        QueryRunner('sql/create_table/landuse_nl.sql').run_query('Creating landuse_nl table...')
        QueryRunner('sql/data_processing/bag/match_admin_boundaries_landuse.sql').run_query_for_each_municipality('Matching landuse_nl to admin boundaries...')
