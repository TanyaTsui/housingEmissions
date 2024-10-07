import time

from data_processing._common.query_runner import QueryRunner
from data_processing._common.database_manager import DatabaseManager
from data_processing._common.params_manager import ParamsManager
from data_processing.bag.xml_importer import XMLImporter
from data_processing.bag.geom_column_adder import GeomColumnAdder
from data_processing._common.index_adder import IndexAdder

from data_processing.landuse.landuse_gml_importer import LanduseGmlImporter
from data_processing.cbs.cbs_data_processor import CBSCsvDownloader, CBSShpDownloader, CBSDataImporter, CBSDataCombiner
from emissions_calculations.embodied_emissions.embodied_emissions_pipeline import NlBuurtenMaker, AdminBoundaryAdder, RenovationInfoAdder, HousingFunctionSqmEstimator, EmbodiedEmissionsCalculator

from data_processing.bag.housing_snapshot_maker import HousingSnapshotMaker

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

class CbsDataPipeline(): 
    def run(self): 
        self.start_time = time.time()
        # CBSShpDownloader().run()
        CBSDataImporter().run()
        # CBSDataCombiner().run()
        self.end_time = time.time()
        print(f'Pipeline took {round((self.end_time - self.start_time)/60, 2)} minutes to run.')

class LanduseDataPipeline(): 
    def run(self): 
        LanduseGmlImporter().run()

class AhnDataPipeline(): 
    def run(self): 
        None

class EmbodiedEmissionsPipeline(): 
    def run(self): 
        self.start_time = time.time()
        # NlBuurtenMaker().run() 
        # AdminBoundaryAdder().run() # run at your own peril - takes an hour
        # RenovationInfoAdder().run()
        # HousingFunctionSqmEstimator().run()
        EmbodiedEmissionsCalculator().run()
        self.end_time = time.time()
        print(f'Pipeline took {round((self.end_time - self.start_time)/60, 2)} minutes to run.')

class OperationalEmissionsPipeline(): 
    def run(self):
        QueryRunner('sql/operational_emissions/calculate_operational_emissions.sql').run_query('Calculating operational emissions...')

class EmissionsAggregator(): 
    def run(self): 
        QueryRunner('sql/create_table/emissions_all.sql').run_query('Creating emissions_all table...')
        QueryRunner('sql/combined_emissions/combine_emissions_by_buurt.sql').run_query_for_each_municipality('Running query to combine emissions...')
    
class S1Pipeline(): 
    def run(self): 
        QueryRunner('sql/create_table/demolished_buildings_nl.sql').run_query('Creating demolished_buildings_nl table...')  
        QueryRunner('sql/data_processing/bag/get_demolished_buildings_nl.sql').run_query_for_each_municipality('Getting demolished buildings...')
        QueryRunner('sql/create_table/housing_nl_s1.sql').run_query('Creating housing_nl_s1 table...')
        QueryRunner('sql/s1_circular_economy/renovation_suitability.sql').run_query_for_each_municipality('Calculating renovation suitability...')
        QueryRunner('sql/create_table/emissions_embodied_housing_nl_s1.sql').run_query('Creating emissions_embodied_housing_nl_s1 table...')
        QueryRunner('sql/embodied_emissions/calculate_emissions/calculate_embodied_emissions_s1.sql').run_query_for_each_municipality('Calculating embodied emissions for strategy one...')

if __name__ == '__main__':
    # # DATA PRE-PROCESSING
    # BagDataPipeline(data_types=['pand']).run()
    # CbsDataPipeline().run()
    # AhnDataPipeline().run()
    # LanduseDataPipeline().run()
    
    # # EXISTING EMISSIONS CALCULATIONS 
    # EmbodiedEmissionsPipeline().run()
    # OperationalEmissionsPipeline().run()
    # EmissionsAggregator().run()
    # TODO: save results to csv

    # # STRATEGY ONE - CIRCULAR ECONOMY (minimize materials) 
    # S1Pipeline().run()
    
    start_time = time.time()
    HousingSnapshotMaker(2012, 2022).run()
    end_time = time.time()
    print(f'\nIt took {round((end_time - start_time)/60, 2)} minutes to create a snapshot of in-use housing in 2012.')
