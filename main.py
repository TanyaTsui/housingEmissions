import time

from data_processing._common.database_manager import DatabaseManager
from data_processing._common.params_manager import ParamsManager
from data_processing.bag.xml_importer import XMLImporter
from data_processing.bag.geom_column_adder import GeomColumnAdder
from data_processing._common.index_adder import IndexAdder

from data_processing.cbs.cbs_data_processor import CBSCsvDownloader, CBSShpDownloader, CBSDataImporter, CBSDataCombiner, CBSDataFormatter
from emissions_calculations.embodied_emissions.embodied_emissions_pipeline import AdminBoundaryAdder, RenovationInfoAdder, HousingFunctionSqmEstimator, EmbodiedEmissionsCalculator

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
    def __init__(self): 
        self.start_time = time.time()
        self.end_time = None

    def run(self): 
        CBSCsvDownloader().run()
        CBSShpDownloader().run()
        CBSDataImporter().run()
        CBSDataCombiner().run()
        CBSDataFormatter().run()
        self.end_time = time.time()
        print(f'Pipeline took {round((self.end_time - self.start_time)/60, 2)} minutes to run.')

class AhnDataPipeline(): 
    def run(self): 
        # AHNDataDownloader().run()
        # GeotiffFileChecker().run()
        # GeotiffFileNumberRenamer().run() 
        # AHNDataToRasterTableImporter().run()
        # ElevationRasterMaker().run()
        None

class EmbodiedEmissionsPipeline(): 
    def __init__(self):
        self.start_time = time.time()
        self.end_time = None

    def run(self): 
        # AdminBoundaryAdder().run()
        RenovationInfoAdder().run()
        HousingFunctionSqmEstimator().run()
        EmbodiedEmissionsCalculator().run()
        self.end_time = time.time()
        print(f'Pipeline took {round((self.end_time - self.start_time)/60, 2)} minutes to run.')


class OperationalEmissionsPipeline(): 
    # process cbs data in database for operational emissions
    # calculate operational emissions  
    None

class EmissionsAggregator(): 
    # aggregate embodied and operational emissions by buurt 
    None

if __name__ == '__main__':
    # BagDataPipeline(data_types=['pand']).run()
    # CbsDataPipeline().run()
    # AhnDataPipeline().run()
    # EmbodiedEmissionsPipeline().run()
    OperationalEmissionsPipeline().run()
    # EmissionsAggregator().run()