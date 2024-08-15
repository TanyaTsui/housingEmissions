from data_processing._common.database_manager import DatabaseManager
from data_processing._common.params_manager import ParamsManager
from data_processing.bag.xml_importer import XMLImporter
from data_processing.bag.geom_column_adder import GeomColumnAdder
from data_processing._common.index_adder import IndexAdder

from emissions_calculations.embodied_emissions.admin_boundary_adder import AdminBoundaryAdder
from emissions_calculations.embodied_emissions.renovation_info_adder import RenovationInfoAdder
from emissions_calculations.embodied_emissions.housing_function_sqm_estimator import HousingFunctionSqmEstimator
from emissions_calculations.embodied_emissions.embodied_emissions_calculator import EmbodiedEmissionsCalculator

class BagDataImporter(): 
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

class CbsDataImporter(): 
    # download cbs data (using API?)
    # process and insert cbs data into database
    None

class AhnDataImporter(): 
    def run(self): 
        # AHNDataDownloader().run()
        # GeotiffFileChecker().run()
        # GeotiffFileNumberRenamer().run() 
        # AHNDataToRasterTableImporter().run()
        # ElevationRasterMaker().run()
        None

class EmbodiedEmissionsPipeline(): 
    def run(self): 
        # AdminBoundaryAdder().run()
        RenovationInfoAdder().run() # TODO: add renovation info (pre-2020, post-2020, function change, new units) into housing_nl
        # HousingFunctionSqmEstimator().run()
        # EmbodiedEmissionsCalculator().run()


class OperationalEmissionsPipeline(): 
    # process cbs data in database for operational emissions
    # calculate operational emissions  
    None

class EmissionsAggregator(): 
    # aggregate embodied and operational emissions by buurt 
    None

if __name__ == '__main__':
    # BagDataImporter(data_types=['pand']).run()
    # CbsDataImporter().run()
    # AhnDataImporter().run()
    EmbodiedEmissionsPipeline().run()
    # OperationalEmissionsPipeline().run()
    # EmissionsAggregator().run()