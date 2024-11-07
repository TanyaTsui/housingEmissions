from src.data_processing.cbs.cbs_data_processor import CBSDataProcessor
from src.emissions_calculations.embodied_emissions_pipeline import AdminBoundaryAdder, ConstructionActivityInfoAdder
from src.emissions_calculations.emissions_calculations import EmissionsCalculator
from src.data_processing.bag.housing_snapshot_maker import HousingSnapshotMaker
from src.scenarios.s2_circular_economy import s2CircularEconomy

if __name__ == '__main__':
    # DATA HARMONISATION - ALREADY RAN, NO NEED TO RUN AGAIN
    # # # # # AdminBoundaryAdder().run() # add wijk-level geoms to bag_pand and bag_vbo - this takes a long time. 1-2% of data is lost in the process.
    # CBSDataProcessor().run() # harmonise cbs data using wijk-level geoms, aggregated to cbs_map_all_wijk 
    
    # EXISTING EMISSIONS CALCULATIONS 
    # ConstructionActivityInfoAdder().run() # add construction, renovation, transformation, and demolition data to housing_nl
    # EmissionsCalculator().run() # calculate embodied and operational emissions 

    # # STRATEGY ONE - ENERGY EFFICIENCY (minimize energy use)

    # # STRATEGY TWO - CIRCULAR ECONOMY (minimize materials) 
    # s2CircularEconomy().run()
    
    # # SNAPSHOT OF HOUSING IN USE
    HousingSnapshotMaker(2012, 2022).run() # caution - takes hours to run

    None 


