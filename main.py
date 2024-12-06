from src.data_processing.cbs.cbs_data_processor import CBSDataProcessor, CBSDataHarmoniser
from src.emissions_calculations.embodied_emissions_pipeline import AdminBoundaryAdder, ConstructionActivityInfoAdder
from src.emissions_calculations.emissions_calculations import EmissionsCalculator
from src.data_processing.bag.housing_snapshot_maker import HousingSnapshotMaker, HousingSnapshotBuurtStatsAdder
from src.scenarios.scenarios_emissions import s1EnergyEfficiency, s2CircularEconomy

import time 

if __name__ == '__main__':
    start_time = time.time()

    # SET STUDY PERIOD (start_year t/m end_year)
    start_year = 2012
    end_year = 2021

    # DATA HARMONISATION - ALREADY RAN, NO NEED TO RUN AGAIN 
    # # # # # AdminBoundaryAdder().run() # add wijk-level geoms to bag_pand and bag_vbo - this takes a long time. 1-2% of data is lost in the process.
    # # # # # CBSDataProcessor().run() # harmonise cbs data using wijk-level geoms, aggregated to cbs_map_all_wijk 
    # # # # # ConstructionActivityInfoAdder().run() # add construction, renovation, transformation, and demolition data to housing_nl
    # # # # # HousingSnapshotMaker(start_year, end_year).run() # caution - takes hours to run
    HousingSnapshotBuurtStatsAdder(start_year, end_year).run() # add buurt-level stats to housing_inuse_2012_2021. Takes ±7 hours to run
    CBSDataHarmoniser().run() # harmonise cbs data using buurt-level geoms, aggregated to cbs_map_all_buurt

    # EXISTING EMISSIONS CALCULATIONS 
    # EmissionsCalculator(start_year, end_year).run() # calculate embodied and operational emissions 

    # # STRATEGY ONE - ENERGY EFFICIENCY (minimize energy use)
    # s1EnergyEfficiency(start_year, end_year).run()

    # # STRATEGY TWO - CIRCULAR ECONOMY (minimize materials) 
    # s2CircularEconomy().run()

    # # STRATEGY THREE - SMALLER HOMES (minimize sqm)
    
    end_time = time.time()
    print(f"\nTime taken: {round((end_time - start_time)/60, 2)} minutes")
    None 


