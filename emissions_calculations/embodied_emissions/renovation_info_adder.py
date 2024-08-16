from data_processing._common.query_runner import QueryRunner
import time

class RenovationInfoAdder(): 
    def run(self):
        # start time
        QueryRunner('sql/create_table/housing_nl.sql').run_query('Creating housing_nl table...')
        QueryRunner('sql/embodied_emissions/renovation/addPre2020Renovation_toHousingNL.sql').run_query_for_each_municipality('Adding pre-2020 renovations to housing_nl...')
        QueryRunner('sql/embodied_emissions/renovation/addPost2020Renovation_toHousingNL.sql').run_query_for_each_municipality('Adding post-2020 renovations to housing_nl...')
        QueryRunner('sql/embodied_emissions/renovation/transformation_functionChange.sql').run_query_for_each_municipality('Adding transformations from function change to housing_nl...')
        QueryRunner('sql/embodied_emissions/renovation/transformation_addUnits.sql').run_query_for_each_municipality('Adding transformations from adding units to housing_nl...')
        QueryRunner('sql/embodied_emissions/renovation/remove_renovation_duplicates.sql').run_query_for_each_municipality('Removing renovation duplicates from housing_nl...')