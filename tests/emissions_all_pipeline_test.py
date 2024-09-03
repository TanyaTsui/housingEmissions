''' 
test for creation of cbs_map_all
- check if there are any mismatching neighborhood codes between cbs_map_all and nl_buurten 
- for each year, check if sum of cbs_map_{year} is equal to cbs_map_all
'''

from sqlalchemy import create_engine
import pandas as pd

class cbsMapAllTester(): 
    def __init__(self): 
        self.engine = create_engine(f'postgresql://postgres:Tunacompany5694!@localhost:5432/urbanmining')
        self.columns_of_interest_old = ['AANT_INW', 'AANTAL_HH', 'WOZ', 'G_GAS_TOT', 'G_ELEK_TOT']
        self.columns_of_interest_new = ['population', 'n_households', 'woz', 'gas_m3', 'electricity_kwh']

    def check_neighborhood_codes(self): 
        print('Checking mismatching neighborhood codes between cbs_map_all and nl_buurten')
        query = ''' 
        SELECT * 
        FROM nl_buurten n 
        FULL JOIN cbs_map_all c  
        ON c.neighborhood_code = n.neighborhood_code 
        WHERE c.neighborhood_code IS NULL OR n.neighborhood_code IS NULL
        '''
        df = pd.read_sql(query, self.engine)
        if df.shape[0] == 0: 
            print('No mismatching neighborhood codes between cbs_map_all and nl_buurten\n')
        else: 
            print('Mismatching neighborhood codes between cbs_map_all and nl_buurten')
            print(df, '\n')

    def check_sum_of_cbs_map(self): 
        for year in range(2012, 2022): 
            print(f'\nChecking sum of cbs_map_all for {year}')
            for i, col_new in enumerate(self.columns_of_interest_new): 
                col_old = self.columns_of_interest_old[i]
                sum_old = pd.read_sql(f'SELECT SUM("{col_old}") FROM cbs_map_{year} WHERE "{col_old}" > 0', self.engine).iloc[0, 0]
                sum_new = pd.read_sql(f'SELECT SUM({col_new}) FROM cbs_map_all WHERE year = {year}', self.engine).iloc[0, 0]
                if sum_old == sum_new:
                    print(f'{col_new} is correct')
                else:
                    print(f'{col_new} is incorrect. difference: {sum_old - sum_new}')

class EmissionsEmbodiedHousingNlTester(): 
    def __init__(self): 
        self.engine = create_engine(f'postgresql://postgres:Tunacompany5694!@localhost:5432/urbanmining')

    def check_neighborhood_codes(self): 
        print('Checking mismatching neighborhood codes between emissions_embodied_housing_nl and nl_buurten')
        query = ''' 
        SELECT * 
        FROM nl_buurten n 
        FULL JOIN emissions_embodied_housing_nl c
        ON c.neighborhood_code = n.neighborhood_code 
        WHERE c.neighborhood_code IS NULL OR n.neighborhood_code IS NULL
        '''
        df = pd.read_sql(query, self.engine)
        if df.shape[0] == 0: 
            print('No mismatching neighborhood codes between cbs_map_all and nl_buurten\n')
        else: 
            print('Mismatching neighborhood codes between cbs_map_all and nl_buurten')
            print(df, '\n')

    def check_sqm(self): 
        for year in range(2012, 2022): 
            print(f'\nChecking sum of emissions_embodied_housing_nl for {year}')
            housing_nl_query = f''' 
            SELECT SUM(sqm) FROM housing_nl 
            WHERE LEFT(registration_start, 4)::INTEGER = {year}
            '''
            sum_housing_nl = pd.read_sql(housing_nl_query, self.engine).iloc[0, 0]
            sum_emissions_embodied = pd.read_sql(f'SELECT SUM(sqm) FROM emissions_embodied_housing_nl WHERE year = {year}', self.engine).iloc[0, 0]
            if sum_housing_nl == sum_emissions_embodied:
                print(f'sqm is correct')
            else:
                print(f'sqm is incorrect. difference: {sum_housing_nl - sum_emissions_embodied}')

class emissionsAllTester(): 
    def __init__(self): 
        self.engine = create_engine(f'postgresql://postgres:Tunacompany5694!@localhost:5432/urbanmining')

    def check_neighborhood_codes(self):
        print('Checking mismatching neighborhood codes between emissions_all and nl_buurten')
        query = ''' 
        SELECT * 
        FROM nl_buurten n 
        FULL JOIN emissions_all e 
        ON e.neighborhood_code = n.neighborhood_code 
        WHERE e.neighborhood_code IS NULL OR n.neighborhood_code IS NULL
        '''
        df = pd.read_sql(query, self.engine)
        if df.shape[0] == 0: 
            print('No mismatching neighborhood codes between emissions_all and nl_buurten\n')
        else: 
            print('Mismatching neighborhood codes between emissions_all and nl_buurten')
            print(df, '\n')

    def check_operational_emissions(self):
        for year in range(2012, 2022): 
            print(f'\nChecking sum of operational emissions for {year}')
            sum_cbs_map_all = pd.read_sql(f'SELECT SUM(emissions_kg_total) FROM cbs_map_all WHERE year = {year}', self.engine).iloc[0, 0]
            sum_emissions_all = pd.read_sql(f'SELECT SUM(emissions_operational) FROM emissions_all WHERE year = {year}', self.engine).iloc[0, 0]
            if sum_cbs_map_all == sum_emissions_all:
                print(f'operational emissions is correct')
            else:
                print(f'operational emissions is incorrect. difference: {sum_cbs_map_all - sum_emissions_all}')

    def check_embodied_emissions(self): 
        for year in range(2012, 2022): 
            print(f'\nChecking sum of embodied emissions for {year}')
            sum_embodied = pd.read_sql(f'SELECT SUM(emissions_embodied_kg) FROM emissions_embodied_housing_nl WHERE year = {year}', self.engine).iloc[0, 0]
            sum_emissions_all = pd.read_sql(f'SELECT SUM(emissions_operational) FROM emissions_all WHERE year = {year}', self.engine).iloc[0, 0]
            if sum_embodied == sum_emissions_all:
                print(f'embodied emissions is correct')
            else:
                print(f'embodied emissions is incorrect. difference: {sum_embodied - sum_emissions_all}')
                print(f'sum_embodied: {sum_embodied}, sum_emissions_all: {sum_emissions_all}')


if __name__ == '__main__': 
    # # test cbs_map_all
    # tester = cbsMapAllTester()
    # tester.check_neighborhood_codes()
    # tester.check_sum_of_cbs_map()

    # # test emissions_embodied_housing_nl
    # tester = EmissionsEmbodiedHousingNlTester()
    # tester.check_neighborhood_codes()
    # tester.check_sqm()

    # test emissions_all
    tester = emissionsAllTester()
    # tester.check_neighborhood_codes()
    # tester.check_operational_emissions()
    tester.check_embodied_emissions()