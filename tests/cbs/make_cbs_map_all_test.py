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

    def check_mismatching_neighborhood_codes(self): 
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

if __name__ == '__main__': 
    tester = cbsMapAllTester()
    tester.check_mismatching_neighborhood_codes()
    tester.check_sum_of_cbs_map()