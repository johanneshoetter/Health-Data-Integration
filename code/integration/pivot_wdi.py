from code.utils.dataframe_utils import df_to_sql

import pandas as pd

def pivot_wdi(engine):

    print("Beggining pivotization for wdi data")

    df = pd.read_sql_table('wdidata', con=engine)
    df = pd.melt(df, id_vars=['Country Code', 'Indicator Code'],
                 value_vars=[str(val) for val in range(1960, 2019, 1)]).dropna()
    df_to_sql(df, 'wdidata_pivot', engine, if_exists='replace')

    print("Finished pivotization")
    return ['wdidata']