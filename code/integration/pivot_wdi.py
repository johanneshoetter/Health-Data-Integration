from code.utils.dataframe_utils import df_to_sql

import pandas as pd

def pivot_wdi(engine):

    print("Beggining pivotization for wdi data")

    df = pd.read_sql_table('wdidata', con=engine)
    df = pd.melt(df, id_vars=['Country Code', 'Indicator Code'],
                 value_vars=[str(val) for val in range(1960, 2019, 1)]).dropna()
    df_to_sql(df, 'wdidata_pivot', engine, if_exists='replace')

    df = pd.read_sql_table('wdifootnote_ids', con=engine)
    df['Year'] = df['Year'].apply(lambda x: x.replace('YR', ''))
    df_to_sql(df, 'wdifootnote_ids', engine, if_exists='replace')

    print("Finished pivotization")
    return ['wdidata', 'wdifootnote'] # footnote isn't used in the method, but also must be cleaned