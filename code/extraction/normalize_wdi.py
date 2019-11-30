import pandas as pd
from code.utils.dataframe_utils import df_to_sql

from code.config import mapping_cols

def normalize_wdi(engine):
    df = pd.read_sql_table('wdifootnote', con=engine)
    mappings_description = {}
    descriptions = list(df['DESCRIPTION'].unique())
    for idx, description in enumerate(descriptions):
        mappings_description[description] = idx
    df['DESCRIPTION_ID'] = df['DESCRIPTION'].apply(lambda x: mappings_description[x])
    df.drop('DESCRIPTION', axis=1, inplace=True)
    df_to_sql(df, 'wdifootnote_ids', engine, if_exists='replace')

    values = []
    for key, val in mappings_description.items():
        values.append([val, key])
    descriptions_df = pd.DataFrame(data=values, columns=mapping_cols)
    df_to_sql(descriptions_df, 'wdifootnote_descriptions', engine, if_exists='replace')