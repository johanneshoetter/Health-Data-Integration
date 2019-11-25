import pandas as pd
from code.utils.dataframe_utils import df_to_sql
from code.config import mapping_cols

def normalize_ihme(engine):
    # create lookup tables
    lookups = [
        ['measure', {}],
        ['location', {}],
        ['sex', {}],
        ['age', {}],
        ['cause', {}],
        ['metric', {}],
    ]

    sql = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """

    for row in engine.execute(sql):
        table_name = row[0]
        if table_name.startswith('ihme_gbd'):
            print(table_name)
            df = pd.read_sql_table(table_name, con=engine)

            # create the lookup tables by finding the key value pairs for measures, locations, ...
            for lookup_idx, (lkp_key, kv_store) in enumerate(lookups):
                for _name in df['{}_name'.format(lkp_key)].unique():
                    _id = df['{}_id'.format(lkp_key)].loc[df['{}_name'.format(lkp_key)] == _name].unique()
                    if len(_id) != 1:
                        print("No unique id for {}: {}".format(lkp_key, _name))
                    else:
                        lookups[lookup_idx][1][_id[0]] = _name

    for kv_store in lookups:
        metric, dict_values = kv_store
        values = []
        for key, val in dict_values.items():
            values.append([key, val])
        metric_df = pd.DataFrame(data=values, columns=mapping_cols)
        metric_df.to_sql(metric, con=engine, if_exists='replace')

    for row in engine.execute(sql):
        table_name = row[0]
        if table_name.startswith('ihme_gbd'):
            print(table_name)
            df = pd.read_sql_table(table_name, con=engine)

            for lkp_key, kv_store in lookups:
                df.drop('{}_name'.format(lkp_key), axis=1, inplace=True)
                print("dropped {}_name".format(lkp_key))
            df_to_sql(df, '{}'.format(table_name), engine, if_exists='replace')
            print("stored table")