
import os
import pandas as pd

from code.utils.dataframe_utils import df_to_sql

def load_csv(data_dir, engine):
    # get all .csv files
    for file in os.listdir(data_dir):
        if file.endswith('.csv'):
            if not file.startswith('.'):  # skip those files
                table_name = file[:-4].replace('-', '_').lower()  # last 4 characters are .csv
                print("Creating table {}".format(table_name))
                df = pd.read_csv(os.path.join(data_dir, file), sep=',')
                print("Saving to database")
                df_to_sql(df, table_name, engine)
                print()