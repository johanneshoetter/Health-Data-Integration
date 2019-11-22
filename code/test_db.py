import timeit

import pandas as pd
import sqlalchemy as db
engine = db.create_engine('sqlite:///../data/RawHealthData.db')

sql = """
SELECT * FROM ihme_gbd_2017_data_cb71683c_10
UNION
SELECT * FROM ihme_gbd_2017_data_cb71683c_11
UNION
SELECT * FROM ihme_gbd_2017_data_cb71683c_12;
"""

#start = timeit.default_timer()
#df = pd.read_sql_query(sql, engine)
#stop = timeit.default_timer()

#print(stop-start)
