import pandas as pd
import sqlalchemy as db

engine = db.create_engine('sqlite:///../data/RawHealthData.db')
connection = engine.connect()

sql = """
SELECT * FROM average_price_of_a_pack_of_cigarettes
"""

df = pd.read_sql(sql, con=engine)