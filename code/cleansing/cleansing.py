from sqlalchemy import create_engine

from code.cleansing.clean_ghdx import clean_ghdx
from code.cleansing.clean_ihme import clean_ihme
from code.cleansing.clean_wdi import clean_wdi
from code.cleansing.clean_wdi_population import clean_wdi_population
from code.config import username, password, host, port, database, data_dir

def cleanse(engine):
    clean_ihme(engine)
    clean_ghdx(engine)
    clean_wdi(engine)

if __name__ == '__main__':
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, database))
    clean_wdi_population(engine)
    #cleanse(engine)