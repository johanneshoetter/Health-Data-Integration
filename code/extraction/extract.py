from sqlalchemy import create_engine

from code.extraction.initialize_db import initialize_db
from code.extraction.load_csvs import load_csv
from code.extraction.normalize_ihme import normalize_ihme
from code.extraction.normalize_wdi import normalize_wdi
from code.config import username, password, host, port, database, data_dir

def extract(engine, do_initialize_db):
    print("Starting extraction of data sources.")
    if do_initialize_db:
        initialize_db(engine)
    load_csv(data_dir, engine)
    normalize_ihme(engine)
    normalize_wdi(engine)
    print("Finished extraction of data sources.")

if __name__ == '__main__':
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, database))
    INITIALIZE_DB = False
    extract(engine, INITIALIZE_DB)
