from sqlalchemy import create_engine
from config import username, password, host, port, database

from extraction.extract import extract
from integration.integrate import integrate

# TODO: argparser bauen
if __name__ == '__main__':
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, database))
    INITIALIZE_DB = True
    CLEAN_DB = True

    extract(engine, INITIALIZE_DB)
    integrate(engine, CLEAN_DB)
