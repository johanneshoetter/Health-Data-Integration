from sqlalchemy import create_engine

from code.integration.clean_db import clean_db
from code.integration.stack_ihme import stack_ihme
from code.integration.denormalize_ghdx import denormalize_ghdx
from code.integration.pivot_wdi import pivot_wdi

from code.config import username, password, host, port, database

def integrate(engine, do_clean_db):
    ihme_tables = stack_ihme(engine)
    ghdx_tables = denormalize_ghdx(engine)
    wdi_tables = pivot_wdi(engine)

    if do_clean_db:
        clean_db(engine, ihme_tables + ghdx_tables + wdi_tables)

if __name__ == '__main__':
    CLEAN_DB = False
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, database))
    integrate(engine, CLEAN_DB)
