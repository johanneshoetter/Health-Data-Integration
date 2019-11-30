from sqlalchemy import create_engine

from code.integration.stack_ihme import stack_ihme
from code.integration.denormalize_ghdx import denormalize_ghdx
from code.integration.pivot_wdi import pivot_wdi

from code.config import username, password, host, port, database, data_dir

CLEAN_DB = False

if __name__ == '__main__':
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, database))

    #stack_ihme(engine)
    #denormalize_ghdx(engine)
    #pivot_wdi(engine)

    if CLEAN_DB:
        pass


